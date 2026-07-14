import json
import os
import socket
import threading
import time
from datetime import datetime, timedelta, timezone

from api.config import (
    DEAD_LETTER_KEY,
    HEARTBEAT_INTERVAL,
    LEASE_TTL,
    PRIORITIES,
    PROCESSING_KEY,
    SCHEDULED_KEY,
    job_key,
    lease_key,
    queue_key,
    worker_key,
)
from api.redis_client import get_redis
from worker.handlers import HANDLERS

# BLPOP checks keys in argument order, so listing high first IS the
# priority scheduling: a high job is always taken before a normal one.
QUEUES = [queue_key(p) for p in PRIORITIES]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Worker:
    def __init__(self):
        self.worker_id = f"{socket.gethostname()}:{os.getpid()}"
        self.redis = get_redis()
        self.current_job_id = None

    # -- heartbeat: runs in a daemon thread so it keeps beating even
    #    while the main thread is stuck inside a slow handler. If the
    #    process dies, the thread dies with it and the lease expires
    #    on its own — Redis TTL is the death detector.
    def _heartbeat_loop(self):
        r = get_redis()
        while True:
            r.set(worker_key(self.worker_id), _now(), ex=LEASE_TTL)
            job_id = self.current_job_id
            if job_id:
                r.expire(lease_key(job_id), LEASE_TTL)
            time.sleep(HEARTBEAT_INTERVAL)

    def claim(self, job_id: str):
        # NOTE: a crash in the gap between BLPOP and this claim still
        # loses the job. BLPOP over multiple queues can't atomically
        # push to a processing set; the window is microseconds and we
        # accept it (documented limitation).
        pipe = self.redis.pipeline(transaction=True)
        pipe.sadd(PROCESSING_KEY, job_id)
        pipe.set(lease_key(job_id), self.worker_id, ex=LEASE_TTL)
        pipe.execute()
        self.current_job_id = job_id

    def release(self, job_id: str):
        self.current_job_id = None
        pipe = self.redis.pipeline(transaction=True)
        pipe.srem(PROCESSING_KEY, job_id)
        pipe.delete(lease_key(job_id))
        pipe.execute()

    def sweep_expired_leases(self):
        for job_id in self.redis.smembers(PROCESSING_KEY):
            if self.redis.exists(lease_key(job_id)):
                continue  # claimant is alive and heartbeating
            # SREM returns 1 to exactly one caller, so concurrent
            # sweepers can't both re-queue the same job.
            if not self.redis.srem(PROCESSING_KEY, job_id):
                continue
            job = self.redis.hgetall(job_key(job_id))
            if not job:
                continue
            self.redis.hset(job_key(job_id), mapping={
                "status": "queued",
                "note": f"re-queued after lease expiry (was on {job.get('worker_id', '?')})",
                "updated_at": _now(),
            })
            self.redis.rpush(queue_key(job.get("priority", "normal")), job_id)
            print(f"[{self.worker_id}] RESCUED {job_id}: lease expired, re-queued", flush=True)

    def promote_scheduled_retries(self):
        due = self.redis.zrangebyscore(SCHEDULED_KEY, 0, time.time())
        for job_id in due:
            # ZREM returns 1 to exactly one caller — same single-promoter
            # guarantee as the lease sweep.
            if not self.redis.zrem(SCHEDULED_KEY, job_id):
                continue
            job = self.redis.hgetall(job_key(job_id))
            if not job:
                continue
            self.redis.hset(job_key(job_id), mapping={
                "status": "queued",
                "updated_at": _now(),
            })
            self.redis.rpush(queue_key(job.get("priority", "normal")), job_id)
            print(f"[{self.worker_id}] retry due, re-queued {job_id}", flush=True)

    def process(self, job_id: str):
        key = job_key(job_id)
        job = self.redis.hgetall(key)
        if not job:
            print(f"[{self.worker_id}] popped id {job_id} with no job record, skipping", flush=True)
            self.release(job_id)
            return

        self.redis.hset(key, mapping={
            "status": "running",
            "worker_id": self.worker_id,
            "attempts": int(job["attempts"]) + 1,
            "updated_at": _now(),
        })
        print(f"[{self.worker_id}] running {job['task']} ({job_id})", flush=True)

        try:
            handler = HANDLERS.get(job["task"])
            if handler is None:
                raise KeyError(f"no handler registered for task '{job['task']}'")
            result = handler(json.loads(job["payload"]))
            self.redis.hset(key, mapping={
                "status": "done",
                "result": json.dumps(result),
                "updated_at": _now(),
            })
            print(f"[{self.worker_id}] done: {job_id}", flush=True)
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            attempts = int(job["attempts"]) + 1  # value we wrote at pickup
            max_retries = int(job.get("max_retries", 3))
            if attempts <= max_retries:
                delay = 2 ** attempts  # 2s, 4s, 8s...
                ready_at = time.time() + delay
                self.redis.hset(key, mapping={
                    "status": "retrying",
                    "error": err,
                    "next_retry_at": (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat(),
                    "updated_at": _now(),
                })
                self.redis.zadd(SCHEDULED_KEY, {job_id: ready_at})
                print(f"[{self.worker_id}] attempt {attempts}/{max_retries + 1} failed: {job_id}: "
                      f"{exc} — retrying in {delay}s", flush=True)
            else:
                self.redis.hset(key, mapping={
                    "status": "dead",
                    "error": err,
                    "updated_at": _now(),
                })
                self.redis.rpush(DEAD_LETTER_KEY, job_id)
                print(f"[{self.worker_id}] DEAD-LETTER: {job_id} after {attempts} attempts: {exc}",
                      flush=True)
        finally:
            self.release(job_id)

    def run(self):
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        print(
            f"[{self.worker_id}] waiting for jobs on {', '.join(QUEUES)} "
            f"(lease {LEASE_TTL}s, heartbeat {HEARTBEAT_INTERVAL}s)",
            flush=True,
        )
        while True:
            self.sweep_expired_leases()
            self.promote_scheduled_retries()
            popped = self.redis.blpop(QUEUES, timeout=5)
            if popped is None:
                continue
            _queue, job_id = popped
            self.claim(job_id)
            self.process(job_id)


if __name__ == "__main__":
    Worker().run()

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from .config import (
    DEAD_LETTER_KEY,
    IDEMPOTENCY_TTL,
    KEY_PREFIX,
    MAX_QUEUE_DEPTH,
    PRIORITIES,
    PROCESSING_KEY,
    SCHEDULED_KEY,
    idem_key,
    job_key,
    queue_key,
)
from .models import EnqueueRequest, EnqueueResponse
from .redis_client import get_redis

app = FastAPI(title="taskq", version="0.1.0")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.get("/healthz")
def healthz():
    get_redis().ping()
    return {"status": "ok"}


@app.post("/enqueue", response_model=EnqueueResponse, status_code=202)
def enqueue(req: EnqueueRequest):
    r = get_redis()
    job_id = uuid.uuid4().hex

    def dedup_response():
        existing_id = r.get(idem_key(req.idempotency_key))
        existing = r.hgetall(job_key(existing_id)) if existing_id else {}
        if not existing:
            return None
        return EnqueueResponse(
            job_id=existing_id,
            status=existing.get("status", "unknown"),
            priority=existing.get("priority", req.priority),
            deduplicated=True,
        )

    # A duplicate of an accepted job succeeds even when the queue is full —
    # it adds no work. Only genuinely new jobs face backpressure.
    if req.idempotency_key:
        hit = dedup_response()
        if hit:
            return hit

    depth = r.llen(queue_key(req.priority))
    if depth >= MAX_QUEUE_DEPTH:
        raise HTTPException(
            status_code=503,
            detail=f"queue '{req.priority}' is full ({depth}/{MAX_QUEUE_DEPTH}); retry later",
            headers={"Retry-After": "5"},
        )

    if req.idempotency_key:
        # SET NX is atomic: of two concurrent requests with the same key,
        # exactly one claims it; the loser is served the winner's job.
        claimed = r.set(idem_key(req.idempotency_key), job_id, nx=True, ex=IDEMPOTENCY_TTL)
        if not claimed:
            hit = dedup_response()
            if hit:
                return hit

    now = _now()
    job = {
        "id": job_id,
        "task": req.task,
        "payload": json.dumps(req.payload),
        "priority": req.priority,
        "idempotency_key": req.idempotency_key or "",
        "status": "queued",
        "attempts": 0,
        "max_retries": req.max_retries,
        "created_at": now,
        "updated_at": now,
    }
    # Transactional pipeline: the job hash and its queue entry must land
    # together, or a worker could pop an id whose metadata doesn't exist.
    pipe = r.pipeline(transaction=True)
    pipe.hset(job_key(job_id), mapping=job)
    pipe.rpush(queue_key(req.priority), job_id)
    pipe.execute()
    return EnqueueResponse(job_id=job_id, status="queued", priority=req.priority)


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = get_redis().hgetall(job_key(job_id))
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    job["payload"] = json.loads(job["payload"])
    return job


@app.get("/stats")
def stats():
    r = get_redis()
    pipe = r.pipeline()
    for p in PRIORITIES:
        pipe.llen(queue_key(p))
    pipe.scard(PROCESSING_KEY)
    pipe.zcard(SCHEDULED_KEY)
    pipe.llen(DEAD_LETTER_KEY)
    *depths, processing, scheduled, dead_count = pipe.execute()

    workers = []
    for wkey in r.scan_iter(f"{KEY_PREFIX}worker:*"):
        workers.append({
            "id": wkey[len(KEY_PREFIX):].split(":", 1)[1],
            "last_heartbeat": r.get(wkey),
            "expires_in": r.ttl(wkey),
        })

    dead_ids = r.lrange(DEAD_LETTER_KEY, -10, -1)
    dead_jobs = []
    for jid in reversed(dead_ids):
        job = r.hgetall(job_key(jid))
        if job:
            dead_jobs.append({k: job.get(k) for k in
                              ("id", "task", "attempts", "error", "updated_at")})

    return {
        "queues": dict(zip(PRIORITIES, depths)),
        "max_queue_depth": MAX_QUEUE_DEPTH,
        "processing": processing,
        "scheduled_retries": scheduled,
        "dead_letter_count": dead_count,
        "workers": sorted(workers, key=lambda w: w["id"]),
        "recent_dead_letter": dead_jobs,
        "server_time": _now(),
    }


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return (Path(__file__).parent / "dashboard.html").read_text()


@app.get("/dead-letter")
def dead_letter(limit: int = 20):
    r = get_redis()
    ids = r.lrange(DEAD_LETTER_KEY, -limit, -1)
    jobs = []
    for jid in ids:
        job = r.hgetall(job_key(jid))
        if job:
            jobs.append({k: job.get(k) for k in
                         ("id", "task", "priority", "attempts", "error", "updated_at")})
    return {"count": r.llen(DEAD_LETTER_KEY), "jobs": jobs}


@app.get("/queues")
def queues():
    r = get_redis()
    pipe = r.pipeline()
    for p in PRIORITIES:
        pipe.llen(queue_key(p))
    depths = pipe.execute()
    return dict(zip(PRIORITIES, depths))

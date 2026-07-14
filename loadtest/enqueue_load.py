"""Fire N enqueue requests at a taskq API and report real latency numbers.

Usage:
    python -m loadtest.enqueue_load --url http://localhost:8000 --jobs 500 --concurrency 20
    python -m loadtest.enqueue_load --wait-drain   # also time workers draining the backlog

Latency is measured per-request around POST /enqueue only. Keep-alive
connections (one httpx client per thread) so we measure the API, not
TCP handshakes.
"""
import argparse
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx


def percentile(sorted_vals, p):
    idx = max(0, math.ceil(p / 100 * len(sorted_vals)) - 1)
    return sorted_vals[idx]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--jobs", type=int, default=500)
    ap.add_argument("--concurrency", type=int, default=20)
    ap.add_argument("--task", default="noop")
    ap.add_argument("--priority", default="normal")
    ap.add_argument("--wait-drain", action="store_true",
                    help="after enqueueing, poll /queues until empty and report processing rate")
    args = ap.parse_args()

    latencies = []
    codes = {}
    lock = threading.Lock()
    tls = threading.local()

    def client():
        if not hasattr(tls, "c"):
            tls.c = httpx.Client(base_url=args.url, timeout=30)
        return tls.c

    def fire(i):
        body = {"task": args.task, "payload": {"n": i}, "priority": args.priority}
        t0 = time.perf_counter()
        try:
            code = client().post("/enqueue", json=body).status_code
        except Exception as exc:
            code = type(exc).__name__
        dt_ms = (time.perf_counter() - t0) * 1000
        with lock:
            latencies.append(dt_ms)
            codes[code] = codes.get(code, 0) + 1

    print(f"target: {args.url}  jobs: {args.jobs}  concurrency: {args.concurrency}")
    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        list(ex.map(fire, range(args.jobs)))
    wall = time.perf_counter() - t_start

    latencies.sort()
    print(f"\nstatus codes: {codes}")
    print(f"enqueue wall time: {wall:.2f}s  ->  throughput: {args.jobs / wall:.1f} req/s")
    print(f"latency  p50: {percentile(latencies, 50):.1f} ms")
    print(f"latency  p90: {percentile(latencies, 90):.1f} ms")
    print(f"latency  p99: {percentile(latencies, 99):.1f} ms")
    print(f"latency  min: {latencies[0]:.1f} ms  max: {latencies[-1]:.1f} ms")

    if args.wait_drain:
        while True:
            left = sum(client().get("/queues").json().values())
            if left == 0:
                break
            time.sleep(0.5)
        total = time.perf_counter() - t_start
        print(f"\nqueues empty {total:.1f}s after first enqueue "
              f"->  end-to-end ~{args.jobs / total:.1f} jobs/s (enqueue + processing)")


if __name__ == "__main__":
    main()

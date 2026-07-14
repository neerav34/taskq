# taskq

A small, real distributed task queue — the same problem category as Celery, SQS,
and BullMQ. Any app can enqueue background jobs over HTTP; a pool of Python
workers processes them reliably, **even when workers crash mid-job**.

Background work (emails, image resizing, webhooks, report generation) can't run
inside an HTTP request — it's too slow, and a failure would take the user's
request down with it. The standard answer is a task queue, and the hard parts
are all failure handling: What if a worker dies holding a job? What if the same
job gets submitted twice? What if a job fails — forever? What if producers
outrun consumers? taskq implements a real answer to each.

**Live:** [API](https://taskq-api-8orl.onrender.com/docs) ·
[Dashboard](https://taskq-api-8orl.onrender.com/dashboard) — free-tier
hosting sleeps after 15 min idle, so the first request may take ~30s. Demo
video: _coming soon_.

## Features

- **Priority scheduling** — three queues (`high` / `normal` / `low`); a high
  job always jumps ahead of waiting lower-priority work
- **Job leasing + automatic rescue** — a worker killed mid-job (`kill -9`) has
  its job re-queued and completed by another worker within seconds
- **Idempotent enqueue** — the same `idempotency_key` twice returns the
  original job instead of creating a duplicate
- **Retries with exponential backoff** — failed jobs retry after 2s / 4s / 8s,
  then land in a **dead-letter queue** for inspection
- **Backpressure** — `/enqueue` returns `503` + `Retry-After` once a queue
  hits its depth cap, instead of accepting unbounded work
- **Live dashboard** — queue depths, worker heartbeats, dead-letter jobs,
  refreshed every 2s

## Architecture

```
Producer (any app, any language)
       │  POST /enqueue
       ▼
FastAPI API server ──────────────┐
       │                         │ GET /jobs/{id}, /stats, /dashboard
       ▼                         │
     Redis                       │
     ├── queue:high ─┐           │
     ├── queue:normal├─ BLPOP ── Workers (Python, run anywhere)
     ├── queue:low  ─┘             │  lease + heartbeat while running
     ├── job:{id}      hashes ◄────┤  status/result written back
     ├── processing    set         │
     ├── scheduled     zset (retries due later)
     ├── dead_letter   list
     ├── lease:{id}    TTL keys (worker liveness per job)
     └── idem:{key}    TTL keys (dedup)
```

Everything coordinates through Redis; API and workers never talk to each other
directly. The API is stateless, so it scales horizontally; workers can run on
any machine that can reach Redis.

## Quickstart (local)

```bash
brew install redis && brew services start redis   # or any Redis ≥ 6

python3 -m venv .venv
./.venv/bin/pip install -r requirements.txt

# Terminal 1 — API
./.venv/bin/uvicorn api.main:app --port 8000

# Terminal 2 — a worker (start as many as you like)
./.venv/bin/python -m worker.main

# enqueue something
curl -X POST localhost:8000/enqueue -H "Content-Type: application/json" \
  -d '{"task": "send_welcome", "payload": {"user_id": 42}, "priority": "high"}'
```

Dashboard: **http://localhost:8000/dashboard** · Interactive API docs:
**http://localhost:8000/docs** · Full manual test walkthrough (including the
kill-a-worker demo): [TESTING.md](TESTING.md)

## API

| Endpoint | What it does |
|---|---|
| `POST /enqueue` | Accept a job: `task`, `payload`, `priority`, `max_retries`, `idempotency_key` → `202` + `job_id`, or `503` under backpressure |
| `GET /jobs/{id}` | Full job record: status, attempts, result/error, timestamps |
| `GET /queues` | Depth of each priority queue |
| `GET /dead-letter` | Jobs that exhausted their retries |
| `GET /stats` | Everything the dashboard shows, as JSON |
| `GET /healthz` | API + Redis liveness |

Job statuses: `queued → running → done`, or `running → retrying → queued →
… → dead` on repeated failure.

## Tasks & writing your own handler

A job's `task` field names a Python function in the worker's registry
([worker/handlers.py](worker/handlers.py)). Built-ins: `send_welcome` and
`resize_image` (simulate slow I/O), `noop` (instant — used by the load test),
`long_task` (seconds of visible progress — used for the kill-a-worker demo),
`always_fail` (used to demo retries/dead-letter). A job naming an unregistered
task fails cleanly with `no handler registered`.

Adding your own is a function plus a registry entry:

```python
def send_invoice(payload):
    # payload is the JSON object the producer enqueued
    ...
    return {"invoiced": payload["order_id"]}   # stored as the job's result

HANDLERS = {..., "send_invoice": send_invoice}
```

Because delivery is at-least-once (see below), handlers should be idempotent —
safe to run twice for the same payload.

## Configuration

All via environment variables, all optional:

| Variable | Default | What it controls |
|---|---|---|
| `TASKQ_REDIS_URL` | `redis://localhost:6379/0` | Redis connection (use `rediss://` for TLS) |
| `TASKQ_KEY_PREFIX` | *(empty)* | Namespace for every key, e.g. `taskq:` — lets taskq share a Redis DB with another app; API and workers must match |
| `TASKQ_LEASE_TTL` | `30` | Seconds before an unrefreshed job lease expires (worker presumed dead) |
| `TASKQ_HEARTBEAT` | `10` | Seconds between lease/liveness refreshes |
| `TASKQ_MAX_QUEUE_DEPTH` | `1000` | Per-queue cap before `/enqueue` returns 503 |
| `TASKQ_IDEMPOTENCY_TTL` | `86400` | Seconds an idempotency key blocks duplicates |

## How the failure handling works

### Leasing & heartbeats (dead-worker rescue)

`BLPOP` removes a job id from its queue atomically — exactly one worker ever
receives it. But that means a crashed worker would take its job to the grave.
So on pickup the worker (1) adds the id to a `processing` set and (2) writes
`lease:{job_id}` with a TTL (default 30s). A daemon thread refreshes the lease
every 10s while the job runs. If the process dies — hard kill, OOM, power loss
— the heartbeat stops and **Redis itself expires the lease**; there is no
failure detector to build or fool. Every worker sweeps the `processing` set on
each loop: an entry with no lease key is an orphan and gets re-queued. The
sweep uses `SREM` (atomic, returns 1 to exactly one caller) so concurrent
sweepers can't double-rescue.

**Known gap:** `BLPOP` across multiple queues can't atomically push into the
processing set, so a crash in the microseconds between pop and claim loses that
job. Single-queue designs close this with `BRPOPLPUSH`; multi-priority designs
accept it or busy-poll.

### Idempotency

`POST /enqueue` with `idempotency_key` does `SET idem:{key} {job_id} NX EX
86400`. `NX` makes check-and-claim a single atomic operation — two concurrent
duplicates can't both win. The loser gets the winner's `job_id` back with
`deduplicated: true`. Keys expire after 24h, so dedup memory is bounded.

### Retries, backoff, dead-letter

A failed job with attempts remaining is scheduled, not immediately re-queued:
it goes into a `scheduled` sorted set scored by ready-at time (`now + 2^attempt`
seconds — 2s, 4s, 8s). Workers promote due entries back to their priority queue
each loop. Backoff matters because a struggling downstream service needs less
traffic to recover, not an instant thundering herd of retries. After
`max_retries` (default 3, so 4 total attempts) the job is marked `dead` and
pushed to the `dead_letter` list — inspectable via `GET /dead-letter`, never
retried again, never silently dropped.

### Backpressure

Each queue has a depth cap (`TASKQ_MAX_QUEUE_DEPTH`, default 1000). At the cap,
`/enqueue` returns `503` with `Retry-After: 5` — the producer finds out *now*
that the system is saturated, instead of the queue growing until Redis dies.
Duplicates of already-accepted jobs still succeed when full (they add no work).

## Delivery semantics — honest version

taskq is **at-least-once**. If a worker is slow-but-alive past its lease (long
GC pause, network partition), the sweeper re-queues the job and it runs twice.
Exactly-once delivery across process crashes is not achievable with this
design (or, practically, with most systems that claim it) — the correct
contract is at-least-once delivery plus **idempotent handlers**, which is the
same contract SQS standard queues and Celery offer.

**If Redis goes down, taskq is down.** Enqueues fail (503/500), workers block
until it returns. Jobs already accepted survive to whatever extent Redis
persistence is configured (Upstash persists; a default local Redis snapshots
periodically). There is no Redis replication or failover here — that's the
single point of failure, stated plainly.

## Load test

Methodology: `loadtest/enqueue_load.py` fires N concurrent `POST /enqueue`
requests (keep-alive connections, one client per thread) and measures
per-request latency; `--wait-drain` then polls until workers empty the queues.
Numbers below are measured, not estimated; run it yourself with:

```bash
./.venv/bin/pip install httpx
./.venv/bin/python -m loadtest.enqueue_load --jobs 500 --concurrency 20 --wait-drain
```

**Local baseline** (M-series MacBook, local Redis, 1 uvicorn process, 2
workers, 500 `noop` jobs @ concurrency 20, warmed up):

| Metric | Value |
|---|---|
| Enqueue throughput | ~520 req/s |
| Enqueue latency p50 | 28 ms |
| Enqueue latency p90 | 57 ms |
| Enqueue latency p99 | 202 ms |
| End-to-end (enqueue + process) | ~515 jobs/s |

First-run (cold thread pool) p99 was ~516 ms — warmup excluded from the
steady-state numbers above, noted here for honesty.

**Deployed** (Render free tier + Upstash free tier, shared database; 200
`noop` jobs @ concurrency 10, warmed instance, client on a home connection in
India):

| Metric | Value |
|---|---|
| Accepted | 200/200 (zero errors) |
| Enqueue throughput | 10.3 req/s (concurrency-bound: ~10 in flight × ~750 ms each) |
| Enqueue latency p50 | 751 ms |
| Enqueue latency p90 | 1004 ms |
| Enqueue latency p99 | 4476 ms |
| End-to-end drain | ~4.9 jobs/s (2 workers running on a laptop) |

What these numbers actually measure: geography, mostly. Each request crosses
client → Render, then the API makes two Redis round trips to Upstash (depth
check + write pipeline). The p99 tail is dominated by the ten initial TLS
handshakes. Workers ran on a laptop, so every job cost ~8 laptop↔Upstash
round trips — colocating workers and Redis in one region is the obvious
production fix and would change these numbers dramatically (compare the local
baseline above, where the same code does ~520 req/s). Test size (200 jobs) was
deliberately small: the Redis database is a shared free tier with a command
budget. Render free-tier cold start after 15 min idle adds ~30s to the first
request (per Render's documentation); load tests were run warmed, never
averaging cold starts into latency numbers.

## Deployment (all free tiers)

- **Redis — Upstash:** create a database, use the `rediss://` (TLS) connection
  URL. Note the free tier's command budget: this is why workers use a blocking
  `BLPOP` (one command per ~5s when idle) and a 10s heartbeat rather than
  tight polling. taskq can **share a Redis database with another app**: set
  `TASKQ_KEY_PREFIX` (e.g. `taskq:`) and every key it writes is namespaced
  under that prefix — API and all workers must use the same value.
- **API — Render:** `render.yaml` is included — create a Blueprint service
  from this repo and set `TASKQ_REDIS_URL` to the Upstash URL. Free tier
  sleeps after 15 min idle; first request pays ~30s.
- **Workers — anywhere:** `TASKQ_REDIS_URL=rediss://... python -m worker.main`
  on any machine with internet — laptop, Pi, a second Render worker service.
  Workers need only Redis, not the API.

## What this is not (vs SQS / Celery / BullMQ)

- No Redis replication/failover — one Redis, one region, one point of failure
- No exactly-once semantics (see above — and be suspicious when anything
  promises it)
- Dead-letter jobs are inspectable but replay is manual
- No per-job timeouts, rate limits, cron schedules, or job dependencies
- Observability is a dashboard, not metrics/tracing/alerting

Knowing precisely where the gap is between this and production systems is the
point of having built it.

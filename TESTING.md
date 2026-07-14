# taskq — Manual Test Script

Walks every feature end-to-end. Run from the repo root (`~/Code/taskq`).

## Setup — 3 terminals

```bash
# Terminal A — API (small queue cap so you can see backpressure later)
TASKQ_MAX_QUEUE_DEPTH=5 ./.venv/bin/uvicorn api.main:app --port 8000

# Terminal B — worker with fast lease timing (so the kill test is quick)
TASKQ_LEASE_TTL=10 TASKQ_HEARTBEAT=3 ./.venv/bin/python -m worker.main

# Terminal C — your commands (everything below runs here)
```

Optional Terminal D: `redis-cli monitor` — live feed of every Redis command.

Sanity check first:

```bash
redis-cli ping                      # -> PONG
curl -s localhost:8000/healthz     # -> {"status":"ok"}
```

Open the dashboard in your browser and keep it visible the whole time:
**http://localhost:8000/dashboard** — it refreshes every 2s and every test
below shows up on it.

---

## Test 1 — Basic job lifecycle (Day 1–2)

```bash
curl -s -X POST localhost:8000/enqueue -H "Content-Type: application/json" \
  -d '{"task":"send_welcome","payload":{"user_id":1}}'
```

Expect: HTTP 202 with a `job_id`. Terminal B prints `running` → `done`
within ~3s. Check the record (paste the id):

```bash
curl -s localhost:8000/jobs/<job_id>
```

Expect `"status":"done"`, `"attempts":"1"`, a `result` field.

## Test 2 — Priority jumping (Day 3)

Paste all three at once:

```bash
curl -s -X POST localhost:8000/enqueue -H "Content-Type: application/json" \
  -d '{"task":"send_welcome","payload":{"user_id":"low-A"},"priority":"low"}'
curl -s -X POST localhost:8000/enqueue -H "Content-Type: application/json" \
  -d '{"task":"send_welcome","payload":{"user_id":"low-B"},"priority":"low"}'
curl -s -X POST localhost:8000/enqueue -H "Content-Type: application/json" \
  -d '{"task":"send_welcome","payload":{"user_id":"VIP"},"priority":"high"}'
```

Expect in Terminal B: **low-A → VIP → low-B**. VIP (enqueued last) jumps
ahead of low-B because the worker checks `queue:high` first.

## Test 3 — Kill a worker mid-job (Day 4) ⭐ the flagship

Start a SECOND worker in another terminal (same command as Terminal B).
Then:

```bash
curl -s -X POST localhost:8000/enqueue -H "Content-Type: application/json" \
  -d '{"task":"long_task","payload":{"seconds":60}}'
```

One worker starts printing `long_task progress N/60`. Its log line shows its
PID, e.g. `[NEERAV-JHA.local:51726]` → PID is 51726. Kill it hard:

```bash
kill -9 <PID>
```

Watch the OTHER worker's terminal. Within ~15s (10s lease expiry + sweep):

```
RESCUED <job_id>: lease expired, re-queued
running long_task (<job_id>)
```

On the dashboard: the dead worker vanishes from Workers (heartbeat key
expired), the survivor remains. When done:

```bash
curl -s localhost:8000/jobs/<job_id>
```

Expect `"attempts":"2"` and a `note` naming the dead worker.

## Test 4 — Idempotency (Day 5)

Run the SAME command twice:

```bash
curl -s -X POST localhost:8000/enqueue -H "Content-Type: application/json" \
  -d '{"task":"send_welcome","payload":{"user_id":42},"idempotency_key":"welcome-42"}'
```

Expect: identical `job_id` both times; second says `"deduplicated":true`
and reports the job's live status. Only ONE run in Terminal B.

## Test 5 — Retries + dead-letter (Day 5)

```bash
curl -s -X POST localhost:8000/enqueue -H "Content-Type: application/json" \
  -d '{"task":"always_fail","payload":{}}'
```

Watch Terminal B for ~40s: attempt 1 → "retrying in 2s" → attempt 2 → 4s →
attempt 3 → 8s → attempt 4 → `DEAD-LETTER after 4 attempts`.
Dashboard: "dead-letter" tile turns red and the job appears in the table.

```bash
curl -s localhost:8000/dead-letter
```

## Test 6 — Backpressure (Day 6)

Stop ALL workers (Ctrl+C) so nothing drains. The cap is 5 (set in Setup).
Fire 7 jobs:

```bash
for i in 1 2 3 4 5 6 7; do
  curl -s -o /dev/null -w "job $i -> HTTP %{http_code}\n" \
    -X POST localhost:8000/enqueue -H "Content-Type: application/json" \
    -d '{"task":"send_welcome","payload":{},"priority":"low"}'
done
```

Expect: jobs 1–5 → `202`, jobs 6–7 → `503`. Dashboard: low-queue bar hits
5/5. Verify high still accepts (independent cap), then restart the worker
and watch the bar drain to 0.

## Test 7 — Dashboard (Day 6)

Already open — confirm it showed: live queue bars, worker rows appearing/
disappearing with heartbeats, the red dead-letter tile, and "API
unreachable" if you Ctrl+C the API (then recovery when you restart it).

## Reset between sessions (optional)

Wipes ALL taskq data in the local Redis (jobs, queues, DLQ, idem keys):

```bash
redis-cli flushdb
```

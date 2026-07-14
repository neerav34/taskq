import os

REDIS_URL = os.getenv("TASKQ_REDIS_URL", "redis://localhost:6379/0")

# Namespace prepended to every key taskq writes (e.g. "taskq:"), so it can
# share a Redis database with other apps without key collisions.
KEY_PREFIX = os.getenv("TASKQ_KEY_PREFIX", "")

# Order matters: workers scan high first, low last.
PRIORITIES = ("high", "normal", "low")

# A worker's lease on a job must outlive several missed heartbeats,
# or a brief stall (GC pause, slow handler step) would look like death.
LEASE_TTL = int(os.getenv("TASKQ_LEASE_TTL", "30"))
HEARTBEAT_INTERVAL = int(os.getenv("TASKQ_HEARTBEAT", "10"))

# Set of job ids currently claimed by some worker.
PROCESSING_KEY = f"{KEY_PREFIX}processing"

# Sorted set of job ids awaiting retry, scored by ready-at unix time.
SCHEDULED_KEY = f"{KEY_PREFIX}scheduled"

# List of job ids that exhausted their retries.
DEAD_LETTER_KEY = f"{KEY_PREFIX}dead_letter"

# How long a producer's idempotency key blocks duplicates.
IDEMPOTENCY_TTL = int(os.getenv("TASKQ_IDEMPOTENCY_TTL", str(24 * 3600)))

# Backpressure: /enqueue rejects with 503 once a queue reaches this depth.
MAX_QUEUE_DEPTH = int(os.getenv("TASKQ_MAX_QUEUE_DEPTH", "1000"))


def queue_key(priority: str) -> str:
    return f"{KEY_PREFIX}queue:{priority}"


def job_key(job_id: str) -> str:
    return f"{KEY_PREFIX}job:{job_id}"


def lease_key(job_id: str) -> str:
    return f"{KEY_PREFIX}lease:{job_id}"


def worker_key(worker_id: str) -> str:
    return f"{KEY_PREFIX}worker:{worker_id}"


def idem_key(key: str) -> str:
    return f"{KEY_PREFIX}idem:{key}"

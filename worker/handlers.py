import time


def send_welcome(payload):
    # Simulates a slow external call (e.g. an email provider API).
    time.sleep(2)
    print(f"  -> welcome email 'sent' to user {payload.get('user_id')}", flush=True)
    return {"sent_to": payload.get("user_id")}


def resize_image(payload):
    time.sleep(1)
    print(f"  -> 'resized' {payload.get('file')}", flush=True)
    return {"file": payload.get("file"), "size": "256x256"}


def noop(payload):
    # Instant handler for load testing: measures queue overhead, not work.
    return {"ok": True}


def long_task(payload):
    seconds = int(payload.get("seconds", 20))
    for i in range(seconds):
        time.sleep(1)
        print(f"  -> long_task progress {i + 1}/{seconds}", flush=True)
    return {"slept": seconds}


def always_fail(payload):
    raise RuntimeError("this handler always fails (for testing retries)")


HANDLERS = {
    "send_welcome": send_welcome,
    "resize_image": resize_image,
    "noop": noop,
    "long_task": long_task,
    "always_fail": always_fail,
}

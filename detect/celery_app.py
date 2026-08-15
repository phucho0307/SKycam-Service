"""Celery app for the cloud-detection worker.

Redis is the broker AND result backend. Celery Beat periodically scans MongoDB for
frames that haven't been scored yet and enqueues a detection task per frame.
"""
import os

from celery import Celery

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
SCAN_INTERVAL_S = float(os.environ.get("DETECT_SCAN_INTERVAL_S", "30"))

app = Celery("skycam_detect", broker=REDIS_URL, backend=REDIS_URL, include=["tasks"])

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
    task_acks_late=True,  # re-deliver if a worker dies mid-task
    worker_prefetch_multiplier=1,
)

# Poll for un-scored frames (avoids cross-language enqueue from the Rust service).
app.conf.beat_schedule = {
    "scan-unscored-frames": {
        "task": "tasks.scan_unscored",
        "schedule": SCAN_INTERVAL_S,
    },
}

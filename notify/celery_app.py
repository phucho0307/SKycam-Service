"""Celery app for rain alerting.

A separate app and a separate queue from `detect/`, sharing the same Redis broker.

The queue separation is the point: detection is high-volume and latency-tolerant,
alerts are low-volume and latency-critical. On one queue, a backlog of thousands
of frame-scoring tasks would sit in front of a rain warning and a 60-minute alert
would arrive in 45. Same head-of-line blocking argument as keeping a 25MB upload
off the control stream.

Run it with:
    celery -A celery_app worker -Q notify --loglevel=info
    celery -A celery_app beat   --loglevel=info      # separate process on Windows
"""
import os

from celery import Celery

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
QUEUE = os.environ.get("NOTIFY_QUEUE", "notify")

# How often to ask "is any site due?". This is only a heartbeat: the real schedule
# is forecast_sites.next_poll_at in Postgres, which is why a duplicated Beat is a
# no-op rather than a double alert.
TICK_S = float(os.environ.get("NOTIFY_TICK_S", "60"))
SEND_TICK_S = float(os.environ.get("NOTIFY_SEND_TICK_S", "15"))
SWEEP_S = float(os.environ.get("NOTIFY_SWEEP_S", "120"))
STALENESS_S = float(os.environ.get("NOTIFY_STALENESS_TICK_S", "300"))

app = Celery("skycam_notify", broker=REDIS_URL, backend=REDIS_URL, include=["tasks"])

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_default_queue=QUEUE,
    # Explicit routing, so a task added later cannot accidentally land on the
    # detection queue and inherit its backlog.
    task_routes={"notify.*": {"queue": QUEUE}},
)

app.conf.beat_schedule = {
    "sync-sites": {"task": "notify.sync_sites", "schedule": 300.0},
    "poll-forecasts": {"task": "notify.poll_forecasts", "schedule": TICK_S},
    "send-pending": {"task": "notify.send_pending", "schedule": SEND_TICK_S},
    "reclaim-stale": {"task": "notify.reclaim_stale", "schedule": SWEEP_S},
    "check-staleness": {"task": "notify.check_staleness", "schedule": STALENESS_S},
}

"""Celery app for the cloud-detection worker.

Redis is the broker AND result backend. Celery Beat periodically scans for frames
that haven't been scored yet and enqueues a detection task per frame.

Two backends exist during the migration, chosen by `DETECT_BACKEND`:

  mongo     (default) Rust ingest -> MongoDB.  tasks.py
  postgres            Go ingest   -> Postgres. tasks_pg.py

Same switch shape as `TELEMETRY_BACKEND` in the Go service, and for the same
reason: the two pipelines must be runnable side by side so the new one can be
verified before the old one is removed.
"""
import os

from celery import Celery

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
BACKEND = os.environ.get("DETECT_BACKEND", "mongo")
SCAN_INTERVAL_S = float(os.environ.get("DETECT_SCAN_INTERVAL_S", "3"))
# How often to return frames claimed by a worker that died. Must be well under
# DETECT_STALE_CLAIM_S, or stale claims sit around for longer than intended.
SWEEP_INTERVAL_S = float(os.environ.get("DETECT_SWEEP_INTERVAL_S", "60"))

if BACKEND not in ("mongo", "postgres"):
    raise SystemExit(f"DETECT_BACKEND must be mongo or postgres, got {BACKEND!r}")

_module = "tasks" if BACKEND == "mongo" else "tasks_pg"
app = Celery("skycam_detect", broker=REDIS_URL, backend=REDIS_URL, include=[_module])

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_expires=3600,
    task_acks_late=True,  # re-deliver if a worker dies mid-task
    worker_prefetch_multiplier=1,
)

if BACKEND == "mongo":
    # Poll for un-scored frames (avoids cross-language enqueue from Rust).
    # NOTE: this query is unindexed in Mongo — a full collection scan every
    # SCAN_INTERVAL_S. That, not polling itself, is the performance defect.
    app.conf.beat_schedule = {
        "scan-unscored-frames": {
            "task": "tasks.scan_unscored",
            "schedule": SCAN_INTERVAL_S,
        },
    }
else:
    # Postgres has a partial index on (received_at) WHERE detect_status='pending',
    # so this poll is an index scan over the few rows still waiting. Cheap enough
    # that it stays the primary trigger; a LISTEN/NOTIFY bridge would let the
    # interval grow to minutes, with the poll as the correctness backstop.
    app.conf.beat_schedule = {
        "scan-pending-frames": {
            "task": "tasks_pg.scan_pending",
            "schedule": SCAN_INTERVAL_S,
        },
        "reclaim-stale-claims": {
            "task": "tasks_pg.reclaim_stale",
            "schedule": SWEEP_INTERVAL_S,
        },
    }

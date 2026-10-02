"""Celery tasks for the Postgres pipeline (Go ingest → Postgres → Redis → here).

The Mongo version lives in `tasks.py` and stays until cutover; pick one with
`DETECT_BACKEND`. `detector.py` is shared and unchanged.

The three defects of the Mongo version are fixed here, and each fix is a design
decision rather than a `try`/`except` bolted on:

* **Poison pills.** Failures are *classified*. A missing preview or an
  undecodable JPEG will never succeed, so it goes straight to a terminal state.
  Only genuinely transient failures (S3 unreachable) are retried, and those have
  a budget counted on the row.
* **One retry mechanism, not two.** Postgres is the queue of record: a transient
  failure sets the row back to `pending` and the next poll re-claims it. Celery's
  own `retry()` is deliberately unused — two independent retry budgets over the
  same work is how you get a task that retries 9 times when you asked for 3.
* **Idempotent alarm writes**, via `ON CONFLICT` on `(frame_id, kind)`.

Note the two safety nets can overlap: `task_acks_late` makes Celery redeliver a
task whose worker died, and `reclaim_stale` returns the row to `pending`. Both
may fire for one frame. That is harmless *because* every write is idempotent —
which is the reason idempotency is not optional here.
"""
import hashlib
import os

import boto3
import cv2
import numpy as np
from botocore.exceptions import BotoCoreError, ClientError

from celery_app import app
from detector import analyze
from fits_archive import ALGORITHMS, compress_lossless
from store_postgres import MAX_ATTEMPTS, PostgresStore

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET = os.environ.get("S3_BUCKET", "skycam-demo")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
SCAN_LIMIT = int(os.environ.get("DETECT_SCAN_LIMIT", "50"))

# Keeping clear-sky FITS (see migration 0006_fits_archive and fits_archive.py).
FITS_COMPRESSION = os.environ.get("FITS_COMPRESSION", "GZIP_2")
if FITS_COMPRESSION not in (*ALGORITHMS, "none"):
    raise SystemExit(f"FITS_COMPRESSION must be one of {ALGORITHMS} or none, got {FITS_COMPRESSION!r}")
# Only frames received this recently are archived. Must be comfortably shorter
# than the bucket's FITS_EXPIRE_DAYS, or the raw file may already be gone.
ARCHIVE_WINDOW_S = int(os.environ.get("DETECT_ARCHIVE_WINDOW_S", str(20 * 3600)))
# Small batches: each job holds a ~25 MB file and its compressed copy in memory.
ARCHIVE_LIMIT = int(os.environ.get("DETECT_ARCHIVE_LIMIT", "10"))

_store = PostgresStore()
_s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    region_name=S3_REGION,
)


class Permanent(Exception):
    """Retrying will not help: bad data, not bad luck."""


@app.task(name="tasks_pg.scan_pending")
def scan_pending():
    """Claim a batch of pending frames and fan them out to workers.

    Claiming (rather than reading) is what lets this run on several Beat
    instances, or overlap with itself if one run is slow, without handing the
    same frame to two workers. The fields the task needs travel in the message,
    so the worker does not re-read the row it was just handed.
    """
    frames = _store.claim_pending(SCAN_LIMIT)
    for f in frames:
        detect_frame.delay(f.frame_id, f.device_id, f.preview_key, f.attempts)
    return {"claimed": len(frames)}


@app.task(name="tasks_pg.reclaim_stale")
def reclaim_stale():
    """The reconciliation sweep: frames claimed by a worker that never came back.

    This is what makes the pipeline self-healing, and it is the reason a poll can
    coexist with a NOTIFY-driven trigger: whatever the trigger misses, this finds.
    """
    n = _store.reclaim_stale()
    a = _store.reclaim_stale_archive()
    return {"reclaimed": n, "archive_reclaimed": a}


@app.task(name="tasks_pg.detect_frame")
def detect_frame(frame_id: str, device_id: str, preview_key: str | None, attempts: int):
    try:
        if not preview_key:
            # Preview-less frames exist (FITS-only cycles). Nothing to analyse,
            # and no future attempt changes that.
            raise Permanent("no preview")

        rgb = _fetch_preview(preview_key)
        result = analyze(rgb)
        if not _store.mark_scored(frame_id, result):
            # Deleted while queued (retention job, operator cleanup). Alarming on
            # it would violate the foreign key, and there is nothing to retry.
            return {"frame_id": frame_id, "gone": True}
        raised = False
        if result["is_cloudy"]:
            raised = _store.raise_alarm(frame_id, device_id, result["cloud_score"])
        return {"frame_id": frame_id, "alarm_raised": raised, **result}

    except Permanent as e:
        reason = str(e)
        # "nothing to analyse" is not the same as "we could not analyse it":
        # skipped is expected, failed wants looking at.
        if reason == "no preview":
            _store.mark_skipped(frame_id, reason)
        else:
            _store.mark_failed(frame_id, reason)
        return {"frame_id": frame_id, "terminal": reason}

    except (ClientError, BotoCoreError, OSError) as e:
        # Object storage or network: usually transient, so spend an attempt.
        return _retry_or_fail(frame_id, attempts, f"storage: {e}")

    except Exception as e:  # noqa: BLE001 — never let one frame kill the worker
        return _retry_or_fail(frame_id, attempts, f"unexpected: {type(e).__name__}: {e}")


def _retry_or_fail(frame_id: str, attempts: int, error: str) -> dict:
    if attempts >= MAX_ATTEMPTS:
        _store.mark_failed(frame_id, error)
        return {"frame_id": frame_id, "terminal": error, "attempts": attempts}
    _store.requeue(frame_id, error)
    return {"frame_id": frame_id, "requeued": error, "attempts": attempts}


def _fetch_preview(key: str) -> np.ndarray:
    obj = _s3.get_object(Bucket=S3_BUCKET, Key=key)
    data = obj["Body"].read()
    bgr = cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_COLOR)
    if bgr is None:
        # The upload was SHA-256 verified end to end, so a preview that will not
        # decode is permanently bad rather than a truncated transfer.
        raise Permanent("cv2 could not decode the preview")
    return cv2.cvtColor(bgr, cv2.COLOR_BGR2RGB)


# -- keeping clear-sky FITS ------------------------------------------------
@app.task(name="tasks_pg.scan_archive")
def scan_archive():
    """Claim clear-sky frames whose FITS should be kept, and fan them out.

    A poll, like detection: self-healing, and it never needs to know when a frame
    was scored. Cloudy, unscored and failed frames are never claimed; their raw
    FITS simply expire under the bucket's lifecycle rule.
    """
    jobs = _store.claim_archive(ARCHIVE_LIMIT, ARCHIVE_WINDOW_S)
    for j in jobs:
        archive_fits.delay(j.frame_id, j.device_id, j.fits_key, j.fits_sha256, j.attempts)
    return {"claimed": len(jobs)}


def archive_key(device_id: str, frame_id: str, compression: str) -> str:
    # Under archive/, never frames/: the frames/ lifecycle rule must not match it.
    # .fits.fz is the convention for tile-compressed FITS; readers open it as-is.
    ext = "fits" if compression == "none" else "fits.fz"
    return f"archive/frames/{device_id}/{frame_id}.{ext}"


@app.task(name="tasks_pg.archive_fits")
def archive_fits(frame_id: str, device_id: str, fits_key: str, fits_sha256: str, attempts: int):
    """Write a verified, losslessly compressed copy of a clear-sky FITS to archive/.

    The raw file is not deleted here: the lifecycle rule removes it, and until it
    does it is the fallback if anything below goes wrong.
    """
    try:
        try:
            obj = _s3.get_object(Bucket=S3_BUCKET, Key=fits_key)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                # Retrying cannot bring it back.
                raise Permanent("raw FITS is no longer in storage (expired before it was archived)")
            raise
        raw = obj["Body"].read()
        if hashlib.sha256(raw).hexdigest() != fits_sha256:
            # Ingest verified this hash on upload, so a mismatch means the stored
            # object changed. Archiving it would preserve the wrong bytes.
            raise Permanent("stored FITS does not match the SHA-256 recorded at upload")

        packed = compress_lossless(raw, FITS_COMPRESSION)
        key = archive_key(device_id, frame_id, packed.compression)
        _s3.put_object(
            Bucket=S3_BUCKET, Key=key, Body=packed.data, ContentType="application/fits",
            Metadata={"frame-id": frame_id, "original-sha256": fits_sha256,
                      "compression": packed.compression},
        )
        _store.mark_archived(frame_id, key, packed.compression, packed.note,
                             len(raw), len(packed.data))
        return {"frame_id": frame_id, "key": key, "compression": packed.compression,
                "ratio": round(len(raw) / len(packed.data), 2)}

    except Permanent as e:
        _store.archive_failed(frame_id, str(e))
        return {"frame_id": frame_id, "terminal": str(e)}
    except (ClientError, BotoCoreError, OSError) as e:
        return _archive_retry_or_fail(frame_id, attempts, f"storage: {e}")
    except Exception as e:  # noqa: BLE001 — never let one frame kill the worker
        return _archive_retry_or_fail(frame_id, attempts, f"unexpected: {type(e).__name__}: {e}")


def _archive_retry_or_fail(frame_id: str, attempts: int, error: str) -> dict:
    if attempts >= MAX_ATTEMPTS:
        _store.archive_failed(frame_id, error)
        return {"frame_id": frame_id, "terminal": error, "attempts": attempts}
    _store.archive_requeue(frame_id, error)
    return {"frame_id": frame_id, "requeued": error, "attempts": attempts}

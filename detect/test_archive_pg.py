"""End-to-end tests for keeping clear-sky FITS, against real Postgres, MinIO and
Redis (same setup as test_tasks_pg.py; the database needs migration 0006).

What must hold:
- clear-sky frames are kept, compressed, and decompress to the exact original;
- cloudy, unscored and failed frames are never kept;
- nothing is ever archived from bytes that differ from what was uploaded;
- the claim is safe under concurrency, and failures end, rather than loop.
"""
import hashlib
import io
import os
import threading
import uuid

import numpy as np
import pytest
from astropy.io import fits

import tasks_pg
from fits_archive import decompress
from test_fits_archive import camera_fits
from test_tasks_pg import make_jpeg, put_preview

BUCKET = os.environ.get("S3_BUCKET", "skycam-demo")


@pytest.fixture(autouse=True, scope="module")
def clean_broker():
    tasks_pg.app.control.purge()
    yield
    tasks_pg.app.control.purge()


@pytest.fixture(scope="module")
def store():
    s = tasks_pg._store
    with s.pool.connection() as conn:
        name = conn.execute("SELECT current_database()").fetchone()[0]
    assert "test" in name, f"refusing to truncate {name!r}"
    return s


@pytest.fixture
def device(store):
    with store.pool.connection() as conn:
        conn.execute("TRUNCATE frames CASCADE")
    yield f"t-{uuid.uuid4().hex[:12]}"


def add_fits_frame(store, device, *, raw: bytes | None = None, status="scored",
                   cloudy=False, age_s=0, upload=True, recorded_sha=None):
    """A frame with a FITS, as Go ingest leaves it, already through detection."""
    raw = camera_fits(seed=hash(device) % 1000) if raw is None else raw
    fid = str(uuid.uuid4())
    key = f"frames/{device}/{fid}.fits"
    if upload:
        tasks_pg._s3.put_object(Bucket=BUCKET, Key=key, Body=io.BytesIO(raw))
    sha = recorded_sha or hashlib.sha256(raw).digest()
    with store.pool.connection() as conn:
        conn.execute(
            """INSERT INTO frames (frame_id, device_id, captured_at, received_at,
                                   fits_key, fits_size_bytes, fits_sha256,
                                   detect_status, is_cloudy)
               VALUES (%s, %s, now(), now() - make_interval(secs => %s), %s, %s, %s, %s, %s)""",
            (fid, device, age_s, key, len(raw), sha, status,
             None if status != "scored" else cloudy),
        )
    return fid, key, raw


def run_archive(store, limit=50):
    """What Beat + a worker do: claim, then run each job."""
    jobs = store.claim_archive(limit, tasks_pg.ARCHIVE_WINDOW_S)
    return [tasks_pg.archive_fits(*j) for j in jobs]


def stored(key: str) -> tuple[bytes, dict]:
    obj = tasks_pg._s3.get_object(Bucket=BUCKET, Key=key)
    return obj["Body"].read(), obj["Metadata"]


# -- the point of the feature ------------------------------------------------
def test_clear_frame_is_kept_compressed_and_exactly_recoverable(store, device):
    fid, _, raw = add_fits_frame(store, device, cloudy=False)

    out = run_archive(store)

    assert len(out) == 1 and out[0]["compression"] == "GZIP_2"
    row = store.get_archive(fid)
    assert row["status"] == "archived"
    assert row["archive_key"] == f"archive/frames/{device}/{fid}.fits.fz"
    assert row["stored_bytes"] < row["original_bytes"] / 1.5

    body, meta = stored(row["archive_key"])
    assert meta["original-sha256"] == hashlib.sha256(raw).hexdigest()
    with fits.open(io.BytesIO(raw)) as h:
        original = np.array(h[0].data)
    back, header = decompress(body)
    assert back.dtype == original.dtype and np.array_equal(back, original)
    assert header["INSTRUME"] == "ZWO ASI676MC"


def test_cloudy_frame_is_never_kept(store, device):
    fid, _, _ = add_fits_frame(store, device, cloudy=True)
    assert run_archive(store) == []
    assert store.get_archive(fid) is None


@pytest.mark.parametrize("status", ["pending", "claimed", "failed", "skipped"])
def test_frames_without_a_clear_verdict_are_never_kept(store, device, status):
    # No verdict means no evidence the sky was clear. The raw file expires.
    fid, _, _ = add_fits_frame(store, device, status=status)
    assert run_archive(store) == []
    assert store.get_archive(fid) is None


def test_archive_lives_outside_the_raw_lifecycle_prefix(store, device):
    # The frames/ rule expires raw files after a day; an archive under frames/
    # would be deleted with them.
    add_fits_frame(store, device)
    key = run_archive(store)[0]["key"]
    assert key.startswith("archive/") and not key.startswith("frames/")


def test_detection_then_archive_end_to_end(store, device):
    """Detect a clear preview, then archive the same frame's FITS."""
    fid = str(uuid.uuid4())
    preview_key = f"previews/{device}/{fid}.jpg"
    put_preview(preview_key, make_jpeg(stars=400))
    raw = camera_fits(seed=7)
    fits_key = f"frames/{device}/{fid}.fits"
    tasks_pg._s3.put_object(Bucket=BUCKET, Key=fits_key, Body=io.BytesIO(raw))
    with store.pool.connection() as conn:
        conn.execute(
            """INSERT INTO frames (frame_id, device_id, captured_at, preview_key,
                                   fits_key, fits_size_bytes, fits_sha256)
               VALUES (%s, %s, now(), %s, %s, %s, %s)""",
            (fid, device, preview_key, fits_key, len(raw), hashlib.sha256(raw).digest()),
        )
    assert run_archive(store) == [], "must not archive before detection has spoken"

    tasks_pg.detect_frame(fid, device, preview_key, 1)
    assert store.get_status(fid)["is_cloudy"] is False

    out = run_archive(store)
    assert [o["frame_id"] for o in out] == [fid]
    assert store.get_archive(fid)["status"] == "archived"


# -- never archive the wrong bytes, never lose a clear frame ------------------
def test_raw_already_expired_fails_once_without_retrying(store, device):
    fid, _, _ = add_fits_frame(store, device, upload=False)
    out = run_archive(store)
    assert "no longer in storage" in out[0]["terminal"]
    assert store.get_archive(fid)["status"] == "failed"
    assert run_archive(store) == [], "a failed archive must not be claimed again"


def test_bytes_that_differ_from_the_upload_are_not_archived(store, device):
    fid, _, _ = add_fits_frame(store, device, recorded_sha=b"\x00" * 32)
    out = run_archive(store)
    assert "SHA-256" in out[0]["terminal"]
    row = store.get_archive(fid)
    assert row["status"] == "failed" and row["archive_key"] is None


def test_clear_frame_that_is_not_a_fits_is_still_kept(store, device):
    junk = bytes(range(256)) * 4096
    fid, _, _ = add_fits_frame(store, device, raw=junk)
    out = run_archive(store)
    assert out[0]["compression"] == "none"
    row = store.get_archive(fid)
    assert row["status"] == "archived" and row["archive_key"].endswith(".fits")
    assert "not a readable FITS" in row["error"]
    assert stored(row["archive_key"])[0] == junk


def test_frame_older_than_the_window_is_not_attempted(store, device):
    # Its raw file may already have been expired by the lifecycle rule.
    fid, _, _ = add_fits_frame(store, device, age_s=tasks_pg.ARCHIVE_WINDOW_S + 3600)
    assert run_archive(store) == []
    assert store.get_archive(fid) is None


# -- idempotency, concurrency, failure handling --------------------------------
def test_redelivered_task_archives_once(store, device):
    fid, key, _ = add_fits_frame(store, device)
    job = store.claim_archive(10, tasks_pg.ARCHIVE_WINDOW_S)[0]
    tasks_pg.archive_fits(*job)
    first = store.get_archive(fid)["archived_at"]
    tasks_pg.archive_fits(*job)  # task_acks_late redelivery
    row = store.get_archive(fid)
    assert row["status"] == "archived" and row["archived_at"] == first


def test_two_pollers_never_claim_the_same_frame(store, device):
    ids = {add_fits_frame(store, device)[0] for _ in range(12)}
    got, barrier = [], threading.Barrier(4)

    def poll():
        barrier.wait()
        got.append([j.frame_id for j in store.claim_archive(12, tasks_pg.ARCHIVE_WINDOW_S)])

    threads = [threading.Thread(target=poll) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    flat = [f for g in got for f in g]
    assert len(flat) == len(set(flat)), "a frame was claimed twice"
    assert set(flat) == ids


def test_storage_outage_is_retried_then_dead_lettered(store, device, monkeypatch):
    fid, _, _ = add_fits_frame(store, device)
    monkeypatch.setattr(tasks_pg, "S3_BUCKET", "no-such-bucket-" + uuid.uuid4().hex[:8])

    out = run_archive(store)  # attempt 1: transient, requeued
    assert "requeued" in out[0]
    assert store.get_archive(fid)["status"] == "pending"

    for _ in range(tasks_pg.MAX_ATTEMPTS - 1):
        out = run_archive(store)
    assert "terminal" in out[0]
    row = store.get_archive(fid)
    assert row["status"] == "failed" and row["attempts"] == tasks_pg.MAX_ATTEMPTS


def test_abandoned_claim_is_returned_by_the_sweep(store, device):
    fid, _, _ = add_fits_frame(store, device)
    store.claim_archive(10, tasks_pg.ARCHIVE_WINDOW_S)  # claimed, worker "dies"
    assert store.reclaim_stale_archive(older_than_s=0) == 1
    assert store.get_archive(fid)["status"] == "pending"
    run_archive(store)
    assert store.get_archive(fid)["status"] == "archived"


def test_scan_archive_claims_and_enqueues(store, device):
    add_fits_frame(store, device)
    add_fits_frame(store, device, cloudy=True)
    assert tasks_pg.scan_archive() == {"claimed": 1}

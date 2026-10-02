"""End-to-end tests for the Postgres detection tasks.

Real Postgres, real MinIO, real OpenCV decode, real `analyze()`, and a real Redis
broker — `scan_pending` publishes for real, which is why these tests need their
own Redis database.

`detect_frame` is invoked directly rather than consumed off the queue, because
what is under test is the task body and its failure classification, not Celery's
ability to deliver a message. A worker consuming the queue is exercised
separately, by running one (see README).

    DETECT_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/skycam_detect_test \
    S3_ENDPOINT=http://localhost:9000 S3_BUCKET=skycam-demo \
    S3_ACCESS_KEY=minioadmin S3_SECRET_KEY=minioadmin \
    DETECT_BACKEND=postgres python -m pytest detect/test_tasks_pg.py -v
"""
import io
import os
import uuid

import pytest

import tasks_pg

# DATABASE_URL and REDIS_URL come from conftest.py, which pytest imports before
# any test module. Setting them at this module's top level would be too late: if
# another test module imported store_postgres first, its pool would already be
# pointing at whatever DATABASE_URL was then — which is how a run of this suite
# once aimed the task tests at the demo database.
#
# `scan_pending` publishes real Celery messages, so the tests get their own Redis
# database (15). Sharing db 0 with a running worker left orphaned tasks carrying
# frame_ids from the test database, which a worker pointed at another database
# then tried to process — foreign key violations from a completely unrelated run.

BUCKET = os.environ.get("S3_BUCKET", "skycam-demo")


@pytest.fixture(autouse=True, scope="module")
def clean_broker():
    """Leave no messages behind: an orphaned task outlives the test database."""
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


def make_jpeg(stars: int, size: int = 256) -> bytes:
    """A starfield, drawn the way fake_pi draws one, so `analyze()` sees real
    point sources rather than noise."""
    import random

    import cv2
    import numpy as np

    img = np.full((size, size, 3), (18, 8, 6), dtype=np.uint8)  # BGR, near-black
    rng = random.Random(stars)
    for _ in range(stars):
        x, y = rng.randrange(4, size - 4), rng.randrange(4, size - 4)
        cv2.circle(img, (x, y), 1, (255, 255, 255), -1)
    ok, buf = cv2.imencode(".jpg", img, [cv2.IMWRITE_JPEG_QUALITY, 92])
    assert ok
    return buf.tobytes()


def put_preview(key: str, body: bytes):
    tasks_pg._s3.put_object(Bucket=BUCKET, Key=key, Body=io.BytesIO(body),
                            ContentType="image/jpeg")


def add_frame(store, device, *, preview_key=None):
    fid = str(uuid.uuid4())
    with store.pool.connection() as conn:
        conn.execute(
            """INSERT INTO frames (frame_id, device_id, captured_at, preview_key)
               VALUES (%s, %s, now(), %s)""",
            (fid, device, preview_key),
        )
    return fid


# -- the happy path ---------------------------------------------------------
def test_clear_sky_scores_low_and_raises_no_alarm(store, device):
    key = f"previews/{device}/clear.jpg"
    put_preview(key, make_jpeg(stars=400))
    fid = add_frame(store, device, preview_key=key)

    out = tasks_pg.detect_frame(fid, device, key, 1)

    st = store.get_status(fid)
    assert st["detect_status"] == "scored"
    assert st["star_count"] > 0, "a starfield must register stars"
    assert out["alarm_raised"] is False
    assert st["is_cloudy"] is False
    with store.pool.connection() as conn:
        assert conn.execute("SELECT count(*) FROM alarms").fetchone()[0] == 0


def test_starless_sky_is_cloudy_and_raises_one_alarm(store, device):
    key = f"previews/{device}/cloudy.jpg"
    put_preview(key, make_jpeg(stars=0))
    fid = add_frame(store, device, preview_key=key)

    out = tasks_pg.detect_frame(fid, device, key, 1)

    st = store.get_status(fid)
    assert st["detect_status"] == "scored"
    assert st["is_cloudy"] is True
    assert out["alarm_raised"] is True
    with store.pool.connection() as conn:
        row = conn.execute(
            "SELECT device_id, kind, cloud_score FROM alarms WHERE frame_id = %s", (fid,)
        ).fetchone()
    assert row[0] == device and row[1] == "cloudy"


def test_redelivery_does_not_duplicate_the_alarm(store, device):
    """task_acks_late: a worker that dies after analysing gets the task again."""
    key = f"previews/{device}/cloudy2.jpg"
    put_preview(key, make_jpeg(stars=0))
    fid = add_frame(store, device, preview_key=key)

    first = tasks_pg.detect_frame(fid, device, key, 1)
    second = tasks_pg.detect_frame(fid, device, key, 1)

    assert first["alarm_raised"] is True
    assert second["alarm_raised"] is False, "the second run must not re-raise"
    with store.pool.connection() as conn:
        assert conn.execute("SELECT count(*) FROM alarms WHERE frame_id=%s",
                            (fid,)).fetchone()[0] == 1


# -- failure classification -------------------------------------------------
def test_missing_preview_is_skipped_permanently(store, device):
    fid = add_frame(store, device, preview_key=None)

    tasks_pg.detect_frame(fid, device, None, 1)

    st = store.get_status(fid)
    assert st["detect_status"] == "skipped"
    # The poison-pill test: it must not come back on the next poll.
    assert store.claim_pending(10) == []


def test_undecodable_preview_fails_immediately_without_burning_retries(store, device):
    """The upload was SHA-256 verified, so a preview that will not decode is
    permanently bad. Retrying it three times would be pure waste."""
    key = f"previews/{device}/garbage.jpg"
    put_preview(key, b"this is not a JPEG")
    fid = add_frame(store, device, preview_key=key)

    out = tasks_pg.detect_frame(fid, device, key, 1)

    st = store.get_status(fid)
    assert st["detect_status"] == "failed"
    assert "decode" in st["detect_error"]
    assert "terminal" in out
    assert store.claim_pending(10) == []


def test_missing_object_is_transient_and_requeued(store, device):
    """S3 404 could be eventual consistency or a key not yet replicated, so it
    gets an attempt rather than a death sentence."""
    key = f"previews/{device}/does-not-exist.jpg"
    fid = add_frame(store, device, preview_key=key)

    out = tasks_pg.detect_frame(fid, device, key, 1)

    st = store.get_status(fid)
    assert st["detect_status"] == "pending", "must go back in the queue"
    assert "requeued" in out
    assert "storage" in st["detect_error"]


def test_transient_failure_becomes_terminal_once_the_budget_is_spent(store, device):
    key = f"previews/{device}/still-missing.jpg"
    fid = add_frame(store, device, preview_key=key)

    tasks_pg.detect_frame(fid, device, key, tasks_pg.MAX_ATTEMPTS)

    st = store.get_status(fid)
    assert st["detect_status"] == "failed"
    assert store.claim_pending(10) == [], "a dead-lettered frame stops being claimed"


def test_one_bad_frame_does_not_block_the_good_ones(store, device):
    """The Mongo version starved: SCAN_LIMIT bad frames filled every batch
    forever. Here a bad frame reaches a terminal state and leaves the index."""
    bad = []
    for i in range(3):
        key = f"previews/{device}/bad{i}.jpg"
        put_preview(key, b"not a jpeg")
        bad.append((add_frame(store, device, preview_key=key), key))
    good_key = f"previews/{device}/good.jpg"
    put_preview(good_key, make_jpeg(stars=400))
    good = add_frame(store, device, preview_key=good_key)

    for fid, key in bad:
        tasks_pg.detect_frame(fid, device, key, 1)
    tasks_pg.detect_frame(good, device, good_key, 1)

    assert store.backlog() == {"failed": 3, "scored": 1}
    assert store.claim_pending(10) == []


def test_frame_deleted_while_queued_is_a_noop(store, device):
    """Found by running the worker for real: a task holding a frame_id that no
    longer exists used to reach the alarm insert and blow up on the foreign key,
    get classified as 'unexpected', and be retried against a row that cannot
    come back."""
    key = f"previews/{device}/deleted.jpg"
    put_preview(key, make_jpeg(stars=0))  # cloudy, so it would try to alarm
    fid = add_frame(store, device, preview_key=key)
    with store.pool.connection() as conn:
        conn.execute("DELETE FROM frames WHERE frame_id = %s", (fid,))

    out = tasks_pg.detect_frame(fid, device, key, 1)

    assert out == {"frame_id": fid, "gone": True}
    with store.pool.connection() as conn:
        assert conn.execute("SELECT count(*) FROM alarms").fetchone()[0] == 0


# -- the poller ------------------------------------------------------------
def test_scan_pending_claims_and_leaves_nothing_behind(store, device):
    keys = []
    for i in range(4):
        key = f"previews/{device}/scan{i}.jpg"
        put_preview(key, make_jpeg(stars=400 if i % 2 else 0))
        keys.append(key)
        add_frame(store, device, preview_key=key)

    assert tasks_pg.scan_pending() == {"claimed": 4}
    # Nothing left pending, and a second scan finds nothing to do.
    assert tasks_pg.scan_pending() == {"claimed": 0}
    assert store.backlog() == {"claimed": 4}


def test_reclaim_returns_an_abandoned_claim(store, device):
    key = f"previews/{device}/abandoned.jpg"
    put_preview(key, make_jpeg(stars=400))
    fid = add_frame(store, device, preview_key=key)
    tasks_pg.scan_pending()  # claimed, then the "worker dies"

    with store.pool.connection() as conn:
        conn.execute("UPDATE frames SET claimed_at = now() - interval '1 hour' "
                     "WHERE frame_id = %s", (fid,))
    assert tasks_pg.reclaim_stale()["reclaimed"] == 1
    assert store.get_status(fid)["detect_status"] == "pending"

    # And it now completes normally on the retry.
    tasks_pg.detect_frame(fid, device, key, 2)
    assert store.get_status(fid)["detect_status"] == "scored"

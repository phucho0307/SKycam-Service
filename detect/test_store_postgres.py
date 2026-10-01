"""Tests for the detect worker's Postgres layer, against a real database.

No mocks: every behaviour under test is a property of the SQL (SKIP LOCKED,
ON CONFLICT, partial indexes), and a mock would assert my beliefs about Postgres
rather than Postgres.

    # a DISPOSABLE database — the frames table is truncated between tests
    createdb skycam_detect_test && ingest migrate
    DETECT_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/skycam_detect_test \
        python -m pytest detect/test_store_postgres.py -v

**Point this at a throwaway database, never at one with real frames in it.**
`claim_pending` is deliberately global — it claims the oldest pending frames in
the whole table, because that is what a poller must do — so a test that ran
against a populated database would claim (and mutate) rows it does not own, and
would never see its own. The fixture truncates instead of filtering, so what is
tested is the real query rather than a test-only variant of it.
"""
import os
import uuid

import pytest

from store_postgres import PostgresStore

DSN = os.environ.get(
    "DETECT_TEST_DATABASE_URL",
    "postgres://postgres:postgres@localhost:55432/skycam_detect_test",
)


@pytest.fixture(scope="module")
def store():
    s = PostgresStore(DSN)
    with s.pool.connection() as conn:
        name = conn.execute("SELECT current_database()").fetchone()[0]
    # Cheap guard against pointing this at the demo or a real database.
    assert "test" in name, f"refusing to truncate {name!r}: use a test database"
    yield s
    s.close()


@pytest.fixture
def device(store):
    """Empty table, then a device id unique to this test.

    Truncating is what makes the assertions about ordering and claim counts
    meaningful: `claim_pending` takes the globally oldest pending rows, so any
    leftover frame would be claimed ahead of this test's.
    """
    with store.pool.connection() as conn:
        conn.execute("TRUNCATE frames CASCADE")  # cascades to alarms
    yield f"t-{uuid.uuid4().hex[:12]}"


def add_frame(store, device, *, preview=True, status="pending", offset_s=0):
    fid = str(uuid.uuid4())
    with store.pool.connection() as conn:
        conn.execute(
            """INSERT INTO frames (frame_id, device_id, captured_at, received_at,
                                   preview_key, detect_status)
               VALUES (%s, %s, now(), now() + make_interval(secs => %s), %s, %s)""",
            (fid, device, offset_s, f"previews/{device}/{fid}.jpg" if preview else None, status),
        )
    return fid


def claim_for(store, device, limit=10):
    """The table is empty per test, so a plain claim returns only its rows."""
    return store.claim_pending(limit)


# -- claiming ---------------------------------------------------------------
def test_claim_marks_rows_claimed_and_counts_the_attempt(store, device):
    fid = add_frame(store, device)
    claimed = claim_for(store, device)

    assert [f.frame_id for f in claimed] == [fid]
    assert claimed[0].attempts == 1
    st = store.get_status(fid)
    assert st["detect_status"] == "claimed"
    assert st["detect_attempts"] == 1
    assert st["claimed_at"] is not None


def test_claim_returns_oldest_first(store, device):
    newer = add_frame(store, device, offset_s=10)
    older = add_frame(store, device, offset_s=0)
    claimed = claim_for(store, device)
    assert [f.frame_id for f in claimed] == [older, newer]


def test_claimed_rows_are_not_claimed_again(store, device):
    add_frame(store, device)
    assert len(claim_for(store, device)) == 1
    # The whole point: a frame in flight must not be handed out a second time.
    assert claim_for(store, device) == []


def test_terminal_states_are_never_claimed(store, device):
    for status in ("scored", "failed", "skipped"):
        add_frame(store, device, status=status)
    assert claim_for(store, device) == []


def test_two_pollers_get_disjoint_sets(store, device):
    """SKIP LOCKED: concurrent pollers must not block or overlap."""
    ids = {add_frame(store, device) for _ in range(6)}

    # Hold a transaction open mid-claim so the second poller sees locked rows.
    with store.pool.connection() as conn_a:
        conn_a.autocommit = False
        rows_a = conn_a.execute(
            """WITH c AS (SELECT frame_id FROM frames
                           WHERE detect_status='pending' AND device_id=%s
                           ORDER BY received_at LIMIT 3 FOR UPDATE SKIP LOCKED)
               UPDATE frames f SET detect_status='claimed', claimed_at=now(),
                                   detect_attempts=f.detect_attempts+1
                 FROM c WHERE f.frame_id=c.frame_id
               RETURNING f.frame_id::text""",
            (device,),
        ).fetchall()
        a = {r[0] for r in rows_a}

        # Second poller, while the first transaction is still uncommitted.
        with store.pool.connection() as conn_b:
            rows_b = conn_b.execute(
                """WITH c AS (SELECT frame_id FROM frames
                               WHERE detect_status='pending' AND device_id=%s
                               ORDER BY received_at LIMIT 3 FOR UPDATE SKIP LOCKED)
                   UPDATE frames f SET detect_status='claimed', claimed_at=now(),
                                       detect_attempts=f.detect_attempts+1
                     FROM c WHERE f.frame_id=c.frame_id
                   RETURNING f.frame_id::text""",
                (device,),
            ).fetchall()
            b = {r[0] for r in rows_b}
        conn_a.commit()

    assert len(a) == 3 and len(b) == 3
    assert a.isdisjoint(b), "two pollers claimed the same frame"
    assert a | b == ids


# -- terminal transitions ---------------------------------------------------
def test_mark_scored_stores_every_measurement(store, device):
    fid = add_frame(store, device)
    claim_for(store, device)
    store.mark_scored(fid, {"cloud_score": 0.82, "is_cloudy": True, "star_count": 3,
                            "brightness": 41.5, "rb_ratio": 1.07})

    st = store.get_status(fid)
    assert st["detect_status"] == "scored"
    assert st["cloud_score"] == pytest.approx(0.82, abs=1e-6)
    assert st["is_cloudy"] is True
    assert st["star_count"] == 3
    assert st["claimed_at"] is None, "a scored frame must not look in-flight"


def test_skipped_frame_leaves_the_queue_for_good(store, device):
    """The poison-pill fix: no preview is permanent, so retrying is pointless."""
    fid = add_frame(store, device, preview=False)
    claim_for(store, device)
    store.mark_skipped(fid, "no preview")

    assert store.get_status(fid)["detect_status"] == "skipped"
    assert claim_for(store, device) == []


def test_requeue_keeps_the_attempt_count(store, device):
    fid = add_frame(store, device)
    claim_for(store, device)
    store.requeue(fid, "s3 timeout")

    st = store.get_status(fid)
    assert st["detect_status"] == "pending"
    assert st["detect_attempts"] == 1, "the retry budget must not reset"
    assert st["detect_error"] == "s3 timeout"

    # Picked up again, and the budget keeps shrinking.
    again = claim_for(store, device)
    assert [f.attempts for f in again] == [2]


def test_failed_frame_records_why_and_stops_retrying(store, device):
    fid = add_frame(store, device)
    claim_for(store, device)
    store.mark_failed(fid, "cv2 decode returned None")

    st = store.get_status(fid)
    assert st["detect_status"] == "failed"
    assert "decode" in st["detect_error"]
    assert claim_for(store, device) == []


def test_long_errors_are_truncated_not_rejected(store, device):
    fid = add_frame(store, device)
    store.mark_failed(fid, "x" * 5000)
    assert len(store.get_status(fid)["detect_error"]) == 500


# -- the reconciliation sweep ----------------------------------------------
def test_stale_claim_returns_to_pending(store, device):
    fid = add_frame(store, device)
    claim_for(store, device)

    assert store.reclaim_stale(older_than_s=3600) == 0, "a fresh claim is not stale"

    with store.pool.connection() as conn:
        conn.execute("UPDATE frames SET claimed_at = now() - interval '10 minutes' "
                     "WHERE frame_id = %s", (fid,))
    assert store.reclaim_stale(older_than_s=300) >= 1

    st = store.get_status(fid)
    assert st["detect_status"] == "pending"
    assert st["claimed_at"] is None
    # And it is genuinely back in the queue, on its second attempt.
    assert [f.attempts for f in claim_for(store, device)] == [2]


def test_repeatedly_abandoned_frame_is_failed_not_looped(store, device):
    """A worker that dies on the same frame every time is a poison pill. After
    the budget is spent the sweep must stop resurrecting it."""
    fid = add_frame(store, device)
    with store.pool.connection() as conn:
        conn.execute(
            """UPDATE frames SET detect_status='claimed', detect_attempts=3,
                                 claimed_at = now() - interval '1 hour'
                WHERE frame_id = %s""", (fid,))

    store.reclaim_stale(older_than_s=300)
    st = store.get_status(fid)
    assert st["detect_status"] == "failed"
    assert "abandoned" in st["detect_error"]


# -- alarms -----------------------------------------------------------------
def test_alarm_is_written_once_however_many_times_the_task_runs(store, device):
    fid = add_frame(store, device)
    assert store.raise_alarm(fid, device, 0.91) is True
    # Redelivery under task_acks_late: same frame, same alarm.
    assert store.raise_alarm(fid, device, 0.91) is False
    assert store.raise_alarm(fid, device, 0.95) is False

    with store.pool.connection() as conn:
        n = conn.execute("SELECT count(*) FROM alarms WHERE frame_id = %s", (fid,)).fetchone()[0]
    assert n == 1


def test_different_alarm_kinds_coexist(store, device):
    fid = add_frame(store, device)
    assert store.raise_alarm(fid, device, 0.9, kind="cloudy") is True
    assert store.raise_alarm(fid, device, 0.9, kind="rain") is True


def test_alarm_dies_with_its_frame(store, device):
    """ON DELETE CASCADE: no alarms pointing at frames that no longer exist."""
    fid = add_frame(store, device)
    store.raise_alarm(fid, device, 0.9)
    with store.pool.connection() as conn:
        conn.execute("DELETE FROM frames WHERE frame_id = %s", (fid,))
        n = conn.execute("SELECT count(*) FROM alarms WHERE frame_id = %s", (fid,)).fetchone()[0]
    assert n == 0


# -- the index that makes polling cheap ------------------------------------
def test_claim_query_uses_the_partial_index(store, device):
    """If this regresses to a sequential scan, the 'polling is fine' argument
    collapses — that was the real defect in the Mongo version, not polling."""
    add_frame(store, device)
    with store.pool.connection() as conn:
        plan = "\n".join(r[0] for r in conn.execute(
            """EXPLAIN SELECT frame_id FROM frames WHERE detect_status='pending'
                 ORDER BY received_at LIMIT 50 FOR UPDATE SKIP LOCKED""").fetchall())
    assert "frames_pending_detect_idx" in plan, plan
    assert "Seq Scan" not in plan, plan

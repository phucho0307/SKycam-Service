"""Postgres data access for the detect worker.

All SQL lives here so `tasks.py` holds Celery concerns and `detector.py` stays a
pure function. The schema is owned by the Go ingest service
(`services/ingest/internal/store/migrations/`); this module is a client of it.

Three design choices worth knowing:

1. **Claim, don't just read.** `claim_pending` marks rows `claimed` in the same
   statement that selects them, using `FOR UPDATE SKIP LOCKED`. Two pollers can
   run concurrently and will never hand the same frame to two workers. The Mongo
   version re-enqueued any frame a worker hadn't finished yet.

2. **Attempts live on the row, not in the broker.** The retry budget survives a
   broker flush, and "why did this frame never score" is answerable from the
   database months later via `detect_error`.

3. **Terminal states are explicit.** `scored`, `skipped` and `failed` all leave
   the pending index, so a frame that cannot be processed stops coming back.
   That is what kills the poison-pill loop: the Mongo query was "has no
   cloud_score", and the error path wrote nothing, so a bad frame was retried
   every 3 seconds forever.
"""
import os
from typing import NamedTuple

from psycopg_pool import ConnectionPool

DEFAULT_DSN = "postgres://postgres:postgres@localhost:55432/skycam_demo"
# How long a claimed frame may sit before the sweep assumes its worker died.
# Must exceed the task's own time limit, or the sweep will resurrect frames that
# are still being processed and duplicate the work.
STALE_CLAIM_S = int(os.environ.get("DETECT_STALE_CLAIM_S", "300"))
MAX_ATTEMPTS = int(os.environ.get("DETECT_MAX_ATTEMPTS", "3"))


class PendingFrame(NamedTuple):
    frame_id: str
    device_id: str
    preview_key: str | None
    attempts: int


class PostgresStore:
    def __init__(self, dsn: str | None = None, min_size: int = 1, max_size: int = 4):
        # Resolved here, not as a default argument: a default is evaluated when
        # this module is imported, which made the target database depend on
        # import order. Under pytest that silently pointed the task tests at the
        # demo database instead of the throwaway one.
        dsn = dsn or os.environ.get("DATABASE_URL", DEFAULT_DSN)
        # A pool, because Celery runs several worker processes and each holds
        # connections; an unpooled connect-per-task would churn them.
        self.pool = ConnectionPool(dsn, min_size=min_size, max_size=max_size, open=True)

    def close(self):
        self.pool.close()

    # -- the poll -----------------------------------------------------------
    def claim_pending(self, limit: int) -> list[PendingFrame]:
        """Atomically take up to `limit` pending frames and mark them claimed.

        `FOR UPDATE SKIP LOCKED` is what makes this safe to run concurrently:
        a row already locked by another poller is skipped rather than waited on,
        so two pollers get disjoint sets and neither blocks.
        """
        sql = """
        WITH claimed AS (
            SELECT frame_id
              FROM frames
             WHERE detect_status = 'pending'
             ORDER BY received_at
             LIMIT %s
             FOR UPDATE SKIP LOCKED
        )
        UPDATE frames f
           SET detect_status   = 'claimed',
               claimed_at      = now(),
               detect_attempts = f.detect_attempts + 1
          FROM claimed c
         WHERE f.frame_id = c.frame_id
        RETURNING f.frame_id::text, f.device_id, f.preview_key, f.detect_attempts
        """
        with self.pool.connection() as conn:
            rows = conn.execute(sql, (limit,)).fetchall()
        return [PendingFrame(*r) for r in rows]

    def reclaim_stale(self, older_than_s: int = STALE_CLAIM_S) -> int:
        """Return claims whose worker died to 'pending'.

        This is the reconciliation sweep. It is what makes the pipeline
        self-healing: any frame lost between claim and completion — worker OOM,
        pod eviction, broker flush — comes back on its own. Without it, a claimed
        row would sit invisible forever.

        Frames already over the attempt budget go straight to 'failed' instead of
        looping, since a worker that dies repeatedly on one frame is a poison
        pill rather than bad luck.
        """
        sql = """
        UPDATE frames
           SET detect_status = CASE WHEN detect_attempts >= %s THEN 'failed' ELSE 'pending' END,
               claimed_at    = NULL,
               detect_error  = CASE WHEN detect_attempts >= %s
                                    THEN 'abandoned: claimed but never completed'
                                    ELSE detect_error END
         WHERE detect_status = 'claimed'
           AND claimed_at < now() - make_interval(secs => %s)
        """
        with self.pool.connection() as conn:
            cur = conn.execute(sql, (MAX_ATTEMPTS, MAX_ATTEMPTS, older_than_s))
            return cur.rowcount

    # -- terminal states ----------------------------------------------------
    def mark_scored(self, frame_id: str, result: dict) -> bool:
        """Returns False if the frame no longer exists.

        A frame can be deleted between being claimed and being processed — a
        retention job, or an operator clearing bad data. That is not an error, but
        it must not be ignored either: writing an alarm for a deleted frame
        violates the foreign key, and the caller needs to know to stop.
        """
        with self.pool.connection() as conn:
            cur = conn.execute(
                """
                UPDATE frames
                   SET detect_status = 'scored',
                       cloud_score   = %(cloud_score)s,
                       is_cloudy     = %(is_cloudy)s,
                       star_count    = %(star_count)s,
                       brightness    = %(brightness)s,
                       rb_ratio      = %(rb_ratio)s,
                       claimed_at    = NULL,
                       detect_error  = NULL
                 WHERE frame_id = %(frame_id)s
                """,
                {"frame_id": frame_id, **{k: result[k] for k in
                 ("cloud_score", "is_cloudy", "star_count", "brightness", "rb_ratio")}},
            )
            return cur.rowcount == 1

    def mark_skipped(self, frame_id: str, reason: str) -> None:
        """Nothing to analyse, and retrying will not change that."""
        self._terminal(frame_id, "skipped", reason)

    def mark_failed(self, frame_id: str, error: str) -> None:
        """Out of retries. This is the dead letter: the row stays queryable with
        the reason attached, instead of the frame vanishing into a broker queue
        nobody reads."""
        self._terminal(frame_id, "failed", error)

    def _terminal(self, frame_id: str, status: str, error: str | None) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                """UPDATE frames SET detect_status = %s, detect_error = %s, claimed_at = NULL
                    WHERE frame_id = %s""",
                (status, (error or "")[:500], frame_id),
            )

    def requeue(self, frame_id: str, error: str) -> None:
        """A transient failure with retries left: back to 'pending' so the next
        poll picks it up. `detect_attempts` is NOT reset, so the budget shrinks."""
        with self.pool.connection() as conn:
            conn.execute(
                """UPDATE frames SET detect_status = 'pending', detect_error = %s, claimed_at = NULL
                    WHERE frame_id = %s""",
                (error[:500], frame_id),
            )

    # -- alarms -------------------------------------------------------------
    def raise_alarm(self, frame_id: str, device_id: str, cloud_score: float,
                    kind: str = "cloudy") -> bool:
        """Insert an alarm, or do nothing if one already exists.

        `ON CONFLICT DO NOTHING` against the unique (frame_id, kind) index is the
        whole fix for the duplicate-alarm bug: with task_acks_late a worker that
        dies after analysing but before acking gets the task redelivered, and a
        plain INSERT would raise the same alarm twice.

        Returns True if this call created the alarm.
        """
        with self.pool.connection() as conn:
            cur = conn.execute(
                """INSERT INTO alarms (frame_id, device_id, kind, cloud_score)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (frame_id, kind) DO NOTHING""",
                (frame_id, device_id, kind, cloud_score),
            )
            return cur.rowcount == 1

    # -- read side (for tests and diagnostics) ------------------------------
    def get_status(self, frame_id: str) -> dict | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                """SELECT detect_status, detect_attempts, detect_error, cloud_score,
                          is_cloudy, star_count, claimed_at
                     FROM frames WHERE frame_id = %s""",
                (frame_id,),
            ).fetchone()
        if row is None:
            return None
        return dict(zip(("detect_status", "detect_attempts", "detect_error", "cloud_score",
                         "is_cloudy", "star_count", "claimed_at"), row))

    def backlog(self) -> dict[str, int]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT detect_status, count(*) FROM frames GROUP BY detect_status"
            ).fetchall()
        return {status: n for status, n in rows}

"""Postgres access for the alerting pipeline.

Same three properties as `detect/store_postgres.py`, deliberately: claim rather
than read, attempts counted on the row, explicit terminal states. One pattern
across detection, forecasting and delivery rather than three.

The schedule lives here too — `forecast_sites.next_poll_at` — which is what makes
a duplicated scheduler harmless: two tickers both fire, one claims the rows, the
other gets none.
"""
from __future__ import annotations

import datetime as dt
import json
import os
from typing import NamedTuple

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DEFAULT_DSN = "postgres://postgres:postgres@localhost:55432/skycam_demo"
POLL_INTERVAL_S = int(os.environ.get("NOTIFY_POLL_INTERVAL_S", "600"))
MAX_DELIVERY_ATTEMPTS = int(os.environ.get("NOTIFY_MAX_DELIVERY_ATTEMPTS", "4"))
STALE_CLAIM_S = int(os.environ.get("NOTIFY_STALE_CLAIM_S", "300"))
# A site that has not produced a forecast for this long is itself an alarm: a dead
# poller means no alerts, and no alerts looks exactly like good weather.
STALE_FORECAST_S = int(os.environ.get("NOTIFY_STALE_FORECAST_S", "1800"))


class DueSite(NamedTuple):
    site_key: str
    latitude: float
    longitude: float
    last_payload_sha: str | None


class PendingDelivery(NamedTuple):
    id: int
    alert_id: int
    user_id: str
    channel: str
    target: str
    attempts: int
    device_id: str
    predicted_for: dt.datetime
    probability: float | None


class Store:
    def __init__(self, dsn: str | None = None, min_size: int = 1, max_size: int = 4):
        # Resolved at construction, not as a default argument: a default is
        # evaluated at import time, which makes the target database depend on
        # import order.
        dsn = dsn or os.environ.get("DATABASE_URL", DEFAULT_DSN)
        self.pool = ConnectionPool(dsn, min_size=min_size, max_size=max_size, open=True)

    def close(self):
        self.pool.close()

    # -- sites ---------------------------------------------------------------
    def sync_sites(self) -> int:
        """Create a forecast_sites row for every distinct located device.

        Grouping by rounded coordinates in SQL rather than in Python means a new
        camera at an existing observatory joins that site automatically, with no
        migration step and no duplicate forecast calls.
        """
        with self.pool.connection() as conn:
            cur = conn.execute("""
                INSERT INTO forecast_sites (site_key, latitude, longitude)
                SELECT site_key, avg(latitude), avg(longitude)
                  FROM devices
                 WHERE site_key IS NOT NULL
                 GROUP BY site_key
                ON CONFLICT (site_key) DO NOTHING
            """)
            return cur.rowcount

    def claim_due_sites(self, limit: int) -> list[DueSite]:
        """Take up to `limit` sites whose next poll is due.

        SKIP LOCKED is what lets this run on several workers, and what makes a
        duplicated scheduler a no-op instead of a double API call.
        """
        with self.pool.connection() as conn:
            rows = conn.execute("""
                WITH due AS (
                    SELECT site_key FROM forecast_sites
                     WHERE next_poll_at <= now()
                     ORDER BY next_poll_at
                     LIMIT %s
                     FOR UPDATE SKIP LOCKED
                )
                UPDATE forecast_sites f
                   SET last_polled_at = now(),
                       -- Pushed out immediately, so a crash mid-fetch cannot make
                       -- this site spin: it waits one interval like any other.
                       next_poll_at = now() + make_interval(secs => %s)
                  FROM due d
                 WHERE f.site_key = d.site_key
                RETURNING f.site_key, f.latitude, f.longitude, f.last_payload_sha
            """, (limit, POLL_INTERVAL_S)).fetchall()
        return [DueSite(*r) for r in rows]

    def mark_site_success(self, site_key: str, payload_sha: str) -> None:
        with self.pool.connection() as conn:
            conn.execute("""
                UPDATE forecast_sites
                   SET last_success_at = now(), consecutive_failures = 0,
                       last_error = NULL, last_payload_sha = %s
                 WHERE site_key = %s
            """, (payload_sha, site_key))

    def mark_site_failure(self, site_key: str, error: str) -> None:
        """Back off this site alone, capped so it never drops off the schedule."""
        with self.pool.connection() as conn:
            conn.execute("""
                UPDATE forecast_sites
                   SET consecutive_failures = consecutive_failures + 1,
                       last_error = %s,
                       next_poll_at = now() + make_interval(
                           secs => least(%s * power(2, least(consecutive_failures, 5)), 3600))
                 WHERE site_key = %s
            """, (error[:500], 60, site_key))

    def record_observation(self, site_key: str, provider: str, payload: dict) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "INSERT INTO forecast_observations (site_key, provider, payload) VALUES (%s,%s,%s)",
                (site_key, provider, json.dumps(payload)))

    def stale_sites(self, older_than_s: int = STALE_FORECAST_S) -> list[str]:
        """Sites with no successful forecast recently — monitoring the monitor."""
        with self.pool.connection() as conn:
            rows = conn.execute("""
                SELECT site_key FROM forecast_sites
                 WHERE last_success_at IS NULL
                    OR last_success_at < now() - make_interval(secs => %s)
            """, (older_than_s,)).fetchall()
        return [r[0] for r in rows]

    def devices_at_site(self, site_key: str) -> list[str]:
        with self.pool.connection() as conn:
            rows = conn.execute("""
                SELECT device_id FROM devices WHERE site_key = %s
            """, (site_key,)).fetchall()
        return [r[0] for r in rows]

    # -- alerts --------------------------------------------------------------
    def open_alert(self, device_id: str, kind: str) -> dict | None:
        with self.pool.connection() as conn:
            # A cursor-scoped row factory, NOT conn.row_factory: the connection is
            # pooled, so mutating it leaks dict rows to whoever borrows it next and
            # every tuple-unpacking query then fails with KeyError.
            with conn.cursor(row_factory=dict_row) as cur:
                return cur.execute("""
                    SELECT id, fingerprint, predicted_for, probability
                      FROM alerts
                     WHERE device_id = %s AND kind = %s AND state = 'firing'
                     ORDER BY raised_at DESC LIMIT 1
                """, (device_id, kind)).fetchone()

    def raise_alert(self, device_id: str, kind: str, fingerprint: str,
                    predicted_for: dt.datetime, probability: float,
                    payload: dict) -> int | None:
        """Insert an alert, or return None if this fingerprint already exists.

        ON CONFLICT is the deduplication. A forecast re-evaluated every 10 minutes
        crosses the threshold repeatedly; without this, each crossing is another
        email. Durable rather than a Redis TTL key, because losing the key means
        spamming a user.
        """
        with self.pool.connection() as conn:
            row = conn.execute("""
                INSERT INTO alerts (device_id, kind, fingerprint, predicted_for,
                                    probability, payload)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (fingerprint) DO NOTHING
                RETURNING id
            """, (device_id, kind, fingerprint, predicted_for, probability,
                  json.dumps(payload))).fetchone()
        return row[0] if row else None

    def resolve_alert(self, alert_id: int) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "UPDATE alerts SET state='resolved', resolved_at=now() WHERE id=%s AND state='firing'",
                (alert_id,))

    def acknowledge_alert(self, alert_id: int, user_id: str) -> bool:
        with self.pool.connection() as conn:
            cur = conn.execute("""
                UPDATE alerts SET state='acknowledged', acknowledged_at=now(), acknowledged_by=%s
                 WHERE id=%s AND state='firing'
            """, (user_id, alert_id))
            return cur.rowcount == 1

    # -- fan-out -------------------------------------------------------------
    def fan_out(self, alert_id: int, device_id: str) -> int:
        """Create one delivery per user who may act on this device.

        Authorization decides the audience: `device_grants` already answers "who
        cares about this camera". Viewers are excluded — they cannot close a dome,
        so waking them is noise. Done as a single INSERT..SELECT so it is one
        round trip and idempotent via the unique constraint: re-running after a
        crash inserts nothing new.
        """
        with self.pool.connection() as conn:
            cur = conn.execute("""
                INSERT INTO notification_deliveries (alert_id, user_id, channel, target)
                SELECT %s, g.user_id, 'email',
                       coalesce(c.target, u.email)
                  FROM device_grants g
                  JOIN users u ON u.user_id = g.user_id
                  LEFT JOIN notification_channels c
                         ON c.user_id = g.user_id AND c.channel = 'email'
                 WHERE g.device_id = %s
                   AND g.role IN ('operator', 'admin')
                   AND u.disabled_at IS NULL
                   -- A hard-bounced address is skipped: continuing to send to a
                   -- dead mailbox wrecks the sending domain's reputation, and
                   -- then nothing is delivered to anyone.
                   AND (c.undeliverable_at IS NULL)
                   AND coalesce(c.target, u.email) <> ''
                ON CONFLICT (alert_id, user_id, channel) DO NOTHING
            """, (alert_id, device_id))
            return cur.rowcount

    def claim_deliveries(self, user_limit: int) -> list[PendingDelivery]:
        """Claim every pending delivery for the next `user_limit` users.

        The batch unit is a **user, not a delivery**, and that is load-bearing.
        Claiming N deliveries splits one person's alerts across passes: at 200
        sites with a batch of 100, every user got two emails instead of one, and
        the ratio only worsens with scale. The unit of work here is an email, so
        the batch has to be counted in emails.

        Users are ordered by their most urgent alert, so when a front sweeps 200
        observatories the operator with 15 minutes of warning is mailed before the
        one with 55.
        """
        with self.pool.connection() as conn:
            rows = conn.execute("""
                WITH urgent_users AS (
                    SELECT d.user_id, min(a.predicted_for) AS soonest
                      FROM notification_deliveries d
                      JOIN alerts a ON a.id = d.alert_id
                     WHERE d.state = 'pending'
                     GROUP BY d.user_id
                     ORDER BY soonest
                     LIMIT %s
                ), due AS (
                    SELECT d.id
                      FROM notification_deliveries d
                      JOIN urgent_users u ON u.user_id = d.user_id
                     WHERE d.state = 'pending'
                     FOR UPDATE OF d SKIP LOCKED
                )
                UPDATE notification_deliveries d
                   SET state='claimed', claimed_at=now(), attempts = d.attempts + 1
                  FROM due
                 WHERE d.id = due.id
                RETURNING d.id, d.alert_id, d.user_id, d.channel, d.target, d.attempts,
                          (SELECT device_id FROM alerts WHERE id = d.alert_id),
                          (SELECT predicted_for FROM alerts WHERE id = d.alert_id),
                          (SELECT probability FROM alerts WHERE id = d.alert_id)
            """, (user_limit,)).fetchall()
        return [PendingDelivery(*r) for r in rows]

    def mark_sent(self, delivery_id: int) -> None:
        with self.pool.connection() as conn:
            conn.execute("""UPDATE notification_deliveries
                               SET state='sent', sent_at=now(), claimed_at=NULL, last_error=NULL
                             WHERE id=%s""", (delivery_id,))

    def requeue_delivery(self, delivery_id: int, error: str) -> None:
        with self.pool.connection() as conn:
            conn.execute("""UPDATE notification_deliveries
                               SET state='pending', claimed_at=NULL, last_error=%s
                             WHERE id=%s""", (error[:500], delivery_id))

    def fail_delivery(self, delivery_id: int, error: str) -> None:
        """Dead letter as a row state, so "why was this never sent" is a query."""
        with self.pool.connection() as conn:
            conn.execute("""UPDATE notification_deliveries
                               SET state='failed', claimed_at=NULL, last_error=%s
                             WHERE id=%s""", (error[:500], delivery_id))

    def reclaim_stale_deliveries(self, older_than_s: int = STALE_CLAIM_S) -> int:
        """Return deliveries whose worker died; dead-letter ones out of attempts."""
        with self.pool.connection() as conn:
            cur = conn.execute("""
                UPDATE notification_deliveries
                   SET state = CASE WHEN attempts >= %s THEN 'failed' ELSE 'pending' END,
                       claimed_at = NULL,
                       last_error = CASE WHEN attempts >= %s
                                         THEN 'abandoned: claimed but never sent'
                                         ELSE last_error END
                 WHERE state = 'claimed'
                   AND claimed_at < now() - make_interval(secs => %s)
            """, (MAX_DELIVERY_ATTEMPTS, MAX_DELIVERY_ATTEMPTS, older_than_s))
            return cur.rowcount

    def mark_undeliverable(self, user_id: str, target: str, reason: str) -> None:
        """Bounce handling. Without it, a dead address keeps being retried and the
        sending domain's reputation — the thing every other user depends on —
        degrades until nothing arrives."""
        with self.pool.connection() as conn:
            conn.execute("""
                INSERT INTO notification_channels (user_id, channel, target,
                                                   undeliverable_at, undeliverable_reason)
                VALUES (%s, 'email', %s, now(), %s)
                ON CONFLICT (user_id, channel) DO UPDATE
                   SET undeliverable_at = now(), undeliverable_reason = EXCLUDED.undeliverable_reason
            """, (user_id, target, reason[:500]))

    # -- read side -----------------------------------------------------------
    def delivery_states(self) -> dict[str, int]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT state, count(*) FROM notification_deliveries GROUP BY 1").fetchall()
        return {s: n for s, n in rows}

    def get_delivery(self, delivery_id: int) -> dict | None:
        with self.pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                return cur.execute(
                    "SELECT * FROM notification_deliveries WHERE id=%s",
                    (delivery_id,)).fetchone()

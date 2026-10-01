"""End-to-end alerting against real Postgres and a stub forecast provider.

Real database, real SQL (the claim queries are the interesting part), real task
bodies. Stubbed at the two edges that would otherwise need the internet: the
weather API and the mail server. Both are interface implementations rather than
mocks, so the code path under test is the production one.

    createdb skycam_notify_test && ingest migrate
    NOTIFY_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/skycam_notify_test \
        python -m pytest notify/test_pipeline.py -v
"""
from __future__ import annotations

import datetime as dt
import uuid

import pytest
import tasks
from mailer import FakeMailer, HardBounce, SoftFailure
from rules import HourlyPoint, site_key
from store import Store
from weather import ForecastError, StubProvider

NOW = dt.datetime.now(dt.timezone.utc)
LAT, LON = 51.4778, -0.0015


@pytest.fixture(scope="module")
def store():
    s = tasks._store
    with s.pool.connection() as conn:
        name = conn.execute("SELECT current_database()").fetchone()[0]
    assert "test" in name, f"refusing to truncate {name!r}: use a test database"
    return s


@pytest.fixture
def clean(store):
    """Empty every table these tests touch.

    Truncating rather than filtering, because the claim queries are deliberately
    global — they take the globally most urgent work, which is what a poller must
    do — so leftover rows would be claimed ahead of a test's own.
    """
    with store.pool.connection() as conn:
        conn.execute("TRUNCATE alerts, notification_deliveries, notification_channels, "
                     "forecast_observations, forecast_sites CASCADE")
        conn.execute("DELETE FROM device_grants")
        conn.execute("DELETE FROM users")
        conn.execute("DELETE FROM devices")
    yield


def add_device(store, lat=LAT, lon=LON) -> str:
    dev = "cam-" + uuid.uuid4().hex[:8]
    with store.pool.connection() as conn:
        conn.execute("""INSERT INTO devices (device_id, token_sha256, latitude, longitude)
                        VALUES (%s, %s, %s, %s)""",
                     (dev, b"x" * 32 + uuid.uuid4().bytes[:0] + uuid.uuid4().bytes, lat, lon))
    return dev


def add_user(store, device_id, role="operator", email=None) -> tuple[str, str]:
    uid = "u-" + uuid.uuid4().hex[:8]
    email = email or f"{uid}@example.org"
    with store.pool.connection() as conn:
        conn.execute("INSERT INTO users (user_id, email) VALUES (%s,%s)", (uid, email))
        conn.execute("""INSERT INTO device_grants (user_id, device_id, role)
                        VALUES (%s,%s,%s)""", (uid, device_id, role))
    return uid, email


def forecast(prob, minutes=45):
    return [HourlyPoint(time=NOW + dt.timedelta(minutes=minutes), precip_prob=prob)]


def use(provider=None, mailer=None):
    """Swap the module-level provider/mailer the tasks use."""
    if provider is not None:
        tasks._prov = provider
    if mailer is not None:
        tasks._mail = mailer
    return tasks._prov, tasks._mail


# -- the happy path ---------------------------------------------------------
def test_rain_forecast_reaches_the_users_inbox(store, clean):
    dev = add_device(store)
    uid, email = add_user(store, dev)
    use(StubProvider({site_key(LAT, LON): forecast(0.85)}), FakeMailer())

    assert tasks.sync_sites()["created"] == 1
    poll = tasks.poll_forecasts()
    assert poll["raised"] == 1, poll

    send = tasks.send_pending()
    assert send["sent"] == 1, send
    assert len(tasks._mail.sent) == 1

    to, subject, body = tasks._mail.sent[0]
    assert to == email
    assert "Rain expected" in subject
    assert dev in body
    assert store.delivery_states() == {"sent": 1}


def test_quiet_weather_raises_nothing(store, clean):
    dev = add_device(store)
    add_user(store, dev)
    use(StubProvider({site_key(LAT, LON): forecast(0.05)}), FakeMailer())

    tasks.sync_sites()
    assert tasks.poll_forecasts()["raised"] == 0
    assert tasks.send_pending()["claimed"] == 0
    assert tasks._mail.sent == []


# -- deduplication ----------------------------------------------------------
def test_re_polling_the_same_rain_does_not_send_twice(store, clean):
    """The forecast is re-evaluated every few minutes and keeps crossing the
    threshold. Without the fingerprint unique index that is an email each time."""
    dev = add_device(store)
    add_user(store, dev)
    use(StubProvider({site_key(LAT, LON): forecast(0.85)}), FakeMailer())

    tasks.sync_sites()
    tasks.poll_forecasts()
    tasks.send_pending()
    assert len(tasks._mail.sent) == 1

    # Force the site due again and re-poll with a *different* payload (so the
    # unchanged-hash shortcut is not what is being tested) but the same rain hour.
    with store.pool.connection() as conn:
        conn.execute("UPDATE forecast_sites SET next_poll_at = now(), last_payload_sha = NULL")
    tasks._prov.by_site = {site_key(LAT, LON): forecast(0.91)}

    second = tasks.poll_forecasts()
    assert second["raised"] == 0
    assert second["deduped"] == 1, second
    tasks.send_pending()
    assert len(tasks._mail.sent) == 1, "a second email was sent for the same rain"


def test_unchanged_forecast_skips_evaluation(store, clean):
    dev = add_device(store)
    add_user(store, dev)
    use(StubProvider({site_key(LAT, LON): forecast(0.05)}), FakeMailer())

    tasks.sync_sites()
    tasks.poll_forecasts()
    with store.pool.connection() as conn:
        conn.execute("UPDATE forecast_sites SET next_poll_at = now()")

    again = tasks.poll_forecasts()
    assert again["unchanged"] == 1, again


# -- coalescing -------------------------------------------------------------
def test_one_user_with_many_cameras_gets_one_email(store, clean):
    """The optimisation that turns 2,000 emails into ~500 at 200 sites."""
    uid = "u-" + uuid.uuid4().hex[:8]
    with store.pool.connection() as conn:
        conn.execute("INSERT INTO users (user_id, email) VALUES (%s,%s)",
                     (uid, f"{uid}@example.org"))
    devices = []
    for i in range(5):
        dev = add_device(store, LAT + i * 0.0001, LON)  # same rounded site
        devices.append(dev)
        with store.pool.connection() as conn:
            conn.execute("""INSERT INTO device_grants (user_id, device_id, role)
                            VALUES (%s,%s,'operator')""", (uid, dev))

    use(StubProvider({site_key(LAT, LON): forecast(0.85)}), FakeMailer())
    tasks.sync_sites()
    assert tasks.poll_forecasts()["raised"] == 5

    send = tasks.send_pending()
    assert send["sent"] == 5, send
    assert send["emails"] == 1, "five alerts for one person must be one email"

    _, subject, body = tasks._mail.sent[0]
    assert "5 sites" in subject
    for dev in devices:
        assert dev in body


def test_nearby_cameras_share_one_forecast_call(store, clean):
    """Grouping by rounded coordinates is what keeps 200 sites inside a free tier."""
    for i in range(4):
        add_device(store, LAT + i * 0.0001, LON)
    prov = StubProvider({site_key(LAT, LON): forecast(0.05)})
    use(prov, FakeMailer())

    tasks.sync_sites()
    tasks.poll_forecasts()
    assert len(prov.calls) == 1
    assert len(prov.calls[0]) == 1, "four cameras at one observatory must be one coordinate"


# -- authorization decides the audience -------------------------------------
def test_viewers_are_not_notified(store, clean):
    """A viewer cannot close a dome, so waking them is noise."""
    dev = add_device(store)
    add_user(store, dev, role="viewer")
    use(StubProvider({site_key(LAT, LON): forecast(0.85)}), FakeMailer())

    tasks.sync_sites()
    assert tasks.poll_forecasts()["raised"] == 1
    assert tasks.send_pending()["claimed"] == 0
    assert tasks._mail.sent == []


def test_users_of_other_devices_are_not_notified(store, clean):
    mine = add_device(store)
    theirs = add_device(store, 40.0, 10.0)
    _, my_email = add_user(store, mine)
    _, their_email = add_user(store, theirs)

    use(StubProvider({site_key(LAT, LON): forecast(0.85),
                      site_key(40.0, 10.0): forecast(0.01)}), FakeMailer())
    tasks.sync_sites()
    tasks.poll_forecasts()
    tasks.send_pending()

    recipients = {to for to, _, _ in tasks._mail.sent}
    assert recipients == {my_email}, recipients


def test_disabled_user_is_not_notified(store, clean):
    dev = add_device(store)
    uid, _ = add_user(store, dev)
    with store.pool.connection() as conn:
        conn.execute("UPDATE users SET disabled_at = now() WHERE user_id = %s", (uid,))

    use(StubProvider({site_key(LAT, LON): forecast(0.85)}), FakeMailer())
    tasks.sync_sites()
    tasks.poll_forecasts()
    assert tasks.send_pending()["claimed"] == 0


# -- delivery failure handling ----------------------------------------------
def test_soft_failure_is_retried(store, clean):
    dev = add_device(store)
    add_user(store, dev)
    use(StubProvider({site_key(LAT, LON): forecast(0.85)}),
        FakeMailer(fail_with=SoftFailure("smtp timeout")))

    tasks.sync_sites()
    tasks.poll_forecasts()
    out = tasks.send_pending()
    assert out["requeued"] == 1, out
    assert store.delivery_states() == {"pending": 1}

    # And it succeeds once the mail server recovers.
    use(mailer=FakeMailer())
    assert tasks.send_pending()["sent"] == 1
    assert store.delivery_states() == {"sent": 1}


def test_hard_bounce_stops_retrying_and_marks_the_address_dead(store, clean):
    """Continuing to send to a dead mailbox wrecks the sending domain's reputation,
    at which point nothing is delivered to anyone."""
    dev = add_device(store)
    uid, email = add_user(store, dev)
    use(StubProvider({site_key(LAT, LON): forecast(0.85)}),
        FakeMailer(fail_with=HardBounce("550 no such user")))

    tasks.sync_sites()
    tasks.poll_forecasts()
    out = tasks.send_pending()
    assert out["failed"] == 1, out
    assert store.delivery_states() == {"failed": 1}

    with store.pool.connection() as conn:
        row = conn.execute("""SELECT undeliverable_at IS NOT NULL, undeliverable_reason
                                FROM notification_channels WHERE user_id=%s""", (uid,)).fetchone()
    assert row and row[0] is True
    assert "550" in row[1]

    # A later alert skips that address entirely rather than burning attempts.
    with store.pool.connection() as conn:
        conn.execute("UPDATE forecast_sites SET next_poll_at=now(), last_payload_sha=NULL")
    tasks._prov.by_site = {site_key(LAT, LON): forecast(0.9, minutes=120)}
    tasks.poll_forecasts()
    assert tasks.send_pending()["claimed"] == 0


def test_delivery_is_dead_lettered_once_out_of_attempts(store, clean):
    dev = add_device(store)
    add_user(store, dev)
    use(StubProvider({site_key(LAT, LON): forecast(0.85)}),
        FakeMailer(fail_with=SoftFailure("still down")))

    tasks.sync_sites()
    tasks.poll_forecasts()
    for _ in range(10):
        tasks.send_pending()

    states = store.delivery_states()
    assert states == {"failed": 1}, states
    with store.pool.connection() as conn:
        err = conn.execute("SELECT last_error FROM notification_deliveries").fetchone()[0]
    assert "out of attempts" in err


def test_one_bad_address_does_not_block_the_others(store, clean):
    """The poison-pill lesson, applied to email."""
    dev = add_device(store)
    good_uid, good_email = add_user(store, dev)
    bad_uid, bad_email = add_user(store, dev)

    class Selective(FakeMailer):
        def send(self, to, subject, body):
            if to == bad_email:
                raise HardBounce("550 nope")
            self.sent.append((to, subject, body))

    use(StubProvider({site_key(LAT, LON): forecast(0.85)}), Selective())
    tasks.sync_sites()
    tasks.poll_forecasts()
    out = tasks.send_pending()

    assert out["sent"] == 1 and out["failed"] == 1, out
    assert [to for to, _, _ in tasks._mail.sent] == [good_email]


# -- the self-healing half --------------------------------------------------
def test_a_dead_worker_releases_its_claim(store, clean):
    dev = add_device(store)
    add_user(store, dev)
    use(StubProvider({site_key(LAT, LON): forecast(0.85)}), FakeMailer())
    tasks.sync_sites()
    tasks.poll_forecasts()

    claimed = store.claim_deliveries(10)
    assert len(claimed) == 1
    assert store.delivery_states() == {"claimed": 1}

    # The worker dies without finishing.
    with store.pool.connection() as conn:
        conn.execute("UPDATE notification_deliveries SET claimed_at = now() - interval '1 hour'")
    assert tasks.reclaim_stale()["reclaimed"] == 1
    assert store.delivery_states() == {"pending": 1}

    assert tasks.send_pending()["sent"] == 1


def test_two_pollers_claim_disjoint_sites(store, clean):
    """SKIP LOCKED: this is what makes a duplicated Beat a no-op rather than
    double API calls and double alerts."""
    for i in range(6):
        add_device(store, 10.0 + i, 20.0)
    tasks.sync_sites()

    with store.pool.connection() as a:
        a.autocommit = False
        first = a.execute("""
            WITH due AS (SELECT site_key FROM forecast_sites WHERE next_poll_at <= now()
                          ORDER BY next_poll_at LIMIT 3 FOR UPDATE SKIP LOCKED)
            UPDATE forecast_sites f SET next_poll_at = now() + interval '10 minutes'
              FROM due d WHERE f.site_key = d.site_key RETURNING f.site_key""").fetchall()
        with store.pool.connection() as b:
            second = b.execute("""
                WITH due AS (SELECT site_key FROM forecast_sites WHERE next_poll_at <= now()
                              ORDER BY next_poll_at LIMIT 3 FOR UPDATE SKIP LOCKED)
                UPDATE forecast_sites f SET next_poll_at = now() + interval '10 minutes'
                  FROM due d WHERE f.site_key = d.site_key RETURNING f.site_key""").fetchall()
        a.commit()

    got_a = {r[0] for r in first}
    got_b = {r[0] for r in second}
    assert len(got_a) == 3 and len(got_b) == 3
    assert got_a.isdisjoint(got_b), "two pollers claimed the same site"


# -- provider failure -------------------------------------------------------
def test_provider_failure_backs_off_without_losing_the_site(store, clean):
    add_device(store)
    use(StubProvider(fail_with=ForecastError("503 upstream")), FakeMailer())
    tasks.sync_sites()

    out = tasks.poll_forecasts()
    assert "error" in out
    with store.pool.connection() as conn:
        row = conn.execute("""SELECT consecutive_failures, last_error,
                                     next_poll_at > now()
                                FROM forecast_sites""").fetchone()
    assert row[0] == 1
    assert "503" in row[1]
    assert row[2] is True, "a failed site must be rescheduled, not dropped"


def test_staleness_is_detected(store, clean):
    """Monitoring the monitor: silence is the failure that actually hurts."""
    add_device(store)
    tasks.sync_sites()
    # Never polled successfully.
    assert tasks.check_staleness()["stale_sites"] == 1

    use(StubProvider({site_key(LAT, LON): forecast(0.01)}), FakeMailer())
    tasks.poll_forecasts()
    assert tasks.check_staleness()["stale_sites"] == 0

    with store.pool.connection() as conn:
        conn.execute("UPDATE forecast_sites SET last_success_at = now() - interval '2 hours'")
    assert tasks.check_staleness()["stale_sites"] == 1


def test_device_without_coordinates_is_not_polled(store, clean):
    dev = "cam-" + uuid.uuid4().hex[:8]
    with store.pool.connection() as conn:
        conn.execute("INSERT INTO devices (device_id, token_sha256) VALUES (%s,%s)",
                     (dev, uuid.uuid4().bytes * 2))
    assert tasks.sync_sites()["created"] == 0
    assert tasks.poll_forecasts()["sites"] == 0

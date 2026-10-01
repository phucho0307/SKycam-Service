"""Scale behaviour: 200 observatories all forecast rain at once.

The scenario that motivated the design - a frontal system sweeping a region, so
every site crosses the threshold inside one evaluation cycle. What this asserts is
not raw speed but that the two things which *would* break at scale do not:

  * forecast calls stay batched, because a free provider tier is ~10k/day and
    200 sites polled naively every 10 minutes is 28,800
  * emails stay coalesced per person, because that is the difference between
    ~500 sends and ~2,000

Run with: python -m pytest notify/test_scale.py -v -s
"""
from __future__ import annotations

import datetime as dt
import time
import uuid

import pytest
import tasks
from mailer import FakeMailer
from rules import HourlyPoint, site_key
from store import Store
from weather import StubProvider

SITES = 200
USERS = 50          # each watching several sites, as a real operator would
NOW = dt.datetime.now(dt.timezone.utc)


@pytest.fixture(scope="module")
def store() -> Store:
    s = tasks._store
    with s.pool.connection() as conn:
        name = conn.execute("SELECT current_database()").fetchone()[0]
    assert "test" in name, f"refusing to truncate {name!r}"
    return s


@pytest.fixture
def clean(store):
    with store.pool.connection() as conn:
        conn.execute("TRUNCATE alerts, notification_deliveries, notification_channels, "
                     "forecast_observations, forecast_sites CASCADE")
        conn.execute("DELETE FROM device_grants")
        conn.execute("DELETE FROM users")
        conn.execute("DELETE FROM devices")
    yield


def test_two_hundred_sites_raining_at_once(store, clean, capsys):
    # --- build the fleet -----------------------------------------------------
    setup = time.monotonic()
    coords = [(40.0 + i * 0.05, 10.0 + i * 0.05) for i in range(SITES)]
    devices = []
    with store.pool.connection() as conn:
        for lat, lon in coords:
            dev = "cam-" + uuid.uuid4().hex[:8]
            conn.execute("""INSERT INTO devices (device_id, token_sha256, latitude, longitude)
                            VALUES (%s,%s,%s,%s)""", (dev, uuid.uuid4().bytes * 2, lat, lon))
            devices.append(dev)

        users = []
        for _ in range(USERS):
            uid = "u-" + uuid.uuid4().hex[:8]
            conn.execute("INSERT INTO users (user_id, email) VALUES (%s,%s)",
                         (uid, f"{uid}@example.org"))
            users.append(uid)
        # Each user watches 4 sites, so every site has exactly one operator and
        # every operator has several sites - the case coalescing exists for.
        for i, dev in enumerate(devices):
            conn.execute("""INSERT INTO device_grants (user_id, device_id, role)
                            VALUES (%s,%s,'operator')""", (users[i % USERS], dev))
    setup_s = time.monotonic() - setup

    # Every site forecasts rain within the hour.
    rain = {site_key(lat, lon): [HourlyPoint(time=NOW + dt.timedelta(minutes=40),
                                             precip_prob=0.9)]
            for lat, lon in coords}
    prov = StubProvider(rain)
    mail = FakeMailer()
    tasks._prov, tasks._mail = prov, mail

    # --- poll ----------------------------------------------------------------
    assert tasks.sync_sites()["created"] == SITES

    t0 = time.monotonic()
    polls, raised = 0, 0
    # SITE_BATCH caps how many sites one task claims, so a full sweep is a few
    # passes. That bound is deliberate: it keeps one task's runtime predictable.
    while True:
        out = tasks.poll_forecasts()
        if out["sites"] == 0:
            break
        polls += 1
        raised += out["raised"]
    poll_s = time.monotonic() - t0

    assert raised == SITES, f"raised {raised} alerts for {SITES} sites"

    # The claim per task is bounded, so the number of provider calls is
    # ceil(SITES / SITE_BATCH) - NOT one call per site.
    assert len(prov.calls) == polls
    assert polls <= (SITES // tasks.SITE_BATCH) + 1
    total_coords = sum(len(c) for c in prov.calls)
    assert total_coords == SITES, "every site must be covered exactly once"

    # --- deliver -------------------------------------------------------------
    t1 = time.monotonic()
    sent = emails = 0
    while True:
        out = tasks.send_pending()
        if out["claimed"] == 0:
            break
        sent += out["sent"]
        emails += out["emails"]
    send_s = time.monotonic() - t1

    assert sent == SITES, f"sent {sent} of {SITES} deliveries"
    assert store.delivery_states() == {"sent": SITES}

    # The headline: 200 alerts become at most one email per user, not 200.
    assert emails <= USERS, f"{emails} emails for {USERS} users - coalescing failed"
    assert len(mail.sent) == emails

    # Every email names several sites, since each user watches four.
    multi = [b for _, _, b in mail.sent if b.count("cam-") > 1]
    assert multi, "expected at least one email covering several sites"

    with capsys.disabled():
        print(f"\n  {SITES} sites / {USERS} users")
        print(f"  setup              {setup_s:6.2f}s")
        print(f"  {polls:3d} forecast polls  {poll_s:6.2f}s   "
              f"({len(prov.calls)} provider calls for {SITES} sites)")
        print(f"  {emails:3d} emails          {send_s:6.2f}s   "
              f"({SITES} deliveries coalesced {SITES / max(emails, 1):.1f}x)")


def test_a_second_sweep_sends_nothing_new(store, clean):
    """Idempotence at scale: the forecast is re-polled constantly, and the
    fingerprint index is what stops that becoming a second round of 200 emails."""
    coords = [(50.0 + i * 0.05, 5.0) for i in range(20)]
    with store.pool.connection() as conn:
        for lat, lon in coords:
            dev = "cam-" + uuid.uuid4().hex[:8]
            conn.execute("""INSERT INTO devices (device_id, token_sha256, latitude, longitude)
                            VALUES (%s,%s,%s,%s)""", (dev, uuid.uuid4().bytes * 2, lat, lon))
            uid = "u-" + uuid.uuid4().hex[:8]
            conn.execute("INSERT INTO users (user_id, email) VALUES (%s,%s)",
                         (uid, f"{uid}@example.org"))
            conn.execute("""INSERT INTO device_grants (user_id, device_id, role)
                            VALUES (%s,%s,'operator')""", (uid, dev))

    rain = {site_key(lat, lon): [HourlyPoint(time=NOW + dt.timedelta(minutes=40),
                                             precip_prob=0.9)] for lat, lon in coords}
    tasks._prov, tasks._mail = StubProvider(rain), FakeMailer()

    tasks.sync_sites()
    while tasks.poll_forecasts()["sites"]:
        pass
    while tasks.send_pending()["claimed"]:
        pass
    first = len(tasks._mail.sent)
    assert first > 0

    # Everything becomes due again, with a changed payload so the unchanged-hash
    # shortcut is not what is being exercised.
    with store.pool.connection() as conn:
        conn.execute("UPDATE forecast_sites SET next_poll_at = now(), last_payload_sha = NULL")
    tasks._prov.by_site = {k: [HourlyPoint(time=NOW + dt.timedelta(minutes=40),
                                           precip_prob=0.95)] for k in rain}

    while tasks.poll_forecasts()["sites"]:
        pass
    while tasks.send_pending()["claimed"]:
        pass

    assert len(tasks._mail.sent) == first, "a re-poll sent a second round of emails"

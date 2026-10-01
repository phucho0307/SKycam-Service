"""Celery tasks for rain alerting.

Runs on its own queue with its own workers. Sharing the queue with detection would
put a backlog of thousands of frame-scoring tasks in front of a time-critical
alert — the same head-of-line blocking argument as keeping a 25MB upload off the
control stream, one layer up.

Redis is only ever a *hint* here. Every piece of state is a Postgres row claimed
with SKIP LOCKED, so a lost message costs nothing: the sweep finds the unsent row.
That is why an alert is a row and not a published message.
"""
from __future__ import annotations

import datetime as dt
import os
from collections import defaultdict

from celery_app import app
from mailer import FakeMailer, HardBounce, Mailer, SMTPMailer, SoftFailure, render
from rules import evaluate_rain
from store import MAX_DELIVERY_ATTEMPTS, Store
from weather import ForecastError, OpenMeteo, StubProvider, payload_sha, to_payload

SITE_BATCH = int(os.environ.get("NOTIFY_SITE_BATCH", "50"))
# Counted in users, not deliveries: one pass produces at most this many emails.
USER_BATCH = int(os.environ.get("NOTIFY_USER_BATCH", "100"))

_store = Store()


def _provider():
    if os.environ.get("NOTIFY_PROVIDER", "open-meteo") == "stub":
        return StubProvider()
    return OpenMeteo()


def _mailer() -> Mailer:
    if os.environ.get("NOTIFY_MAILER", "smtp") == "fake":
        return FakeMailer()
    return SMTPMailer()


_prov = _provider()
_mail = _mailer()


@app.task(name="notify.sync_sites")
def sync_sites():
    """Make sure every located device has a forecast site."""
    return {"created": _store.sync_sites()}


@app.task(name="notify.poll_forecasts")
def poll_forecasts():
    """Claim due sites, fetch in one batched call, evaluate, raise alerts.

    Claiming is what makes this safe to run from several workers, or from a
    duplicated scheduler: both fire, one gets the rows, the other gets none. No
    double API calls, no double alerts.
    """
    sites = _store.claim_due_sites(SITE_BATCH)
    if not sites:
        return {"sites": 0}

    points = [(s.latitude, s.longitude) for s in sites]
    try:
        # One request for the whole batch. 200 sites as 200 calls would exhaust a
        # free tier in hours; batched it is a handful of calls.
        forecasts = _prov.fetch(points)
    except ForecastError as e:
        for s in sites:
            _store.mark_site_failure(s.site_key, str(e))
        return {"sites": len(sites), "error": str(e)}

    raised = skipped = unchanged = 0
    now = dt.datetime.now(dt.timezone.utc)

    # zip, not a lookup by key: the provider returns one forecast per input point
    # in order, so there is no key string for Python and SQL to disagree about.
    for s, hourly in zip(sites, forecasts):
        if not hourly:
            _store.mark_site_failure(s.site_key, "provider returned no hourly data")
            continue

        sha = payload_sha(hourly)
        _store.mark_site_success(s.site_key, sha)
        if sha == s.last_payload_sha:
            # Identical inputs cannot change the decision. Skipping saves the
            # rule pass and, more importantly, the alert churn.
            unchanged += 1
            continue
        _store.record_observation(s.site_key, _prov.name, to_payload(hourly))

        for device_id in _store.devices_at_site(s.site_key):
            existing = _store.open_alert(device_id, "rain_forecast")
            decision = evaluate_rain(hourly, now, alert_open=existing is not None)

            if decision.fire:
                alert_id = _store.raise_alert(
                    device_id, "rain_forecast", decision.fingerprint(device_id),
                    decision.predicted_for, decision.probability, to_payload(hourly))
                if alert_id is None:
                    # Same fingerprint: this rain was already warned about.
                    skipped += 1
                    continue
                _store.fan_out(alert_id, device_id)
                raised += 1
            elif decision.resolve and existing:
                _store.resolve_alert(existing["id"])

    return {"sites": len(sites), "raised": raised, "deduped": skipped, "unchanged": unchanged}


@app.task(name="notify.send_pending")
def send_pending():
    """Claim pending deliveries and send one coalesced email per user.

    Grouping by user is what turns 2,000 emails into ~500 at 200 sites, and turns
    twenty emails into one for a person watching twenty cameras.
    """
    pending = _store.claim_deliveries(USER_BATCH)
    if not pending:
        return {"claimed": 0}

    by_user: dict[str, list] = defaultdict(list)
    for d in pending:
        by_user[(d.user_id, d.target)].append(d)

    sent = failed = requeued = 0
    for (user_id, target), rows in by_user.items():
        sites = [{"device_id": r.device_id, "predicted_for": r.predicted_for,
                  "probability": r.probability} for r in rows]
        subject, body = render(sites)
        try:
            _mail.send(target, subject, body)
            for r in rows:
                _store.mark_sent(r.id)
            sent += len(rows)
        except HardBounce as e:
            # Permanent. Stop retrying and stop using this address at all.
            _store.mark_undeliverable(user_id, target, str(e))
            for r in rows:
                _store.fail_delivery(r.id, f"hard bounce: {e}")
            failed += len(rows)
        except SoftFailure as e:
            for r in rows:
                if r.attempts >= MAX_DELIVERY_ATTEMPTS:
                    _store.fail_delivery(r.id, f"out of attempts: {e}")
                    failed += 1
                else:
                    _store.requeue_delivery(r.id, str(e))
                    requeued += 1
        except Exception as e:  # noqa: BLE001 — one bad address must not stop the batch
            for r in rows:
                if r.attempts >= MAX_DELIVERY_ATTEMPTS:
                    _store.fail_delivery(r.id, f"unexpected: {type(e).__name__}: {e}")
                    failed += 1
                else:
                    _store.requeue_delivery(r.id, f"unexpected: {type(e).__name__}")
                    requeued += 1

    return {"claimed": len(pending), "sent": sent, "failed": failed, "requeued": requeued,
            "emails": len(by_user)}


@app.task(name="notify.reclaim_stale")
def reclaim_stale():
    """Return deliveries whose worker died. The self-healing half."""
    return {"reclaimed": _store.reclaim_stale_deliveries()}


@app.task(name="notify.check_staleness")
def check_staleness():
    """Monitoring the monitor.

    If forecasts stop arriving, no alerts are raised and everything *looks* fine.
    Silence is the failure mode that actually hurts, so it has to be loud.
    """
    stale = _store.stale_sites()
    if stale:
        # Logged rather than emailed: this is an operator problem, and emailing it
        # through the machinery that is already broken would be optimistic.
        print(f"STALE FORECASTS for {len(stale)} site(s): {stale[:10]}", flush=True)
    return {"stale_sites": len(stale)}

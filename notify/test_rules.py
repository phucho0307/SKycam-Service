"""Rule tests. No database, no network, no Redis — it is a pure function.

If the prediction is wrong, perfect delivery is worthless, so this is the file
that gets the most attention.
"""
import datetime as dt

import pytest
from rules import HourlyPoint, evaluate_rain, site_key

NOW = dt.datetime(2026, 3, 1, 22, 0, tzinfo=dt.timezone.utc)


def hourly(*probs_at_offsets):
    """hourly((30, 0.1), (60, 0.9)) -> points at +30min and +60min."""
    return [HourlyPoint(time=NOW + dt.timedelta(minutes=m), precip_prob=p)
            for m, p in probs_at_offsets]


def test_fires_when_probability_crosses_the_threshold():
    d = evaluate_rain(hourly((45, 0.80)), NOW, alert_open=False)
    assert d.fire is True
    assert d.probability == pytest.approx(0.80)
    assert d.predicted_for == NOW + dt.timedelta(minutes=45)


def test_quiet_when_probability_is_low():
    d = evaluate_rain(hourly((30, 0.10), (60, 0.20)), NOW, alert_open=False)
    assert d.fire is False
    assert d.resolve is False


def test_ignores_rain_beyond_the_lead_window():
    """Rain in six hours is not actionable now, and warning about it would train
    people to ignore the alert."""
    d = evaluate_rain(hourly((360, 0.99)), NOW, alert_open=False)
    assert d.fire is False


def test_ignores_points_in_the_past():
    d = evaluate_rain(hourly((-120, 0.99)), NOW, alert_open=False)
    assert d.fire is False


def test_picks_the_worst_hour_in_the_window():
    d = evaluate_rain(hourly((15, 0.30), (45, 0.90), (60, 0.40)), NOW, alert_open=False)
    assert d.probability == pytest.approx(0.90)
    assert d.predicted_for == NOW + dt.timedelta(minutes=45)


# -- hysteresis --------------------------------------------------------------
def test_hysteresis_keeps_an_open_alert_alive_in_the_dead_band():
    """0.40 is below the 0.55 fire threshold but above the 0.30 clear threshold.

    Without hysteresis this would resolve and re-fire every cycle, emailing
    someone a dozen times an hour.
    """
    mid = hourly((45, 0.40))
    assert evaluate_rain(mid, NOW, alert_open=False).fire is False   # would not start
    still = evaluate_rain(mid, NOW, alert_open=True)
    assert still.fire is True, "an open alert must persist through the dead band"
    assert still.resolve is False


def test_resolves_only_below_the_clear_threshold():
    d = evaluate_rain(hourly((45, 0.10)), NOW, alert_open=True)
    assert d.fire is False
    assert d.resolve is True


def test_no_flapping_across_a_realistic_oscillation():
    """Walk a probability up and down through the dead band and count state changes.

    A single-threshold implementation flaps on every step; two thresholds give one
    fire and one resolve.
    """
    series = [0.10, 0.50, 0.58, 0.45, 0.52, 0.40, 0.48, 0.25, 0.10]
    open_alert = False
    transitions = []
    for p in series:
        d = evaluate_rain(hourly((45, p)), NOW, alert_open=open_alert)
        if d.fire and not open_alert:
            open_alert = True
            transitions.append(("fire", p))
        elif d.resolve and open_alert:
            open_alert = False
            transitions.append(("resolve", p))
    assert transitions == [("fire", 0.58), ("resolve", 0.25)], transitions


def test_thresholds_must_form_a_dead_band():
    with pytest.raises(ValueError):
        evaluate_rain(hourly((45, 0.9)), NOW, alert_open=False, fire_prob=0.3, clear_prob=0.5)


# -- the safe failure -------------------------------------------------------
def test_missing_forecast_does_not_resolve_an_open_alert():
    """No data is not an all-clear. Silently resolving a warning someone is acting
    on is the worst possible failure here."""
    d = evaluate_rain([], NOW, alert_open=True)
    assert d.fire is False
    assert d.resolve is False, "a missing forecast must never clear an alert"


# -- fingerprints -----------------------------------------------------------
def test_fingerprint_is_stable_within_the_same_hour():
    """Re-evaluating every 10 minutes must produce the same key, or each pass is a
    new alert and another email."""
    a = evaluate_rain(hourly((45, 0.80)), NOW, alert_open=False)
    later = NOW + dt.timedelta(minutes=10)
    b = evaluate_rain(
        [HourlyPoint(time=NOW + dt.timedelta(minutes=45), precip_prob=0.82)],
        later, alert_open=True)
    assert a.fingerprint("cam-1") == b.fingerprint("cam-1")


def test_fingerprint_differs_by_device_and_by_hour():
    d = evaluate_rain(hourly((45, 0.80)), NOW, alert_open=False)
    assert d.fingerprint("cam-1") != d.fingerprint("cam-2")

    later = evaluate_rain(
        [HourlyPoint(time=NOW + dt.timedelta(minutes=50), precip_prob=0.8)],
        NOW + dt.timedelta(hours=3), alert_open=False)
    assert later.fire is False  # out of window at that `now`


# -- site grouping ----------------------------------------------------------
def test_nearby_cameras_share_one_site():
    """The saving that makes 200 sites affordable on a free API tier."""
    assert site_key(51.4778, -0.0015) == site_key(51.4779, -0.0014)


def test_distant_cameras_do_not_share_a_site():
    assert site_key(51.48, -0.00) != site_key(52.48, -0.00)


def test_site_key_is_stable_and_formatted():
    assert site_key(51.4, -0.1) == "51.40,-0.10"
    assert site_key(-33.9, 18.4) == "-33.90,18.40"

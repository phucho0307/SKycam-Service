"""Turning a forecast into a decision.

Deliberately a pure function over plain data: no database, no network, no clock
of its own. If the prediction is wrong, perfect delivery is worthless, so this is
the part that gets tested hardest and it must be testable without any
infrastructure.

Two ideas do most of the work:

**Hysteresis.** Fire at one threshold, clear at a lower one. A probability
oscillating around a single threshold would raise and resolve an alert every
evaluation cycle, emailing someone a dozen times an hour. Cheap to add, miserable
to retrofit once people have stopped reading your alerts.

**Asymmetric cost.** A false positive costs an hour of observing. A false
negative costs a mirror. So the fire threshold is deliberately low, and that is a
policy decision rather than a tuning accident.
"""
from __future__ import annotations

import datetime as dt
import os
from dataclasses import dataclass

# Fire when precipitation probability reaches this within the lead window.
RAIN_FIRE_PROB = float(os.environ.get("NOTIFY_RAIN_FIRE_PROB", "0.55"))
# Clear only once it drops below this. The gap between the two is the hysteresis.
RAIN_CLEAR_PROB = float(os.environ.get("NOTIFY_RAIN_CLEAR_PROB", "0.30"))
# How far ahead to look. An hour of warning is the product requirement.
LEAD_MINUTES = int(os.environ.get("NOTIFY_LEAD_MINUTES", "60"))


@dataclass(frozen=True)
class HourlyPoint:
    """One hour of a forecast."""
    time: dt.datetime
    precip_prob: float            # 0..1
    precip_mm: float = 0.0
    temp_c: float | None = None
    dewpoint_c: float | None = None


@dataclass(frozen=True)
class Decision:
    """What to do about one site right now."""
    fire: bool
    # Set when fire is True.
    predicted_for: dt.datetime | None = None
    probability: float | None = None
    # True when an existing alert should be resolved (probability fell below the
    # clear threshold, or the window passed without rain).
    resolve: bool = False
    reason: str = ""

    def fingerprint(self, device_id: str, kind: str = "rain_forecast") -> str:
        """Dedup key: device + kind + the hour being warned about.

        Bucketed to the hour on purpose. Re-evaluating every 10 minutes must
        produce the *same* fingerprint for the same predicted rain, or each
        evaluation is a new alert and a new email.
        """
        assert self.predicted_for is not None
        hour = self.predicted_for.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H")
        return f"{device_id}:{kind}:{hour}"


def evaluate_rain(
    hourly: list[HourlyPoint],
    now: dt.datetime,
    *,
    alert_open: bool,
    lead_minutes: int = LEAD_MINUTES,
    fire_prob: float = RAIN_FIRE_PROB,
    clear_prob: float = RAIN_CLEAR_PROB,
) -> Decision:
    """Decide whether to warn about rain for one site.

    `alert_open` is whether this device already has a firing alert; it is what
    makes the two thresholds mean anything.
    """
    if fire_prob <= clear_prob:
        raise ValueError("fire_prob must exceed clear_prob, or there is no hysteresis")

    horizon = now + dt.timedelta(minutes=lead_minutes)
    # Only hours between now and the horizon. A point in the past cannot be
    # warned about, and one beyond the horizon is not yet actionable.
    window = [p for p in hourly if now <= p.time <= horizon]
    if not window:
        # No usable forecast is NOT an all-clear. Leaving an open alert alone is
        # the safe failure: a missing forecast must never silently resolve a
        # warning somebody is acting on.
        return Decision(fire=False, reason="no forecast points in the lead window")

    worst = max(window, key=lambda p: p.precip_prob)

    # The threshold that applies depends on whether we are already warning. This
    # is the hysteresis, and it is the whole reason `alert_open` is a parameter.
    threshold = clear_prob if alert_open else fire_prob

    if worst.precip_prob >= threshold:
        return Decision(
            fire=True,
            predicted_for=worst.time,
            probability=worst.precip_prob,
            reason=f"{worst.precip_prob:.0%} precipitation probability at {worst.time:%H:%M}",
        )

    if alert_open:
        return Decision(
            fire=False,
            resolve=True,
            reason=f"probability fell to {worst.precip_prob:.0%}, below the {clear_prob:.0%} clear threshold",
        )
    return Decision(fire=False, reason=f"peak probability {worst.precip_prob:.0%} is below {fire_prob:.0%}")


def site_key(latitude: float, longitude: float, precision: int = 2) -> str:
    """Group nearby cameras onto one forecast.

    Two decimal places is about 1km, which is far finer than a weather model's
    grid. Several cameras at one observatory therefore share a single forecast
    call — and a forecast call is the scarce resource, since free tiers run to
    ~10k/day while 200 devices polled every 10 minutes would be 28,800.
    """
    # Negative zero is normalised away. Python renders round(-0.0015, 2) as
    # '-0.00' and Postgres renders the same value as '0.00', which once meant a
    # camera at a small negative longitude never matched its own forecast site.
    # The database's generated column is now the single definition used for
    # lookups; this keeps the Python spelling identical anyway, because two
    # functions that disagree about the same place are a trap.
    def norm(v: float) -> float:
        r = round(v, precision)
        return 0.0 if r == 0 else r

    return f"{norm(latitude):.{precision}f},{norm(longitude):.{precision}f}"

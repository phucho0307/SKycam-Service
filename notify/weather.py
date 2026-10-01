"""Forecast providers behind one interface.

Same ports-and-adapters shape as `store.TelemetryStore` in the Go service, for the
same two reasons: the provider can be swapped without touching the rules, and
tests run against a stub instead of a real API. A test that needs the internet is
a test that fails on a train.

Open-Meteo is the default because it needs no API key and, importantly, accepts
*several coordinates in one request*. That is what makes 200 sites affordable: one
call for a batch instead of 200 calls against a ~10k/day free tier.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from typing import Protocol

from rules import HourlyPoint

FORECAST_TIMEOUT_S = float(os.environ.get("NOTIFY_FORECAST_TIMEOUT_S", "15"))


class ForecastError(Exception):
    """Transient by assumption: the caller retries with backoff."""


class Provider(Protocol):
    name: str

    def fetch(self, points: list[tuple[float, float]]) -> list[list[HourlyPoint]]:
        """Fetch hourly forecasts for several coordinates at once.

        Returns one forecast per input point, *in the same order*. Deliberately
        positional rather than keyed by a site string: keying it meant Python had
        to reproduce the database's rounding exactly, and it did not. Postgres
        renders round(-0.0015, 2) as '0.00' where Python gives '-0.00', so a site
        at a small negative longitude silently matched nothing. Ordering cannot
        drift the way two formatting rules can.

        Batching is in the interface rather than bolted on, because a provider
        that cannot batch forces the caller into N requests, and that belongs in
        the type.
        """
        ...


class OpenMeteo:
    """https://open-meteo.com - no key, batches coordinates, generous free tier."""

    name = "open-meteo"
    BASE = "https://api.open-meteo.com/v1/forecast"

    def __init__(self, base_url: str | None = None, session=None):
        self.base = base_url or os.environ.get("NOTIFY_FORECAST_URL", self.BASE)
        self._session = session

    def fetch(self, points: list[tuple[float, float]]) -> list[list[HourlyPoint]]:
        if not points:
            return []
        import requests

        session = self._session or requests
        params = {
            # Comma-separated lists: one HTTP call for the whole batch.
            "latitude": ",".join(f"{lat:.4f}" for lat, _ in points),
            "longitude": ",".join(f"{lon:.4f}" for _, lon in points),
            "hourly": "precipitation_probability,precipitation,temperature_2m,dew_point_2m",
            "forecast_days": "2",
            "timezone": "UTC",
        }
        try:
            resp = session.get(self.base, params=params, timeout=FORECAST_TIMEOUT_S)
            resp.raise_for_status()
            body = resp.json()
        except Exception as e:  # noqa: BLE001 - every failure here is retryable
            raise ForecastError(f"{self.name}: {e}") from e

        # A single coordinate returns an object; several return a list. Normalising
        # here keeps that quirk out of the caller.
        blocks = body if isinstance(body, list) else [body]
        if len(blocks) != len(points):
            # Positional results are only safe while the counts match. Failing
            # loudly beats pairing the wrong forecast with the wrong observatory.
            raise ForecastError(
                f"{self.name}: asked for {len(points)} locations, got {len(blocks)}")

        return [_parse_hourly(block) for block in blocks]


def _parse_hourly(block: dict) -> list[HourlyPoint]:
    hourly = block.get("hourly") or {}
    times = hourly.get("time") or []
    probs = hourly.get("precipitation_probability") or []
    mm = hourly.get("precipitation") or []
    temps = hourly.get("temperature_2m") or []
    dews = hourly.get("dew_point_2m") or []

    points: list[HourlyPoint] = []
    for i, raw in enumerate(times):
        # Provider times are UTC (we ask for timezone=UTC) but carry no offset, and
        # comparing a naive datetime against an aware `now` raises. Attach it here.
        when = dt.datetime.fromisoformat(raw)
        if when.tzinfo is None:
            when = when.replace(tzinfo=dt.timezone.utc)
        prob = probs[i] if i < len(probs) else None
        points.append(HourlyPoint(
            time=when,
            # The API reports percent; the rules work in 0..1. Converting at the
            # boundary means the rules never have to know which provider it was.
            precip_prob=(prob or 0) / 100.0,
            precip_mm=(mm[i] if i < len(mm) else 0) or 0.0,
            temp_c=temps[i] if i < len(temps) else None,
            dewpoint_c=dews[i] if i < len(dews) else None,
        ))
    return points


class StubProvider:
    """A forecast fixture, for tests and for running the pipeline offline.

    Not a mock of the HTTP client: it implements the same interface the real
    provider does, so the code path under test is the production one.
    """

    name = "stub"

    def __init__(self, by_site: dict[str, list[HourlyPoint]] | None = None,
                 fail_with: Exception | None = None):
        self.by_site = by_site or {}
        self.fail_with = fail_with
        self.calls: list[list[tuple[float, float]]] = []

    def fetch(self, points: list[tuple[float, float]]) -> list[list[HourlyPoint]]:
        from rules import site_key

        self.calls.append(list(points))
        if self.fail_with:
            raise self.fail_with
        # Keyed internally for fixture convenience; returned positionally, exactly
        # as the real provider does.
        return [self.by_site.get(site_key(lat, lon), []) for lat, lon in points]


def payload_sha(points: list[HourlyPoint]) -> str:
    """Fingerprint of a forecast, so an unchanged one skips rule evaluation.

    A forecast refreshes every ~10-15 minutes; polling faster than the data changes
    is waste, and so is re-deciding on identical inputs.
    """
    blob = json.dumps([[p.time.isoformat(), p.precip_prob, p.precip_mm] for p in points],
                      sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()


def to_payload(points: list[HourlyPoint], limit: int = 24) -> dict:
    """The forecast as stored on an alert: the evidence for the decision."""
    return {"hourly": [
        {"time": p.time.isoformat(), "precip_prob": p.precip_prob, "precip_mm": p.precip_mm}
        for p in points[:limit]
    ]}

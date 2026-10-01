# notify — rain alerting (Python + Celery + Redis + Postgres)

Warns operators by email when precipitation is forecast within the hour, so
instruments can be parked.

```
Beat tick --claim due sites (SKIP LOCKED)--> batched forecast call
   -> rules (pure function, hysteresis) -> alerts row (fingerprint = dedup)
   -> fan out via device_grants -> notification_deliveries rows
   -> claim per USER -> one coalesced email -> mark sent / retry / dead-letter
```

## Why it is shaped this way

**Nothing in this platform can predict rain.** `detector.py` counts stars: that is
*current* cloudiness and it is night-only. Telemetry humidity can indicate dew on
the optics but will not give an hour of warning. The forecast therefore comes from
a third party (Open-Meteo by default — no key, and it accepts several coordinates
per request).

**An alert is a row, not a message.** Pub/sub was the obvious first instinct and it
is wrong here: it cannot retry, cannot be acknowledged, cannot be audited, and a
subscriber that is restarting loses the message silently. For "your telescope is
about to get rained on", silent loss is the one unacceptable failure. Redis is the
Celery broker and nothing more; every piece of state is a Postgres row.

**The schedule lives in Postgres.** `forecast_sites.next_poll_at`, claimed with
`FOR UPDATE SKIP LOCKED`. Staggering, per-site backoff and the staleness check all
fall out of the claim query, and — the point — **a duplicated scheduler becomes a
no-op**: two tickers fire, one claims the rows, the other gets none. No double API
calls, no double alerts. That makes Beat's one real weakness (it must be a
singleton) stop mattering, which is why Beat is fine here.

**Its own queue.** `-Q notify`, separate workers. Sharing the detection queue would
put a backlog of thousands of frame-scoring tasks in front of a time-critical
alert; the same head-of-line blocking argument as keeping a 25MB upload off the
control stream.

## Scale decisions, with the arithmetic

| Concern | Decision | Why |
|---|---|---|
| ~10k/day free API tier | group devices by rounded coordinates, batch the call | 200 sites polled naively every 10 min is 28,800 calls/day |
| 200 sites firing at once | claim **users**, not deliveries | 200 alerts become one email per person |
| forecast re-evaluated constantly | unique index on `alerts.fingerprint` | otherwise every crossing is another email |
| unchanged forecast | skip the rule pass on an identical payload hash | polling faster than the data changes is waste |
| probability oscillating | hysteresis: fire at 0.55, clear at 0.30 | a single threshold flaps and trains people to ignore alerts |
| a dead mailbox | hard bounce marks the address undeliverable | retrying wrecks the sending domain and then *nobody* gets mail |
| a dead poller | `check_staleness` | no alerts looks exactly like good weather; silence must be loud |

Measured: **200 sites / 50 users → 4 provider calls and 50 emails** (200
deliveries coalesced 4×).

The batch unit was originally deliveries, and that was a real bug: 200 deliveries
at a batch of 100 gave every user *two* emails, and the ratio worsens with scale.
The unit of work is an email, so the batch is counted in users.

## Running it

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:55432/skycam_demo
export REDIS_URL=redis://localhost:6379/0
export NOTIFY_SMTP_HOST=localhost NOTIFY_SMTP_PORT=1025   # MailHog locally
export NOTIFY_FROM=alerts@observatory.services

celery -A celery_app worker -Q notify --loglevel=info   # one terminal
celery -A celery_app beat --loglevel=info               # another (Beat cannot
                                                       # be embedded on Windows)
```

Devices need coordinates before anything happens — set by an operator at install
time, never reported by the Pi (it cannot know where it is, the value never
changes, and a device asserting its own location would be untrusted input driving
outbound API calls):

```sql
UPDATE devices SET latitude = 51.4778, longitude = -0.0015 WHERE device_id = 'skycam-01';
```

## Config

`NOTIFY_RAIN_FIRE_PROB` (0.55), `NOTIFY_RAIN_CLEAR_PROB` (0.30),
`NOTIFY_LEAD_MINUTES` (60), `NOTIFY_POLL_INTERVAL_S` (600),
`NOTIFY_SITE_BATCH` (50), `NOTIFY_USER_BATCH` (100),
`NOTIFY_MAX_DELIVERY_ATTEMPTS` (4), `NOTIFY_STALE_CLAIM_S` (300),
`NOTIFY_STALE_FORECAST_S` (1800), `NOTIFY_PROVIDER` (open-meteo|stub),
`NOTIFY_MAILER` (smtp|fake).

The fire threshold is deliberately low. A false positive costs an hour of
observing; a false negative costs a mirror. That asymmetry is the whole argument
for where it sits.

## Tests

```bash
createdb skycam_notify_test        # then: ingest migrate
NOTIFY_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/skycam_notify_test \
  python -m pytest notify/ -v
```

35 tests. `test_rules.py` needs nothing at all — the rules are a pure function, and
that is where the most attention went, because if the prediction is wrong then
perfect delivery is worthless. It includes a flapping test that walks a
probability up and down through the dead band and asserts exactly one fire and one
resolve.

`test_pipeline.py` and `test_scale.py` use real Postgres with a **stub forecast
provider and a fake mailer** — both interface implementations rather than mocks,
so the code path under test is the production one.

**A bug the tests caught:** the site key was computed in *two* places, Python and
SQL, and they disagreed. Postgres renders `round(-0.0015, 2)` as `0.00`, Python as
`-0.00`, so a camera at a small negative longitude never matched its own forecast
site. Fixed by making the database's generated column the single definition and
having the provider return results **positionally** rather than keyed by a string
two languages have to agree on.

## Not built

No `Dockerfile`, no kustomize entry — this runs in no cluster environment. No
bounce webhook (the store method exists; nothing calls it from SES). No weekly
canary through the real mail path, which is what stops you discovering a lapsed
SMTP credential on the night it rains. SES must be out of sandbox before this is
useful at scale: sandbox is 1/s and 200/day, to verified addresses only.

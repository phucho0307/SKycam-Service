-- Rain alerting: forecast scheduling, alert state, and email delivery.
--
-- The design choice that shapes this file: **the schedule lives here, not in a
-- scheduler.** `forecast_sites.next_poll_at` is claimed with FOR UPDATE SKIP
-- LOCKED, exactly like `frames` detection and like deliveries below. That buys,
-- without writing any of it:
--   * staggering      -- each site has its own next_poll_at, so no herd
--   * per-site backoff -- a failing provider call just pushes one row out
--   * a staleness alarm -- "WHERE last_success_at < now() - interval '20 min'"
--   * safety against a duplicated scheduler -- two tickers both fire, one claims
--     the rows and the other gets none, so no double API calls or double alerts
-- One mechanism to understand across three subsystems, not three.

-- Where a camera is. Set by an operator at install time, never reported by the
-- device: a Pi cannot know where it is, the value never changes, and a device
-- asserting its own location would be untrusted input driving outbound API calls.
ALTER TABLE devices ADD COLUMN latitude  double precision;
ALTER TABLE devices ADD COLUMN longitude double precision;
ALTER TABLE devices ADD CONSTRAINT devices_latlon_together CHECK (
    (latitude IS NULL AND longitude IS NULL) OR
    (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180)
);

-- The grouping key, defined ONCE. It was briefly computed in both SQL and
-- Python, and they disagreed: Postgres renders round(-0.0015, 2) as '0.00' while
-- Python renders it as '-0.00', so a camera at a location with a small negative
-- longitude was never matched to its own forecast site. A generated column makes
-- the database the single definition, and callers compare against the column
-- rather than rebuilding the string.
ALTER TABLE devices ADD COLUMN site_key text GENERATED ALWAYS AS (
    round(latitude::numeric, 2)::text || ',' || round(longitude::numeric, 2)::text
) STORED;

CREATE INDEX devices_site_key_idx ON devices (site_key) WHERE site_key IS NOT NULL;

-- One row per distinct *location*, not per device. Several cameras at one
-- observatory share weather, and a forecast call is the scarce resource: free
-- provider tiers are ~10k calls/day, while 200 devices polled every 10 minutes
-- would be 28,800. Grouping by rounded coordinates is what makes it affordable.
CREATE TABLE forecast_sites (
    -- Coordinates rounded to 2dp (~1km) and formatted, e.g. '51.48,-0.01'.
    -- Derived rather than a surrogate key so a new device at an existing
    -- observatory joins the existing site automatically.
    site_key        text        PRIMARY KEY,
    latitude        double precision NOT NULL,
    longitude       double precision NOT NULL,
    next_poll_at    timestamptz NOT NULL DEFAULT now(),
    last_polled_at  timestamptz,
    last_success_at timestamptz,
    consecutive_failures smallint NOT NULL DEFAULT 0,
    last_error      text,
    -- Hash of the last forecast payload: unchanged means the rules cannot have
    -- changed their mind, so evaluation is skipped.
    last_payload_sha text
);

-- The claim query's index. Partial is pointless here (every row is pollable),
-- but ordering by next_poll_at must not be a sort.
CREATE INDEX forecast_sites_due_idx ON forecast_sites (next_poll_at);

-- What the provider said, kept because "did we know?" is the question asked after
-- a wet mirror. Pruned on a retention schedule, not kept forever.
CREATE TABLE forecast_observations (
    id          bigserial   PRIMARY KEY,
    site_key    text        NOT NULL REFERENCES forecast_sites (site_key) ON DELETE CASCADE,
    fetched_at  timestamptz NOT NULL DEFAULT now(),
    provider    text        NOT NULL,
    payload     jsonb       NOT NULL
);

CREATE INDEX forecast_observations_site_idx ON forecast_observations (site_key, fetched_at DESC);

-- An alert is a row with a lifecycle, not a message. That is the whole reason
-- pub/sub is wrong for this: an alert has to survive the notifier being down, be
-- retried, acknowledged and audited.
CREATE TABLE alerts (
    id             bigserial   PRIMARY KEY,
    device_id      text        NOT NULL REFERENCES devices (device_id) ON DELETE CASCADE,
    kind           text        NOT NULL CHECK (kind IN ('rain_forecast', 'dew_point')),
    -- device + kind + the hour being warned about. The unique index is the
    -- deduplication: a forecast re-evaluated every 10 minutes crosses the
    -- threshold repeatedly, and without this each crossing is another email.
    -- Durable rather than a Redis key with a TTL, because losing the key means
    -- spamming a user.
    fingerprint    text        NOT NULL,
    state          text        NOT NULL DEFAULT 'firing'
        CHECK (state IN ('firing', 'acknowledged', 'resolved')),
    severity       text        NOT NULL DEFAULT 'warning',
    -- When the bad weather is expected, not when we noticed. Deliveries are
    -- ordered by this, so the site with 15 minutes of warning goes before the
    -- one with 55.
    predicted_for  timestamptz NOT NULL,
    probability    real,
    raised_at      timestamptz NOT NULL DEFAULT now(),
    acknowledged_at timestamptz,
    acknowledged_by text,
    resolved_at    timestamptz,
    -- The forecast that justified this alert. When someone asks "why did you
    -- close my dome", this is the answer.
    payload        jsonb
);

CREATE UNIQUE INDEX alerts_fingerprint_idx ON alerts (fingerprint);
CREATE INDEX alerts_device_raised_idx ON alerts (device_id, raised_at DESC);
-- Hysteresis needs the current open alert for a device+kind, and only that.
CREATE INDEX alerts_open_idx ON alerts (device_id, kind) WHERE state = 'firing';

-- One row per (alert, user, channel). Postgres is the queue of record; Redis
-- carries a hint. A lost hint costs nothing because the sweep finds the unsent
-- row -- the same shape as frame detection.
CREATE TABLE notification_deliveries (
    id          bigserial   PRIMARY KEY,
    alert_id    bigint      NOT NULL REFERENCES alerts (id) ON DELETE CASCADE,
    user_id     text        NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    channel     text        NOT NULL DEFAULT 'email' CHECK (channel IN ('email', 'webhook')),
    target      text        NOT NULL,
    state       text        NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'claimed', 'sent', 'failed', 'suppressed')),
    attempts    smallint    NOT NULL DEFAULT 0,
    claimed_at  timestamptz,
    sent_at     timestamptz,
    last_error  text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    -- One notification per person per alert per channel. Makes fan-out
    -- idempotent: re-running it after a crash inserts nothing new.
    CONSTRAINT notification_deliveries_unique UNIQUE (alert_id, user_id, channel)
);

-- The claim query: oldest-predicted first, so the most urgent alert is sent
-- first when 200 sites fire at once.
CREATE INDEX notification_deliveries_pending_idx
    ON notification_deliveries (created_at) WHERE state = 'pending';
-- Backs the sweep that returns claims whose worker died.
CREATE INDEX notification_deliveries_claimed_idx
    ON notification_deliveries (claimed_at) WHERE state = 'claimed';

-- Where to reach a user. `users.email` is provider-verified already, because
-- sign-in rejects an unverified email_verified claim -- so no separate
-- verification flow is needed. This table exists for the other half: a hard
-- bounce marks the address undeliverable, because continuing to send to a dead
-- address wrecks the sending domain's reputation and then *nothing* is delivered.
CREATE TABLE notification_channels (
    user_id       text        NOT NULL REFERENCES users (user_id) ON DELETE CASCADE,
    channel       text        NOT NULL CHECK (channel IN ('email', 'webhook')),
    target        text        NOT NULL,
    undeliverable_at timestamptz,
    undeliverable_reason text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, channel)
);

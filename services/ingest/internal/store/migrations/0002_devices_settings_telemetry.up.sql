-- Devices that may connect. The token is stored only as a SHA-256 digest:
-- tokens are high-entropy random strings, so a digest is enough and a database
-- leak doesn't hand over working credentials.
CREATE TABLE devices (
    device_id     text PRIMARY KEY,
    token_sha256  bytea       NOT NULL,
    display_name  text,
    created_at    timestamptz NOT NULL DEFAULT now(),
    last_seen_at  timestamptz,
    CONSTRAINT device_id_charset CHECK (device_id ~ '^[A-Za-z0-9_-]{1,64}$')
);

-- Lookup is by token on every authenticated call, so it needs its own index.
CREATE UNIQUE INDEX devices_token_idx ON devices (token_sha256);

-- Environmental readings, one row per device report.
CREATE TABLE telemetry (
    id            bigserial PRIMARY KEY,
    device_id     text        NOT NULL,
    recorded_at   timestamptz NOT NULL,
    received_at   timestamptz NOT NULL DEFAULT now(),
    temperature_c double precision,
    humidity_pct  double precision,
    probe_temp_c  double precision
);

CREATE INDEX telemetry_device_recorded_idx ON telemetry (device_id, recorded_at DESC);

-- Current settings: one row per device, last write wins. This table is the
-- source of truth. Pushing to a connected device is an optimisation; a device
-- re-reads this row when it connects, so a lost push cannot leave it stale.
CREATE TABLE device_settings (
    device_id          text PRIMARY KEY,
    exposure_ms        double precision,
    gain               bigint,
    preview_gamma      double precision,
    preview_contrast   double precision,
    preview_brightness double precision,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    updated_by         text
);

-- Who changed what, and what it was before. An observatory needs to explain a
-- bad night's data months later.
CREATE TABLE settings_audit (
    id          bigserial PRIMARY KEY,
    device_id   text        NOT NULL,
    changed_at  timestamptz NOT NULL DEFAULT now(),
    changed_by  text,
    before_json jsonb,
    after_json  jsonb NOT NULL
);

CREATE INDEX settings_audit_device_idx ON settings_audit (device_id, changed_at DESC);

-- Frame metadata. Image bytes live in S3; rows hold keys only.
CREATE TABLE frames (
    frame_id        uuid PRIMARY KEY,            -- client-generated; idempotency key
    device_id       text        NOT NULL,
    captured_at     timestamptz NOT NULL,
    received_at     timestamptz NOT NULL DEFAULT now(),
    temperature_c   double precision,
    probe_temp_c    double precision,
    preview_key     text,
    fits_key        text,
    fits_size_bytes bigint,
    fits_sha256     bytea,
    -- Explicit detection state instead of null sentinels. 'skipped' = no
    -- preview to analyse. The detect worker (deferred) moves pending rows on.
    detect_status   text NOT NULL DEFAULT 'pending'
        CHECK (detect_status IN ('pending', 'scored', 'failed', 'skipped')),
    cloud_score     real,
    is_cloudy       boolean,
    CONSTRAINT fits_fields_together CHECK (
        (fits_key IS NULL AND fits_size_bytes IS NULL AND fits_sha256 IS NULL)
        OR (fits_key IS NOT NULL AND fits_size_bytes IS NOT NULL AND fits_sha256 IS NOT NULL)
    )
);

-- Latest frame per device, and per-device time ranges.
CREATE INDEX frames_device_captured_idx ON frames (device_id, captured_at DESC);

-- Detection backlog / reconciliation sweep. Partial, so it only holds the
-- handful of rows still waiting rather than the whole table.
CREATE INDEX frames_pending_detect_idx ON frames (received_at)
    WHERE detect_status = 'pending';

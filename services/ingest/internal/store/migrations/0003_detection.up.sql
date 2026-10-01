-- Detection pipeline: claim semantics, bounded retries, and alarms.
--
-- The detect worker (Python/Celery) reads and writes these columns. The schema
-- lives here because the ingest service owns the database; the worker is a
-- client of it. See CLOUD_DETECTION_AND_REDIS.md.

-- 'claimed' lets a poller take rows atomically so two pollers never hand the
-- same frame to two workers. Without it, "still pending" stays true while a
-- worker is mid-task and the frame is enqueued again on the next scan.
ALTER TABLE frames DROP CONSTRAINT frames_detect_status_check;
ALTER TABLE frames ADD CONSTRAINT frames_detect_status_check
    CHECK (detect_status IN ('pending', 'claimed', 'scored', 'failed', 'skipped'));

-- Bounded retries. Counting attempts on the row (not in the broker) means the
-- budget survives a broker flush, and "why did this frame fail" is answerable
-- from the database months later.
ALTER TABLE frames ADD COLUMN detect_attempts smallint NOT NULL DEFAULT 0;
ALTER TABLE frames ADD COLUMN claimed_at timestamptz;
ALTER TABLE frames ADD COLUMN detect_error text;

-- The detector's raw measurements, not just its verdict. cloud_score is derived
-- from these via thresholds that are still tuned on synthetic data, so keeping
-- the inputs is what makes recalibration possible on real night sky without
-- re-reading every JPEG out of object storage.
ALTER TABLE frames ADD COLUMN star_count integer;
ALTER TABLE frames ADD COLUMN brightness real;
ALTER TABLE frames ADD COLUMN rb_ratio real;

-- A worker that dies between claiming and finishing leaves the row 'claimed'
-- forever. This index backs the sweep that returns stale claims to 'pending' —
-- partial, so it holds only rows currently in flight.
CREATE INDEX frames_claimed_idx ON frames (claimed_at)
    WHERE detect_status = 'claimed';

-- Raised when a frame scores above the cloud threshold.
CREATE TABLE alarms (
    id          bigserial   PRIMARY KEY,
    frame_id    uuid        NOT NULL REFERENCES frames (frame_id) ON DELETE CASCADE,
    device_id   text        NOT NULL,
    kind        text        NOT NULL DEFAULT 'cloudy',
    raised_at   timestamptz NOT NULL DEFAULT now(),
    cloud_score real        NOT NULL
);

-- The point of this index: it makes alarm writes idempotent via
-- ON CONFLICT DO NOTHING. With task_acks_late, a worker dying mid-task gets the
-- task redelivered, and a plain INSERT would raise the same alarm twice.
CREATE UNIQUE INDEX alarms_frame_kind_idx ON alarms (frame_id, kind);

-- "Recent alarms for this device", the query a dashboard actually runs.
CREATE INDEX alarms_device_raised_idx ON alarms (device_id, raised_at DESC);

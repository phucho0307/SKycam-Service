-- Keep FITS only for clear-sky frames, compressed.
--
-- Every raw FITS under frames/ expires after FITS_EXPIRE_DAYS (bucket lifecycle,
-- infra/k8s/minio/bucket-init.yaml). Keeping one is an explicit act: once a
-- frame is scored clear, the detect worker writes a losslessly compressed copy
-- under archive/, which has its own, longer retention. So the default is delete,
-- and the failure mode is safe for the disk: if detection or archiving is down,
-- nothing is kept and nothing piles up. The raw file's lifetime doubles as a
-- grace period in which a wrong "cloudy" verdict can still be corrected.
--
-- A separate table rather than more columns on frames: archiving touches a
-- small fraction of rows, and frames is the hottest table in the system.
CREATE TABLE fits_archive (
    frame_id       uuid        PRIMARY KEY REFERENCES frames (frame_id) ON DELETE CASCADE,
    status         text        NOT NULL
        CHECK (status IN ('claimed', 'pending', 'archived', 'failed')),
    -- Retry budget on the row, as for detection: it survives a broker flush and
    -- "why was this clear frame not kept" is answerable later.
    attempts       smallint    NOT NULL DEFAULT 0,
    claimed_at     timestamptz,
    error          text,
    archive_key    text,
    -- GZIP_2 or RICE_1 (FITS tile compression), or 'none' when the file could
    -- not be compressed losslessly (not a valid FITS, floating-point data). A
    -- clear-sky frame is kept either way; compression is never allowed to lose it.
    compression    text,
    original_bytes bigint,
    stored_bytes   bigint,
    archived_at    timestamptz,
    CONSTRAINT archived_has_key CHECK ((status = 'archived') = (archive_key IS NOT NULL))
);

-- Candidates: scored clear with a FITS. Partial, so it holds only the frames
-- that could be archived, not the whole table.
CREATE INDEX frames_clear_fits_idx ON frames (received_at)
    WHERE detect_status = 'scored' AND is_cloudy = false AND fits_key IS NOT NULL;

-- Backs the sweep that returns claims whose worker died.
CREATE INDEX fits_archive_claimed_idx ON fits_archive (claimed_at)
    WHERE status = 'claimed';

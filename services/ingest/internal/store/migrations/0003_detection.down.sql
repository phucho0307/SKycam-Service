DROP TABLE alarms;

DROP INDEX frames_claimed_idx;

ALTER TABLE frames DROP COLUMN rb_ratio;
ALTER TABLE frames DROP COLUMN brightness;
ALTER TABLE frames DROP COLUMN star_count;
ALTER TABLE frames DROP COLUMN detect_error;
ALTER TABLE frames DROP COLUMN claimed_at;
ALTER TABLE frames DROP COLUMN detect_attempts;

-- Rows mid-flight would violate the narrower constraint, so land them first.
UPDATE frames SET detect_status = 'pending' WHERE detect_status = 'claimed';

ALTER TABLE frames DROP CONSTRAINT frames_detect_status_check;
ALTER TABLE frames ADD CONSTRAINT frames_detect_status_check
    CHECK (detect_status IN ('pending', 'scored', 'failed', 'skipped'));

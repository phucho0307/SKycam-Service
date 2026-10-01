package store

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// TokenDigest is how device tokens are stored and compared. Tokens are
// high-entropy random strings rather than passwords, so a single SHA-256 is the
// right trade: no per-login CPU cost, and a leaked table yields no usable token.
func TokenDigest(token string) []byte {
	sum := sha256.Sum256([]byte(token))
	return sum[:]
}

func (p *Postgres) AuthenticateDevice(ctx context.Context, token string) (string, error) {
	if token == "" {
		return "", ErrUnknownToken
	}
	var deviceID string
	err := p.pool.QueryRow(ctx,
		`SELECT device_id FROM devices WHERE token_sha256 = $1`, TokenDigest(token),
	).Scan(&deviceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrUnknownToken
	}
	if err != nil {
		return "", fmt.Errorf("authenticate device: %w", err)
	}
	return deviceID, nil
}

func (p *Postgres) UpsertDevice(ctx context.Context, deviceID, token, displayName string) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO devices (device_id, token_sha256, display_name)
		VALUES ($1, $2, NULLIF($3, ''))
		ON CONFLICT (device_id) DO UPDATE
		SET token_sha256 = EXCLUDED.token_sha256,
		    display_name = COALESCE(EXCLUDED.display_name, devices.display_name)`,
		deviceID, TokenDigest(token), displayName)
	if err != nil {
		return fmt.Errorf("upsert device: %w", err)
	}
	return nil
}

func (p *Postgres) TouchDevice(ctx context.Context, deviceID string, at time.Time) error {
	_, err := p.pool.Exec(ctx, `UPDATE devices SET last_seen_at = $2 WHERE device_id = $1`, deviceID, at)
	return err
}

func (p *Postgres) InsertTelemetry(ctx context.Context, t TelemetryReading) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO telemetry (device_id, recorded_at, temperature_c, humidity_pct, probe_temp_c)
		VALUES ($1, $2, $3, $4, $5)`,
		t.DeviceID, t.RecordedAt, t.TemperatureC, t.HumidityPct, t.ProbeTempC)
	if err != nil {
		return fmt.Errorf("insert telemetry: %w", err)
	}
	return nil
}

const settingsColumns = `device_id, exposure_ms, gain, preview_gamma, preview_contrast,
	preview_brightness, updated_at, updated_by`

func scanSettings(row pgx.Row) (*Settings, error) {
	var s Settings
	err := row.Scan(&s.DeviceID, &s.ExposureMs, &s.Gain, &s.PreviewGamma,
		&s.PreviewContrast, &s.PreviewBrightness, &s.UpdatedAt, &s.UpdatedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (p *Postgres) GetSettings(ctx context.Context, deviceID string) (*Settings, error) {
	s, err := scanSettings(p.pool.QueryRow(ctx,
		`SELECT `+settingsColumns+` FROM device_settings WHERE device_id = $1`, deviceID))
	if err != nil {
		return nil, fmt.Errorf("get settings: %w", err)
	}
	return s, nil
}

// UpsertSettings writes the new values and the audit row in one transaction:
// a settings change is never applied without a record of who made it.
func (p *Postgres) UpsertSettings(ctx context.Context, s Settings, changedBy string) (Settings, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return Settings{}, err
	}
	defer tx.Rollback(ctx)

	// Lock the row so two concurrent edits can't interleave and lose one.
	before, err := scanSettings(tx.QueryRow(ctx,
		`SELECT `+settingsColumns+` FROM device_settings WHERE device_id = $1 FOR UPDATE`, s.DeviceID))
	if err != nil {
		return Settings{}, fmt.Errorf("read current settings: %w", err)
	}

	after, err := scanSettings(tx.QueryRow(ctx, `
		INSERT INTO device_settings (device_id, exposure_ms, gain, preview_gamma,
			preview_contrast, preview_brightness, updated_at, updated_by)
		VALUES ($1, $2, $3, $4, $5, $6, now(), NULLIF($7, ''))
		ON CONFLICT (device_id) DO UPDATE SET
			exposure_ms = EXCLUDED.exposure_ms,
			gain = EXCLUDED.gain,
			preview_gamma = EXCLUDED.preview_gamma,
			preview_contrast = EXCLUDED.preview_contrast,
			preview_brightness = EXCLUDED.preview_brightness,
			updated_at = now(),
			updated_by = EXCLUDED.updated_by
		RETURNING `+settingsColumns,
		s.DeviceID, s.ExposureMs, s.Gain, s.PreviewGamma,
		s.PreviewContrast, s.PreviewBrightness, changedBy))
	if err != nil {
		return Settings{}, fmt.Errorf("upsert settings: %w", err)
	}

	beforeJSON, afterJSON := settingsJSON(before), settingsJSON(after)
	if _, err := tx.Exec(ctx, `
		INSERT INTO settings_audit (device_id, changed_by, before_json, after_json)
		VALUES ($1, NULLIF($2, ''), $3, $4)`,
		s.DeviceID, changedBy, beforeJSON, afterJSON); err != nil {
		return Settings{}, fmt.Errorf("write settings audit: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Settings{}, err
	}
	return *after, nil
}

func settingsJSON(s *Settings) []byte {
	if s == nil {
		return nil
	}
	b, err := json.Marshal(map[string]any{
		"exposure_ms":        s.ExposureMs,
		"gain":               s.Gain,
		"preview_gamma":      s.PreviewGamma,
		"preview_contrast":   s.PreviewContrast,
		"preview_brightness": s.PreviewBrightness,
	})
	if err != nil {
		return nil
	}
	return b
}

package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // registers pgx5://
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

type Postgres struct {
	pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) *Postgres { return &Postgres{pool: pool} }

func (p *Postgres) Exists(ctx context.Context, id uuid.UUID) (bool, error) {
	var exists bool
	err := p.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM frames WHERE frame_id = $1)`, id.String(),
	).Scan(&exists)
	return exists, err
}

func (p *Postgres) Insert(ctx context.Context, f Frame) (bool, error) {
	status := "pending"
	if f.PreviewKey == nil {
		status = "skipped" // nothing for the detector to look at
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) // no-op after Commit

	tag, err := tx.Exec(ctx, `
		INSERT INTO frames (
			frame_id, device_id, captured_at, temperature_c, probe_temp_c,
			preview_key, fits_key, fits_size_bytes, fits_sha256, detect_status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (frame_id) DO NOTHING`,
		f.ID.String(), f.DeviceID, f.CapturedAt, f.TemperatureC, f.ProbeTempC,
		f.PreviewKey, f.FitsKey, f.FitsSizeBytes, f.FitsSHA256, status,
	)
	if err != nil {
		return false, fmt.Errorf("insert frame: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil // already committed by an earlier attempt
	}

	// Inside the transaction: Postgres delivers the notification only when the
	// transaction commits, so a row without an event (or an event without a
	// row) can't happen. This is what removes the dual-write gap.
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, NotifyChannel, f.ID.String()); err != nil {
		return false, fmt.Errorf("notify: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

// Migrate applies all pending migrations. databaseURL is a postgres:// URL.
func Migrate(databaseURL string) error {
	src, err := iofs.New(migrationsFS, "migrations")
	if err != nil {
		return err
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, toPgx5URL(databaseURL))
	if err != nil {
		return fmt.Errorf("init migrate: %w", err)
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}

// golang-migrate's pgx/v5 driver registers the pgx5:// scheme.
func toPgx5URL(u string) string {
	for _, prefix := range []string{"postgresql://", "postgres://"} {
		if strings.HasPrefix(u, prefix) {
			return "pgx5://" + strings.TrimPrefix(u, prefix)
		}
	}
	return u
}

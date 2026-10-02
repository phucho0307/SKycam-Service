// Package testenv wires the integration tests to real Postgres and S3.
//
// Tests are skipped unless INGEST_TEST_DATABASE_URL and INGEST_TEST_S3_ENDPOINT
// are set, so `go test ./...` stays green without infrastructure:
//
//	INGEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/ingest_test?sslmode=disable
//	INGEST_TEST_S3_ENDPOINT=http://localhost:9000
//	INGEST_TEST_S3_ACCESS_KEY / INGEST_TEST_S3_SECRET_KEY   (default minioadmin)
//	INGEST_TEST_S3_BUCKET                                   (default ingest-test)
//	INGEST_TEST_DIRECT_DATABASE_URL  optional; see below
//
// To run the suite through PgBouncer, point INGEST_TEST_DATABASE_URL at the
// pooler and INGEST_TEST_DIRECT_DATABASE_URL at Postgres itself. The two things
// that need a real session -- migrations (golang-migrate holds a session-level
// advisory lock) and LISTEN -- then use the direct URL, exactly as production
// does. Unset, the direct URL defaults to INGEST_TEST_DATABASE_URL.
package testenv

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/blob"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/config"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
)

type Env struct {
	DatabaseURL string
	// DirectURL bypasses any pooler. Used for migrations and LISTEN only.
	DirectURL string
	Pool      *pgxpool.Pool
	Blobs     *blob.Store
}

// SetupDB migrates the test database and connects. Postgres only.
func SetupDB(t *testing.T) *Env {
	t.Helper()
	dbURL := os.Getenv("INGEST_TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("set INGEST_TEST_DATABASE_URL to run Postgres integration tests")
	}
	directURL := getenv("INGEST_TEST_DIRECT_DATABASE_URL", dbURL)
	if err := store.Migrate(directURL); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	poolCfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		t.Fatalf("parse database url: %v", err)
	}
	// Pool size is set here rather than as a `pool_max_conns` query parameter,
	// because the same URL is handed to golang-migrate, which opens a plain
	// connection and rejects pgxpool-only parameters.
	if v := os.Getenv("INGEST_TEST_POOL_MAX_CONNS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			t.Fatalf("INGEST_TEST_POOL_MAX_CONNS must be a positive integer, got %q", v)
		}
		poolCfg.MaxConns = int32(n)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return &Env{DatabaseURL: dbURL, DirectURL: directURL, Pool: pool}
}

// Setup is SetupDB plus S3, with the test bucket created if missing.
func Setup(t *testing.T) *Env {
	t.Helper()
	endpoint := os.Getenv("INGEST_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set INGEST_TEST_S3_ENDPOINT (and INGEST_TEST_DATABASE_URL) to run S3 integration tests")
	}
	env := SetupDB(t)
	ctx := context.Background()

	blobs, err := blob.New(ctx, config.S3{
		Endpoint:  endpoint,
		Bucket:    getenv("INGEST_TEST_S3_BUCKET", "ingest-test"),
		Region:    "us-east-1",
		AccessKey: getenv("INGEST_TEST_S3_ACCESS_KEY", "minioadmin"),
		SecretKey: getenv("INGEST_TEST_S3_SECRET_KEY", "minioadmin"),
	})
	if err != nil {
		t.Fatalf("s3 client: %v", err)
	}
	if err := blobs.EnsureBucket(ctx); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}
	env.Blobs = blobs
	return env
}

// Listener holds a dedicated connection LISTENing on the frame channel. It must
// be a direct connection: LISTEN does not work through transaction poolers.
type Listener struct{ conn *pgx.Conn }

func (e *Env) Listen(t *testing.T) *Listener {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), e.DirectURL)
	if err != nil {
		t.Fatalf("listen connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	if _, err := conn.Exec(context.Background(), "LISTEN "+store.NotifyChannel); err != nil {
		t.Fatalf("LISTEN: %v", err)
	}
	return &Listener{conn: conn}
}

// WaitFor reports whether a notification with this payload arrives within d.
// Other payloads are skipped: test packages run in parallel against one DB.
func (l *Listener) WaitFor(payload string, d time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	for {
		n, err := l.conn.WaitForNotification(ctx)
		if err != nil {
			return false
		}
		if n.Payload == payload {
			return true
		}
	}
}

func getenv(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}

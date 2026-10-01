// Command migrate-telemetry copies the legacy MongoDB `telemetry` collection
// into the DynamoDB table, then verifies the two agree.
//
//	migrate-telemetry backfill   copy rows, in batches, resumable
//	migrate-telemetry verify     compare counts and spot-check values
//	migrate-telemetry bench      write latency, Postgres vs DynamoDB
//
// This is a one-off migration tool, deleted after the cutover. It is a separate
// command so the service itself never links the Mongo driver.
//
// Re-running it is safe: the DynamoDB key is (device_id, recorded_at), so a
// repeated row overwrites itself rather than duplicating. That means an
// interrupted backfill is resumed by simply running it again.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/config"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
)

// mongoTelemetry mirrors the document the Rust service writes. Timestamps are
// stored as RFC3339 strings there, not BSON dates.
type mongoTelemetry struct {
	DeviceID     string   `bson:"device_id"`
	RecordedAt   string   `bson:"recorded_at"`
	TemperatureC *float64 `bson:"temperature_c"`
	HumidityPct  *float64 `bson:"humidity_pct"`
	ProbeTempC   *float64 `bson:"probe_temp_c"`
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	fs := flag.NewFlagSet("migrate-telemetry", flag.ExitOnError)
	mongoURI := fs.String("mongo-uri", envOr("MONGODB_URI", "mongodb://localhost:27017"), "source MongoDB URI")
	mongoDB := fs.String("mongo-db", envOr("MONGODB_DB", "observatory_dev"), "source database")
	batchSize := fs.Int("batch", 500, "rows read per batch")
	benchN := fs.Int("n", 200, "writes per backend for bench")
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: migrate-telemetry <backfill|verify|bench> [flags]")
	}
	cmd := os.Args[1]
	_ = fs.Parse(os.Args[2:])

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx := context.Background()

	dyn, err := store.NewDynamoTelemetry(ctx, store.DynamoConfig{
		Endpoint:  cfg.Dynamo.Endpoint,
		Region:    cfg.Dynamo.Region,
		Table:     cfg.Dynamo.Table,
		AccessKey: cfg.Dynamo.AccessKey,
		SecretKey: cfg.Dynamo.SecretKey,
		TTL:       time.Duration(cfg.Dynamo.TTLDays) * 24 * time.Hour,
	})
	if err != nil {
		return err
	}
	if err := dyn.EnsureTable(ctx); err != nil {
		return err
	}

	if cmd == "bench" {
		return bench(ctx, log, cfg, dyn, *benchN) // needs Postgres, not Mongo
	}

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(*mongoURI))
	if err != nil {
		return fmt.Errorf("connect mongo: %w", err)
	}
	defer client.Disconnect(ctx)
	coll := client.Database(*mongoDB).Collection("telemetry")

	switch cmd {
	case "backfill":
		return backfill(ctx, log, coll, dyn, int32(*batchSize))
	case "verify":
		return verify(ctx, log, coll, dyn)

	default:
		return fmt.Errorf("unknown command %q (want backfill, verify or bench)", cmd)
	}
}

func backfill(ctx context.Context, log *slog.Logger, coll *mongo.Collection, dyn *store.DynamoTelemetry, batch int32) error {
	total, err := coll.CountDocuments(ctx, bson.M{})
	if err != nil {
		return fmt.Errorf("count source: %w", err)
	}
	log.Info("backfill starting", "source_rows", total)

	// Sorted by _id so batches are stable and progress is monotonic.
	cur, err := coll.Find(ctx, bson.M{}, options.Find().SetSort(bson.M{"_id": 1}).SetBatchSize(batch))
	if err != nil {
		return fmt.Errorf("find: %w", err)
	}
	defer cur.Close(ctx)

	start := time.Now()
	var copied, skipped int
	pending := make([]store.TelemetryReading, 0, batch)

	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := dyn.BatchInsert(ctx, pending); err != nil {
			return err
		}
		copied += len(pending)
		log.Info("progress", "copied", copied, "of", total)
		pending = pending[:0]
		return nil
	}

	for cur.Next(ctx) {
		var doc mongoTelemetry
		if err := cur.Decode(&doc); err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		recordedAt, err := time.Parse(time.RFC3339Nano, doc.RecordedAt)
		if err != nil || doc.DeviceID == "" {
			// A row without a usable key cannot be migrated; count it rather
			// than failing the run, and report the total at the end.
			skipped++
			continue
		}
		pending = append(pending, store.TelemetryReading{
			DeviceID:     doc.DeviceID,
			RecordedAt:   recordedAt,
			TemperatureC: doc.TemperatureC,
			HumidityPct:  doc.HumidityPct,
			ProbeTempC:   doc.ProbeTempC,
		})
		if len(pending) >= int(batch) {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := cur.Err(); err != nil {
		return fmt.Errorf("cursor: %w", err)
	}
	if err := flush(); err != nil {
		return err
	}

	log.Info("backfill done",
		"copied", copied, "skipped", skipped, "elapsed", time.Since(start).Round(time.Millisecond))
	return nil
}

// verify compares per-device counts and spot-checks the newest row, because a
// count alone would not catch values mangled in translation.
func verify(ctx context.Context, log *slog.Logger, coll *mongo.Collection, dyn *store.DynamoTelemetry) error {
	devices, err := coll.Distinct(ctx, "device_id", bson.M{})
	if err != nil {
		return fmt.Errorf("distinct devices: %w", err)
	}

	ok := true
	for _, d := range devices {
		deviceID, _ := d.(string)
		if deviceID == "" {
			continue
		}
		srcCount, err := coll.CountDocuments(ctx, bson.M{"device_id": deviceID})
		if err != nil {
			return err
		}
		dstCount, err := dyn.CountDevice(ctx, deviceID)
		if err != nil {
			return err
		}
		match := srcCount == int64(dstCount)
		if !match {
			ok = false
		}
		log.Info("count check", "device_id", deviceID, "mongo", srcCount, "dynamo", dstCount, "match", match)

		// Spot-check the newest reading's value, not just its presence.
		var newest mongoTelemetry
		err = coll.FindOne(ctx, bson.M{"device_id": deviceID},
			options.FindOne().SetSort(bson.M{"recorded_at": -1})).Decode(&newest)
		if err != nil {
			return err
		}
		at, err := time.Parse(time.RFC3339Nano, newest.RecordedAt)
		if err != nil {
			continue
		}
		got, err := dyn.QueryRange(ctx, deviceID, at.Add(-time.Second), at.Add(time.Second), 10)
		if err != nil {
			return err
		}
		found := false
		for _, r := range got {
			if equalPtr(r.TemperatureC, newest.TemperatureC) && equalPtr(r.HumidityPct, newest.HumidityPct) {
				found = true
				break
			}
		}
		if !found {
			ok = false
		}
		log.Info("value check", "device_id", deviceID, "recorded_at", newest.RecordedAt, "match", found)
	}

	if !ok {
		return fmt.Errorf("verification FAILED: source and destination differ")
	}
	log.Info("verification passed: counts and spot-checked values agree")
	return nil
}

// bench writes the same readings to both backends and reports the percentiles.
// The point is to decide the migration with a number rather than a preference —
// and to be able to say what the number was when asked.
func bench(ctx context.Context, log *slog.Logger, cfg config.Config, dyn *store.DynamoTelemetry, n int) error {
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	pg := store.NewPostgres(pool)

	device := fmt.Sprintf("bench-%d", time.Now().Unix())
	base := time.Now().UTC()
	run := func(name string, w store.TelemetryStore) error {
		lat := make([]time.Duration, 0, n)
		for i := range n {
			temp := float64(i%40) / 2
			r := store.TelemetryReading{
				DeviceID:     device,
				RecordedAt:   base.Add(time.Duration(i) * time.Second),
				TemperatureC: &temp,
			}
			start := time.Now()
			if err := w.InsertTelemetry(ctx, r); err != nil {
				return fmt.Errorf("%s write %d: %w", name, i, err)
			}
			lat = append(lat, time.Since(start))
		}
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		log.Info("bench", "backend", name, "writes", n,
			"p50", lat[n/2].Round(time.Microsecond),
			"p95", lat[n*95/100].Round(time.Microsecond),
			"p99", lat[n*99/100].Round(time.Microsecond))
		return nil
	}

	if err := run("postgres", pg); err != nil {
		return err
	}
	return run("dynamodb", dyn)
}

func equalPtr(a, b *float64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

package store_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
)

// Runs against DynamoDB Local, which speaks the real DynamoDB API:
//
//	docker run -d -p 58000:8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -inMemory -sharedDb
//	INGEST_TEST_DYNAMO_ENDPOINT=http://localhost:58000 go test ./internal/store/
func dynamoForTest(t *testing.T) *store.DynamoTelemetry {
	t.Helper()
	endpoint := os.Getenv("INGEST_TEST_DYNAMO_ENDPOINT")
	if endpoint == "" {
		t.Skip("set INGEST_TEST_DYNAMO_ENDPOINT to run DynamoDB tests")
	}
	d, err := store.NewDynamoTelemetry(context.Background(), store.DynamoConfig{
		Endpoint:  endpoint,
		Region:    "us-east-1",
		Table:     "skycam_telemetry_test",
		AccessKey: "local", // DynamoDB Local accepts any credentials
		SecretKey: "local",
		TTL:       90 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("dynamo client: %v", err)
	}
	if err := d.EnsureTable(context.Background()); err != nil {
		t.Fatalf("ensure table: %v", err)
	}
	return d
}

// Each test uses its own device so the shared table can't leak rows between them.
func testDeviceID() string { return fmt.Sprintf("cam-%d", rand.Uint32()) }

func f64(v float64) *float64 { return &v }

func TestDynamoInsertAndRangeQuery(t *testing.T) {
	d := dynamoForTest(t)
	ctx := context.Background()
	device := testDeviceID()
	base := time.Now().UTC().Truncate(time.Second)

	for i := range 5 {
		err := d.InsertTelemetry(ctx, store.TelemetryReading{
			DeviceID:     device,
			RecordedAt:   base.Add(time.Duration(i) * time.Minute),
			TemperatureC: f64(float64(i)),
			HumidityPct:  f64(60),
		})
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// The sort key is an ISO-8601 timestamp, so a range is a Query, not a scan.
	got, err := d.QueryRange(ctx, device, base.Add(time.Minute), base.Add(3*time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d readings in range, want 3", len(got))
	}
	// ScanIndexForward=false: newest first, which is what the GUI asks for.
	if got[0].RecordedAt.Before(got[len(got)-1].RecordedAt) {
		t.Fatal("results are oldest-first; expected newest-first")
	}
	if got[0].TemperatureC == nil || *got[0].TemperatureC != 3 {
		t.Fatalf("newest temperature = %v, want 3", got[0].TemperatureC)
	}
}

// The key is (device_id, recorded_at), so re-writing a reading overwrites it.
// That is what makes a retried write and a re-run backfill both safe.
func TestDynamoWritesAreIdempotent(t *testing.T) {
	d := dynamoForTest(t)
	ctx := context.Background()
	device := testDeviceID()
	at := time.Now().UTC().Truncate(time.Second)

	for _, temp := range []float64{1, 2} {
		if err := d.InsertTelemetry(ctx, store.TelemetryReading{
			DeviceID: device, RecordedAt: at, TemperatureC: f64(temp),
		}); err != nil {
			t.Fatal(err)
		}
	}

	count, err := d.CountDevice(ctx, device)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("got %d rows after writing the same key twice, want 1", count)
	}
	got, err := d.QueryRange(ctx, device, at.Add(-time.Second), at.Add(time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].TemperatureC == nil || *got[0].TemperatureC != 2 {
		t.Fatalf("temperature = %v, want the later write (2)", got[0].TemperatureC)
	}
}

// Absent readings must stay absent: a missing sensor is not 0 °C.
func TestDynamoOmitsMissingValues(t *testing.T) {
	d := dynamoForTest(t)
	ctx := context.Background()
	device := testDeviceID()
	at := time.Now().UTC().Truncate(time.Second)

	if err := d.InsertTelemetry(ctx, store.TelemetryReading{
		DeviceID: device, RecordedAt: at, TemperatureC: f64(4.5), // no humidity, no probe
	}); err != nil {
		t.Fatal(err)
	}
	got, err := d.QueryRange(ctx, device, at.Add(-time.Second), at.Add(time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].HumidityPct != nil || got[0].ProbeTempC != nil {
		t.Fatalf("absent sensors came back as %v / %v, want nil", got[0].HumidityPct, got[0].ProbeTempC)
	}
}

func TestDynamoBatchInsert(t *testing.T) {
	d := dynamoForTest(t)
	ctx := context.Background()
	device := testDeviceID()
	base := time.Now().UTC().Truncate(time.Second)

	// 60 rows crosses DynamoDB's 25-item batch limit more than twice.
	readings := make([]store.TelemetryReading, 0, 60)
	for i := range 60 {
		readings = append(readings, store.TelemetryReading{
			DeviceID:   device,
			RecordedAt: base.Add(time.Duration(i) * time.Second),
			ProbeTempC: f64(float64(i) / 2),
		})
	}
	if err := d.BatchInsert(ctx, readings); err != nil {
		t.Fatalf("batch insert: %v", err)
	}
	count, err := d.CountDevice(ctx, device)
	if err != nil {
		t.Fatal(err)
	}
	if count != 60 {
		t.Fatalf("got %d rows, want 60", count)
	}
}

// During the migration Postgres stays authoritative: a DynamoDB failure is
// logged, not returned, so ingest keeps working.
func TestDualWriteSurvivesSecondaryFailure(t *testing.T) {
	primary := &recordingTelemetry{}
	secondary := &failingTelemetry{}
	dual := store.NewDualWriteTelemetry(primary, secondary, "dynamo",
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	err := dual.InsertTelemetry(context.Background(), store.TelemetryReading{
		DeviceID: "cam", RecordedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("secondary failure surfaced to the caller: %v", err)
	}
	if primary.calls != 1 || secondary.calls != 1 {
		t.Fatalf("primary=%d secondary=%d, want both written once", primary.calls, secondary.calls)
	}
}

// A primary failure must still fail the call: that one is the source of truth.
func TestDualWriteFailsOnPrimaryFailure(t *testing.T) {
	dual := store.NewDualWriteTelemetry(&failingTelemetry{}, &recordingTelemetry{}, "dynamo",
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := dual.InsertTelemetry(context.Background(), store.TelemetryReading{DeviceID: "cam"}); err == nil {
		t.Fatal("primary failure was swallowed")
	}
}

type recordingTelemetry struct{ calls int }

func (r *recordingTelemetry) InsertTelemetry(context.Context, store.TelemetryReading) error {
	r.calls++
	return nil
}

type failingTelemetry struct{ calls int }

func (f *failingTelemetry) InsertTelemetry(context.Context, store.TelemetryReading) error {
	f.calls++
	return fmt.Errorf("backend unavailable")
}

package store

import (
	"context"
	"log/slog"
)

// DualWriteTelemetry writes every reading to both backends during a migration.
//
// The asymmetry is the point: the primary decides whether the call succeeded,
// and a secondary failure is logged but swallowed. While Postgres is primary,
// a DynamoDB outage cannot break ingest. After the cutover the roles swap, and
// the old store becomes the one that may fail quietly until it is removed.
//
// Writes are sequential rather than concurrent: telemetry is ~0.5 writes/second,
// so the latency is irrelevant, and sequential keeps the failure semantics easy
// to reason about.
type DualWriteTelemetry struct {
	primary   TelemetryStore
	secondary TelemetryStore
	log       *slog.Logger
	// name labels the secondary in logs, so an alert says which store is behind.
	name string
}

func NewDualWriteTelemetry(primary, secondary TelemetryStore, name string, log *slog.Logger) *DualWriteTelemetry {
	return &DualWriteTelemetry{primary: primary, secondary: secondary, name: name, log: log}
}

func (d *DualWriteTelemetry) InsertTelemetry(ctx context.Context, t TelemetryReading) error {
	if err := d.primary.InsertTelemetry(ctx, t); err != nil {
		return err
	}
	if err := d.secondary.InsertTelemetry(ctx, t); err != nil {
		// Divergence to reconcile later, not a reason to fail the device's write.
		d.log.Error("secondary telemetry write failed",
			"backend", d.name, "device_id", t.DeviceID, "recorded_at", t.RecordedAt, "err", err)
	}
	return nil
}

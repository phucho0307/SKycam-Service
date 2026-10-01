// Package store persists frame metadata.
//
// Storage sits behind FrameStore so the backend can be swapped (and benchmarked)
// without touching the gRPC layer. Postgres is the only implementation today.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// NotifyChannel is the Postgres channel a NOTIFY is sent on when a frame is
// committed. The payload is the frame_id.
const NotifyChannel = "frame_ingested"

type Frame struct {
	ID            uuid.UUID
	DeviceID      string
	CapturedAt    time.Time
	TemperatureC  *float64
	ProbeTempC    *float64
	PreviewKey    *string
	FitsKey       *string
	FitsSizeBytes *int64
	FitsSHA256    []byte
}

type FrameStore interface {
	// Exists reports whether a frame with this id has been committed.
	Exists(ctx context.Context, id uuid.UUID) (bool, error)
	// Insert commits a frame and emits a NOTIFY in the same transaction.
	// Inserting an id that already exists is a no-op that returns false and
	// sends no NOTIFY, so client retries are safe.
	Insert(ctx context.Context, f Frame) (inserted bool, err error)
}

// ErrUnknownToken is returned when no device matches a presented token.
var ErrUnknownToken = errors.New("unknown device token")

type TelemetryReading struct {
	DeviceID     string
	RecordedAt   time.Time
	TemperatureC *float64
	HumidityPct  *float64
	ProbeTempC   *float64
}

// Settings mirrors device_settings. Nil means "unset — use the device default",
// which is distinct from zero.
type Settings struct {
	DeviceID          string
	ExposureMs        *float64
	Gain              *int64
	PreviewGamma      *float64
	PreviewContrast   *float64
	PreviewBrightness *float64
	UpdatedAt         time.Time
	UpdatedBy         *string
}

type DeviceStore interface {
	// AuthenticateDevice maps a bearer token to the device it belongs to.
	// Returns ErrUnknownToken when nothing matches.
	AuthenticateDevice(ctx context.Context, token string) (deviceID string, err error)
	// UpsertDevice registers a device or replaces its token.
	UpsertDevice(ctx context.Context, deviceID, token, displayName string) error
	// TouchDevice records liveness. Best-effort: failures must not drop a session.
	TouchDevice(ctx context.Context, deviceID string, at time.Time) error
}

type TelemetryStore interface {
	InsertTelemetry(ctx context.Context, t TelemetryReading) error
}

type SettingsStore interface {
	// GetSettings returns nil when a device has no stored settings yet.
	GetSettings(ctx context.Context, deviceID string) (*Settings, error)
	// UpsertSettings applies a last-write-wins update and records an audit row
	// in the same transaction, so a change can never be applied unrecorded.
	UpsertSettings(ctx context.Context, s Settings, changedBy string) (Settings, error)
}

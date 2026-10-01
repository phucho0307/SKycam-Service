// Package server implements the SkycamService and SkycamControlService gRPC APIs.
package server

import (
	"log/slog"
	"sync"
	"time"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/blob"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/cluster"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/session"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
)

type Limits struct {
	MaxFitsBytes    int64
	MaxPreviewBytes int
	// SessionIdleTimeout drops a device that has sent nothing for this long,
	// including heartbeats. It catches half-open connections that still look
	// alive locally.
	SessionIdleTimeout time.Duration
}

type Stores struct {
	Frames    store.FrameStore
	Devices   store.DeviceStore
	Telemetry store.TelemetryStore
	Settings  store.SettingsStore
	Grants    store.GrantStore
}

type Server struct {
	skycamv1.UnimplementedSkycamServiceServer

	blobs     *blob.Store
	frames    store.FrameStore
	devices   store.DeviceStore
	telemetry store.TelemetryStore
	settings  store.SettingsStore
	grants    store.GrantStore
	sessions  *session.Registry
	// bus routes commands to sessions held by other replicas. Nil means
	// single-replica operation, where the local registry is the whole picture.
	bus    cluster.Bus
	limits Limits
	log    *slog.Logger

	// One upload per frame at a time. A retry arriving while the previous
	// stream is still alive server-side gets codes.Aborted and backs off.
	// In-process only: correct for a single replica. Several replicas would
	// need a shared lock, e.g. pg_advisory_lock on the frame id.
	inflight keyedLock
}

// busAckTimeout bounds how long a replica holding a stream waits for the device
// to acknowledge a command routed from elsewhere. Shorter than a caller's own
// deadline, so the caller learns "unacknowledged" rather than simply timing out.
const busAckTimeout = 10 * time.Second

// WithBus enables cross-replica command routing.
func (s *Server) WithBus(b cluster.Bus) *Server {
	s.bus = b
	return s
}

func New(blobs *blob.Store, s Stores, limits Limits, log *slog.Logger) *Server {
	if limits.SessionIdleTimeout <= 0 {
		limits.SessionIdleTimeout = 90 * time.Second
	}
	return &Server{
		blobs:     blobs,
		frames:    s.Frames,
		devices:   s.Devices,
		telemetry: s.Telemetry,
		settings:  s.Settings,
		grants:    s.Grants,
		sessions:  session.NewRegistry(),
		limits:    limits,
		log:       log,
	}
}

// Sessions exposes the registry for shutdown and tests.
func (s *Server) Sessions() *session.Registry { return s.sessions }

// Control returns the operator-facing service backed by the same state.
func (s *Server) Control() *ControlServer { return &ControlServer{Server: s} }

type keyedLock struct {
	mu   sync.Mutex
	held map[string]struct{}
}

// tryLock returns an unlock func, or ok=false if key is already held.
func (l *keyedLock) tryLock(key string) (unlock func(), ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held == nil {
		l.held = map[string]struct{}{}
	}
	if _, busy := l.held[key]; busy {
		return nil, false
	}
	l.held[key] = struct{}{}
	return func() {
		l.mu.Lock()
		delete(l.held, key)
		l.mu.Unlock()
	}, true
}

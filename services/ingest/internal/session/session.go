// Package session tracks the devices currently holding an open DeviceSession,
// so a request arriving on another RPC can reach a camera that cannot be dialled.
//
// Concurrency rules this package exists to enforce:
//
//   - A gRPC stream tolerates one goroutine receiving and one sending at the
//     same time, but never two senders. Everything outbound therefore goes
//     through this type's channels, and only the session's own send loop
//     touches the stream.
//   - Settings coalesce: only the newest value matters, so a slow device gets
//     the latest state rather than a backlog of stale ones.
//   - Commands queue and never coalesce, but the queue is bounded: a device too
//     far behind gets an error rather than unbounded memory.
//
// Scope note: this registry is per-process. With one replica that is the whole
// picture; with several, a push would have to be routed to the replica holding
// the stream (Redis pub/sub, or sticky routing by device id).
package session

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
)

var (
	// ErrNotConnected means no session is open for that device right now.
	ErrNotConnected = errors.New("device is not connected")
	// ErrBusy means the device's command queue is full.
	ErrBusy = errors.New("device command queue is full")
	// ErrReplaced means a newer connection for the same device took over.
	ErrReplaced = errors.New("session replaced by a newer connection")
)

const commandQueueDepth = 8

type Session struct {
	DeviceID    string
	ConnectedAt time.Time

	cancel context.CancelCauseFunc

	// wake carries "there is something to send" without carrying the payload,
	// so a burst of updates collapses into one wakeup.
	wake     chan struct{}
	commands chan *skycamv1.Command

	mu       sync.Mutex
	pending  *skycamv1.DeviceSettings // newest settings not yet written to the stream
	acks     map[string]chan *skycamv1.CommandAck
	lastSeen atomic.Int64 // unix nanos of the last message from the device
}

func New(deviceID string, cancel context.CancelCauseFunc) *Session {
	s := &Session{
		DeviceID:    deviceID,
		ConnectedAt: time.Now(),
		cancel:      cancel,
		wake:        make(chan struct{}, 1),
		commands:    make(chan *skycamv1.Command, commandQueueDepth),
		acks:        make(map[string]chan *skycamv1.CommandAck),
	}
	s.MarkSeen()
	return s
}

// PushSettings queues settings for delivery, replacing anything not yet sent.
// Never blocks: settings are last-write-wins by definition.
func (s *Session) PushSettings(ds *skycamv1.DeviceSettings) {
	s.mu.Lock()
	s.pending = ds
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default: // a wakeup is already queued; it will pick up the newer value
	}
}

func (s *Session) takePending() *skycamv1.DeviceSettings {
	s.mu.Lock()
	defer s.mu.Unlock()
	ds := s.pending
	s.pending = nil
	return ds
}

// SendCommand queues a command and, if wait is true, blocks until the device
// acknowledges it or ctx expires.
func (s *Session) SendCommand(ctx context.Context, cmd *skycamv1.Command, wait bool) (*skycamv1.CommandAck, error) {
	var ackCh chan *skycamv1.CommandAck
	if wait {
		ackCh = make(chan *skycamv1.CommandAck, 1)
		s.mu.Lock()
		s.acks[cmd.GetCommandId()] = ackCh
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			delete(s.acks, cmd.GetCommandId())
			s.mu.Unlock()
		}()
	}

	select {
	case s.commands <- cmd:
	default:
		return nil, ErrBusy
	}
	if !wait {
		return nil, nil
	}
	select {
	case ack := <-ackCh:
		return ack, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// DeliverAck routes an acknowledgement back to the waiting SendCommand call.
// An ack for an unknown or already-timed-out command is dropped.
func (s *Session) DeliverAck(ack *skycamv1.CommandAck) {
	s.mu.Lock()
	ch, ok := s.acks[ack.GetCommandId()]
	s.mu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- ack:
	default:
	}
}

// MarkSeen records that the device just sent something.
func (s *Session) MarkSeen() { s.lastSeen.Store(time.Now().UnixNano()) }

func (s *Session) LastSeen() time.Time { return time.Unix(0, s.lastSeen.Load()) }

// Close tears the session down; the handler's goroutines observe the cancelled
// context and return.
func (s *Session) Close(cause error) { s.cancel(cause) }

// Next blocks until there is something to send, or ctx ends. Only the send loop
// calls it, which is what keeps a single writer on the stream.
func (s *Session) Next(ctx context.Context) (*skycamv1.DeviceSessionResponse, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case cmd := <-s.commands:
			return &skycamv1.DeviceSessionResponse{
				Msg: &skycamv1.DeviceSessionResponse_Command{Command: cmd},
			}, nil
		case <-s.wake:
			if ds := s.takePending(); ds != nil {
				return &skycamv1.DeviceSessionResponse{
					Msg: &skycamv1.DeviceSessionResponse_Settings{Settings: ds},
				}, nil
			}
			// Superseded by a newer wakeup that already drained it; loop.
		}
	}
}

// Registry maps device ids to their open session.
type Registry struct {
	mu sync.RWMutex
	m  map[string]*Session
}

func NewRegistry() *Registry { return &Registry{m: make(map[string]*Session)} }

// Add registers a session. If that device already has one, the older session is
// closed and replaced: a device that reconnects is usually telling us the
// previous connection is dead, and the old one may be a half-open socket the
// server hasn't noticed yet. Returns true if it displaced an older session.
func (r *Registry) Add(s *Session) bool {
	r.mu.Lock()
	old, existed := r.m[s.DeviceID]
	r.m[s.DeviceID] = s
	r.mu.Unlock()
	if existed {
		old.Close(ErrReplaced)
	}
	return existed
}

// Remove deletes the session only if it is still the registered one. Without
// the identity check, a slow teardown could evict the newer connection that
// already replaced it.
func (r *Registry) Remove(s *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.m[s.DeviceID]; ok && cur == s {
		delete(r.m, s.DeviceID)
	}
}

func (r *Registry) Get(deviceID string) (*Session, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.m[deviceID]
	return s, ok
}

func (r *Registry) List() []*Session {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Session, 0, len(r.m))
	for _, s := range r.m {
		out = append(out, s)
	}
	return out
}

func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.m)
}

// CloseAll ends every session, for graceful shutdown.
func (r *Registry) CloseAll(cause error) {
	for _, s := range r.List() {
		s.Close(cause)
	}
}

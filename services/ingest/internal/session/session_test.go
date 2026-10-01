package session

import (
	"context"
	"errors"
	"testing"
	"time"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
)

func newTestSession(deviceID string) (*Session, context.Context) {
	ctx, cancel := context.WithCancelCause(context.Background())
	return New(deviceID, cancel), ctx
}

func settings(gain int64) *skycamv1.DeviceSettings {
	return &skycamv1.DeviceSettings{DeviceId: "cam", Gain: &gain}
}

// A device that is slow, or briefly unreachable, must not receive a backlog of
// stale settings — only the newest value is worth sending.
func TestPushSettingsCoalesces(t *testing.T) {
	s, ctx := newTestSession("cam")
	s.PushSettings(settings(1))
	s.PushSettings(settings(2))
	s.PushSettings(settings(3))

	msg, err := s.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := msg.GetSettings().GetGain(); got != 3 {
		t.Fatalf("got gain %d, want the newest (3)", got)
	}

	// Nothing else is queued: the two superseded values were dropped.
	ctx2, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := s.Next(ctx2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want nothing further queued", err)
	}
}

// Commands are distinct work items, so they queue rather than coalesce.
func TestCommandsQueueInOrder(t *testing.T) {
	s, ctx := newTestSession("cam")
	for _, id := range []string{"a", "b"} {
		if _, err := s.SendCommand(ctx, &skycamv1.Command{CommandId: id}, false); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"a", "b"} {
		msg, err := s.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got := msg.GetCommand().GetCommandId(); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

// A device too far behind gets an error rather than unbounded memory.
func TestCommandQueueIsBounded(t *testing.T) {
	s, ctx := newTestSession("cam")
	for i := 0; i < commandQueueDepth; i++ {
		if _, err := s.SendCommand(ctx, &skycamv1.Command{CommandId: "x"}, false); err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
	}
	if _, err := s.SendCommand(ctx, &skycamv1.Command{CommandId: "overflow"}, false); !errors.Is(err, ErrBusy) {
		t.Fatalf("got %v, want ErrBusy once the queue is full", err)
	}
}

func TestSendCommandWaitsForAck(t *testing.T) {
	s, ctx := newTestSession("cam")
	go func() {
		if _, err := s.Next(ctx); err != nil { // the send loop's role
			return
		}
		s.DeliverAck(&skycamv1.CommandAck{CommandId: "cmd-1", Ok: true})
	}()

	ack, err := s.SendCommand(ctx, &skycamv1.Command{CommandId: "cmd-1"}, true)
	if err != nil || !ack.GetOk() {
		t.Fatalf("ack=%v err=%v", ack, err)
	}
}

func TestSendCommandGivesUpOnTimeout(t *testing.T) {
	s, ctx := newTestSession("cam")
	ctx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer cancel()
	if _, err := s.SendCommand(ctx, &skycamv1.Command{CommandId: "never-acked"}, true); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}
}

// An ack for a command nobody is waiting on must not block the receive loop.
func TestDeliverAckForUnknownCommandIsDropped(t *testing.T) {
	s, _ := newTestSession("cam")
	done := make(chan struct{})
	go func() { s.DeliverAck(&skycamv1.CommandAck{CommandId: "ghost"}); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("DeliverAck blocked on an unknown command")
	}
}

// A reconnecting device usually means the old connection is dead — possibly a
// half-open socket the server hasn't noticed. The newest connection wins.
func TestRegistryReplacesOlderSession(t *testing.T) {
	r := NewRegistry()
	first, firstCtx := newTestSession("cam")
	second, _ := newTestSession("cam")

	if replaced := r.Add(first); replaced {
		t.Fatal("first Add reported a replacement")
	}
	if replaced := r.Add(second); !replaced {
		t.Fatal("second Add should report replacing the first")
	}
	if !errors.Is(context.Cause(firstCtx), ErrReplaced) {
		t.Fatalf("old session cause = %v, want ErrReplaced", context.Cause(firstCtx))
	}
	if got, _ := r.Get("cam"); got != second {
		t.Fatal("registry should hold the newer session")
	}
}

// The displaced session's teardown runs after the replacement registered. It
// must not evict the newer session on its way out.
func TestRemoveOnlyEvictsItself(t *testing.T) {
	r := NewRegistry()
	first, _ := newTestSession("cam")
	second, _ := newTestSession("cam")
	r.Add(first)
	r.Add(second)

	r.Remove(first) // the late teardown of the replaced session
	if _, ok := r.Get("cam"); !ok {
		t.Fatal("stale Remove evicted the live session")
	}
	r.Remove(second)
	if r.Count() != 0 {
		t.Fatalf("count = %d, want 0", r.Count())
	}
}

func TestCloseAllEndsEverySession(t *testing.T) {
	r := NewRegistry()
	a, aCtx := newTestSession("cam-a")
	b, bCtx := newTestSession("cam-b")
	r.Add(a)
	r.Add(b)

	shutdown := errors.New("shutting down")
	r.CloseAll(shutdown)
	for _, ctx := range []context.Context{aCtx, bCtx} {
		if !errors.Is(context.Cause(ctx), shutdown) {
			t.Fatalf("cause = %v, want the shutdown cause", context.Cause(ctx))
		}
	}
}

func TestNotConnectedLookup(t *testing.T) {
	r := NewRegistry()
	if _, ok := r.Get("nobody"); ok {
		t.Fatal("empty registry returned a session")
	}
}

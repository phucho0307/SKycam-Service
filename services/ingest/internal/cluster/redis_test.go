package cluster

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Against a real Redis. Pub/sub delivery, key expiry and SCAN semantics are
// properties of Redis, so a fake would only assert my beliefs about it.
//
//	INGEST_TEST_REDIS_URL=redis://localhost:6379/13 go test ./internal/cluster/
func newBus(t *testing.T, cfg Config) *RedisBus {
	t.Helper()
	url := os.Getenv("INGEST_TEST_REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/13"
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("no Redis at %s: %v", url, err)
	}
	t.Cleanup(func() { rdb.Close() })
	return NewRedisBus(rdb, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// Each test gets its own key prefix, so tests never see each other's presence
// keys or published messages even though they share one Redis database.
func testCfg() Config {
	return Config{KeyPrefix: "t-" + uuid.NewString()[:8], TTL: 2 * time.Second}
}

func TestPresenceIsVisibleToAnotherReplica(t *testing.T) {
	cfg := testCfg()
	a, b := newBus(t, cfg), newBus(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if a.ReplicaID() == b.ReplicaID() {
		t.Fatal("two buses must have distinct replica ids")
	}

	release, err := b.Announce(ctx, "cam-1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	// The whole point: replica A can find out that B holds the stream.
	holder, err := a.Holder(ctx, "cam-1")
	if err != nil {
		t.Fatalf("A could not find the holder: %v", err)
	}
	if holder != b.ReplicaID() {
		t.Errorf("holder = %q, want B (%q)", holder, b.ReplicaID())
	}
}

func TestUnknownDeviceIsNotConnected(t *testing.T) {
	a := newBus(t, testCfg())
	if _, err := a.Holder(context.Background(), "never-seen"); err != ErrNotConnected {
		t.Fatalf("err = %v, want ErrNotConnected", err)
	}
}

func TestReleaseRemovesPresence(t *testing.T) {
	cfg := testCfg()
	a, b := newBus(t, cfg), newBus(t, cfg)
	ctx := context.Background()

	release, err := b.Announce(ctx, "cam-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Holder(ctx, "cam-1"); err != nil {
		t.Fatalf("should be present: %v", err)
	}
	release()
	if _, err := a.Holder(ctx, "cam-1"); err != ErrNotConnected {
		t.Fatalf("after release: err = %v, want ErrNotConnected", err)
	}
}

// A replica that dies cannot clean up, so the TTL has to.
func TestPresenceExpiresWhenARelicaStopsRenewing(t *testing.T) {
	cfg := testCfg()
	cfg.TTL = 1 * time.Second
	// Renewal disabled by making the interval longer than the test, simulating a
	// process that died without releasing.
	cfg.RenewEvery = time.Hour
	a, b := newBus(t, cfg), newBus(t, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	if _, err := b.Announce(ctx, "cam-1"); err != nil {
		t.Fatal(err)
	}
	cancel() // "the replica is gone" -- but the key still exists

	if _, err := a.Holder(context.Background(), "cam-1"); err != nil {
		t.Fatalf("should still be present immediately: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)
	if _, err := a.Holder(context.Background(), "cam-1"); err != ErrNotConnected {
		t.Fatal("a dead replica's claim must expire on its own")
	}
}

// And a live one must not expire.
func TestPresenceSurvivesPastTheTTLWhileRenewed(t *testing.T) {
	cfg := testCfg()
	cfg.TTL = 1 * time.Second
	cfg.RenewEvery = 200 * time.Millisecond
	a, b := newBus(t, cfg), newBus(t, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release, err := b.Announce(ctx, "cam-1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	time.Sleep(1600 * time.Millisecond) // well past one TTL
	if _, err := a.Holder(context.Background(), "cam-1"); err != nil {
		t.Fatalf("a renewed session must not expire: %v", err)
	}
}

func TestCommandIsRoutedToTheHoldingReplica(t *testing.T) {
	cfg := testCfg()
	a, b := newBus(t, cfg), newBus(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release, err := b.Announce(ctx, "cam-1")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	cmds, err := b.Commands(ctx, "cam-1")
	if err != nil {
		t.Fatal(err)
	}

	// A registers for this command's ack before publishing, or the reply could
	// arrive before anyone is listening.
	acks, stop, err := a.AwaitAck("cmd-42")
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	sent := Command{DeviceID: "cam-1", CommandID: "cmd-42",
		Kind: "capture_now", ReplyTo: a.ReplicaID()}
	if err := a.Deliver(ctx, sent); err != nil {
		t.Fatalf("deliver: %v", err)
	}

	select {
	case got := <-cmds:
		if got.CommandID != "cmd-42" || got.Kind != "capture_now" {
			t.Fatalf("B received %+v", got)
		}
		if got.ReplyTo != a.ReplicaID() {
			t.Fatalf("reply_to = %q, want A", got.ReplyTo)
		}
		if err := b.SendAck(ctx, got.ReplyTo, Ack{CommandID: got.CommandID, OK: true}); err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("B never received the command")
	}

	select {
	case ack := <-acks:
		if ack.CommandID != "cmd-42" || !ack.OK {
			t.Fatalf("A received %+v", ack)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("A never received the ack")
	}
}

// Delivering to a device nobody holds must fail immediately, not wait out a
// timeout. Pub/sub alone cannot tell you this, which is why presence exists.
func TestDeliverToAnUnheldDeviceFailsFast(t *testing.T) {
	a := newBus(t, testCfg())
	start := time.Now()
	err := a.Deliver(context.Background(), Command{DeviceID: "nobody", CommandID: "x"})
	if err != ErrNotConnected {
		t.Fatalf("err = %v, want ErrNotConnected", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %s; should fail immediately", elapsed)
	}
}

// The presence key says someone holds it, but that replica is gone and nobody is
// subscribed. Publish reports zero receivers, and that must surface as
// disconnected rather than as a silent wait for an impossible ack.
func TestDeliverWithNoSubscriberIsReportedAsDisconnected(t *testing.T) {
	cfg := testCfg()
	a := newBus(t, cfg)
	ctx := context.Background()

	// Forge a presence key without any subscription behind it.
	if err := a.rdb.Set(ctx, a.sessionKey("ghost"), "dead-replica", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	err := a.Deliver(ctx, Command{DeviceID: "ghost", CommandID: "x"})
	if err != ErrNotConnected {
		t.Fatalf("err = %v, want ErrNotConnected", err)
	}
}

func TestConnectedListsEveryReplicasDevices(t *testing.T) {
	cfg := testCfg()
	a, b := newBus(t, cfg), newBus(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ra, err := a.Announce(ctx, "cam-a")
	if err != nil {
		t.Fatal(err)
	}
	defer ra()
	rb, err := b.Announce(ctx, "cam-b")
	if err != nil {
		t.Fatal(err)
	}
	defer rb()

	got, err := a.Connected(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byDevice := map[string]string{}
	for _, p := range got {
		byDevice[p.DeviceID] = p.Replica
	}
	if byDevice["cam-a"] != a.ReplicaID() {
		t.Errorf("cam-a held by %q, want A", byDevice["cam-a"])
	}
	if byDevice["cam-b"] != b.ReplicaID() {
		t.Errorf("cam-b held by %q, want B", byDevice["cam-b"])
	}
}

// A reconnecting device legitimately moves between replicas, and the new holder
// must win. Refusing would strand the camera behind its own stale key.
func TestReannounceOnAnotherReplicaTakesOver(t *testing.T) {
	cfg := testCfg()
	a, b := newBus(t, cfg), newBus(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	releaseA, err := a.Announce(ctx, "cam-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Announce(ctx, "cam-1"); err != nil {
		t.Fatal(err)
	}

	holder, err := b.Holder(ctx, "cam-1")
	if err != nil {
		t.Fatal(err)
	}
	if holder != b.ReplicaID() {
		t.Fatalf("holder = %q, want the new holder B", holder)
	}

	// A's late teardown must not evict B's claim -- the identity check in the
	// release script is what prevents that, mirroring session.Registry.Remove.
	releaseA()
	holder, err = b.Holder(ctx, "cam-1")
	if err != nil {
		t.Fatalf("B's claim was evicted by A's teardown: %v", err)
	}
	if holder != b.ReplicaID() {
		t.Fatalf("holder = %q after A released, want B", holder)
	}
}

// One ack subscription serves every concurrent command on a replica, so acks for
// other commands must not be mistaken for this one's answer.
// Concurrent commands must each receive their own ack. A single shared channel
// with several readers meant a waiter consumed somebody else's ack and discarded
// it, and the rightful waiter timed out -- found by the four-device integration
// test, fixed by demultiplexing on command id.
func TestEachWaiterGetsItsOwnAck(t *testing.T) {
	cfg := testCfg()
	a, b := newBus(t, cfg), newBus(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ids := []string{"one", "two", "three"}
	chans := map[string]<-chan Ack{}
	for _, id := range ids {
		ch, stop, err := a.AwaitAck(id)
		if err != nil {
			t.Fatal(err)
		}
		defer stop()
		chans[id] = ch
	}
	// Published out of order, to make sure nothing depends on arrival sequence.
	for _, id := range []string{"three", "one", "two"} {
		if err := b.SendAck(ctx, a.ReplicaID(), Ack{CommandID: id, OK: true}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids {
		select {
		case ack := <-chans[id]:
			if ack.CommandID != id {
				t.Fatalf("waiter for %q received %q", id, ack.CommandID)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("waiter for %q never received its ack", id)
		}
	}
}

// A waiter that has gone away must not block the dispatcher for everyone else.
func TestAckForACancelledWaiterIsDropped(t *testing.T) {
	cfg := testCfg()
	a, b := newBus(t, cfg), newBus(t, cfg)
	ctx := context.Background()

	_, stop, err := a.AwaitAck("abandoned")
	if err != nil {
		t.Fatal(err)
	}
	stop() // the caller timed out

	live, stopLive, err := a.AwaitAck("live")
	if err != nil {
		t.Fatal(err)
	}
	defer stopLive()

	if err := b.SendAck(ctx, a.ReplicaID(), Ack{CommandID: "abandoned", OK: true}); err != nil {
		t.Fatal(err)
	}
	if err := b.SendAck(ctx, a.ReplicaID(), Ack{CommandID: "live", OK: true}); err != nil {
		t.Fatal(err)
	}
	select {
	case ack := <-live:
		if ack.CommandID != "live" {
			t.Fatalf("got %q", ack.CommandID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("an abandoned waiter blocked ack dispatch")
	}
}

// An ack addressed to another replica must not arrive here.
func TestAcksAreAddressedToOneReplica(t *testing.T) {
	cfg := testCfg()
	a, b := newBus(t, cfg), newBus(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	acks, stop, err := a.AwaitAck("not-yours")
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if err := b.SendAck(ctx, "some-other-replica", Ack{CommandID: "not-yours", OK: true}); err != nil {
		t.Fatal(err)
	}
	select {
	case ack := <-acks:
		t.Fatalf("A received an ack addressed elsewhere: %+v", ack)
	case <-time.After(500 * time.Millisecond):
		// Correct: nothing arrived.
	}
}

// Commands for one device must not reach a replica holding a different one.
func TestCommandsAreScopedToTheDevice(t *testing.T) {
	cfg := testCfg()
	a, b := newBus(t, cfg), newBus(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rb, err := b.Announce(ctx, "cam-b")
	if err != nil {
		t.Fatal(err)
	}
	defer rb()
	cmds, err := b.Commands(ctx, "cam-b")
	if err != nil {
		t.Fatal(err)
	}

	ra, err := a.Announce(ctx, "cam-a")
	if err != nil {
		t.Fatal(err)
	}
	defer ra()
	if _, err := a.Commands(ctx, "cam-a"); err != nil {
		t.Fatal(err)
	}
	if err := a.Deliver(ctx, Command{DeviceID: "cam-a", CommandID: "for-a",
		Kind: "capture_now", ReplyTo: a.ReplicaID()}); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-cmds:
		t.Fatalf("B (holding cam-b) received a command for cam-a: %+v", got)
	case <-time.After(500 * time.Millisecond):
	}
}

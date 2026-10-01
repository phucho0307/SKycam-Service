package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/auth"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/cluster"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/testenv"
)

// Two *real* gRPC servers sharing one database and one Redis, which is what a
// two-replica deployment is. The device connects to one; the operator calls the
// other. Before the bus existed this combination simply could not work: the
// command channel lives in the holding process's memory.

type replica struct {
	srv  *Server
	bus  *cluster.RedisBus
	dial func(t *testing.T, token string) *grpc.ClientConn
}

func newReplicaPair(t *testing.T) (a, b *replica, pg *store.Postgres) {
	t.Helper()
	env := testenv.Setup(t)
	pg = store.NewPostgres(env.Pool)

	url := os.Getenv("INGEST_TEST_REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/13"
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	// A prefix per test, so two tests running against one Redis cannot see each
	// other's presence keys or published commands.
	prefix := "it-" + uuid.NewString()[:8]
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	mk := func() *replica {
		rdb := redis.NewClient(opt)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			t.Skipf("no Redis at %s: %v", url, err)
		}
		t.Cleanup(func() { rdb.Close() })

		bus := cluster.NewRedisBus(rdb, cluster.Config{
			KeyPrefix: prefix, TTL: 5 * time.Second, RenewEvery: time.Second}, log)

		srv := New(env.Blobs,
			Stores{Frames: pg, Devices: pg, Telemetry: pg, Settings: pg, Grants: pg},
			Limits{MaxFitsBytes: 8 << 20, MaxPreviewBytes: 1 << 20,
				SessionIdleTimeout: 90 * time.Second}, log).WithBus(bus)

		authn := auth.New(pg, testOperatorToken, time.Second)
		gs := grpc.NewServer(
			grpc.UnaryInterceptor(authn.UnaryInterceptor()),
			grpc.StreamInterceptor(authn.StreamInterceptor()),
		)
		skycamv1.RegisterSkycamServiceServer(gs, srv)
		skycamv1.RegisterSkycamControlServiceServer(gs, srv.Control())

		lis := bufconn.Listen(1 << 20)
		go gs.Serve(lis)
		t.Cleanup(gs.Stop)

		return &replica{srv: srv, bus: bus, dial: func(t *testing.T, token string) *grpc.ClientConn {
			t.Helper()
			conn, err := grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return lis.DialContext(ctx)
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithPerRPCCredentials(tokenCreds{token: token}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			return conn
		}}
	}
	return mk(), mk(), pg
}

// connectDevice opens a DeviceSession against one replica and sends Hello,
// returning once the server has registered it.
func connectDevice(t *testing.T, r *replica, pg *store.Postgres) (deviceID string,
	stream skycamv1.SkycamService_DeviceSessionClient, cancel context.CancelFunc) {
	t.Helper()
	deviceID, token := newTestDevice()
	if err := pg.UpsertDevice(context.Background(), deviceID, token, "cluster tests"); err != nil {
		t.Fatal(err)
	}
	client := skycamv1.NewSkycamServiceClient(r.dial(t, token))

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.DeviceSession(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := stream.Send(&skycamv1.DeviceSessionRequest{
		Msg: &skycamv1.DeviceSessionRequest_Hello{Hello: &skycamv1.Hello{DeviceId: deviceID}},
	}); err != nil {
		cancel()
		t.Fatal(err)
	}
	// Wait until the announce has landed, so the test is not racing registration.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := r.bus.Holder(context.Background(), deviceID); err == nil {
			return deviceID, stream, cancel
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	t.Fatal("device never registered on the bus")
	return "", nil, nil
}

// deviceAcks plays the Pi: read commands off the stream and acknowledge them.
func deviceAcks(t *testing.T, stream skycamv1.SkycamService_DeviceSessionClient,
	ok bool) chan *skycamv1.Command {
	t.Helper()
	seen := make(chan *skycamv1.Command, 4)
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				return
			}
			cmd := resp.GetCommand()
			if cmd == nil {
				continue
			}
			seen <- cmd
			_ = stream.Send(&skycamv1.DeviceSessionRequest{
				Msg: &skycamv1.DeviceSessionRequest_CommandAck{
					CommandAck: &skycamv1.CommandAck{CommandId: cmd.GetCommandId(), Ok: ok},
				},
			})
		}
	}()
	return seen
}

// The headline: this is impossible without the bus.
func TestCommandCrossesReplicas(t *testing.T) {
	a, b, pg := newReplicaPair(t)
	deviceID, stream, cancel := connectDevice(t, b, pg)
	defer cancel()
	seen := deviceAcks(t, stream, true)

	// Sanity: the device is NOT in replica A's local registry.
	if _, local := a.srv.sessions.Get(deviceID); local {
		t.Fatal("test is not exercising the cross-replica path")
	}

	control := skycamv1.NewSkycamControlServiceClient(a.dial(t, testOperatorToken))
	resp, err := control.SendCommand(context.Background(), &skycamv1.SendCommandRequest{
		DeviceId:     deviceID,
		Command:      &skycamv1.Command{Kind: &skycamv1.Command_CaptureNow{CaptureNow: &skycamv1.CaptureNow{}}},
		AckTimeoutMs: 5000,
	})
	if err != nil {
		t.Fatalf("SendCommand on the non-holding replica failed: %v", err)
	}
	if !resp.GetAcknowledged() {
		t.Fatalf("not acknowledged: %q", resp.GetError())
	}

	select {
	case cmd := <-seen:
		if cmd.GetCaptureNow() == nil {
			t.Fatalf("device received the wrong command: %+v", cmd)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the device never saw the command")
	}
}

// A negative ack must travel back as a failure, not be swallowed as success.
func TestDeviceRejectionSurfacesOnTheOtherReplica(t *testing.T) {
	a, b, pg := newReplicaPair(t)
	deviceID, stream, cancel := connectDevice(t, b, pg)
	defer cancel()
	deviceAcks(t, stream, false) // the device refuses

	control := skycamv1.NewSkycamControlServiceClient(a.dial(t, testOperatorToken))
	resp, err := control.SendCommand(context.Background(), &skycamv1.SendCommandRequest{
		DeviceId:     deviceID,
		Command:      &skycamv1.Command{Kind: &skycamv1.Command_AbortExposure{AbortExposure: &skycamv1.AbortExposure{}}},
		AckTimeoutMs: 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAcknowledged() {
		t.Fatal("a refused command must not report success")
	}
}

// The local path must still be taken when the device is on this replica, rather
// than pointlessly round-tripping through Redis.
func TestLocalSessionStillUsesTheLocalPath(t *testing.T) {
	_, b, pg := newReplicaPair(t)
	deviceID, stream, cancel := connectDevice(t, b, pg)
	defer cancel()
	deviceAcks(t, stream, true)

	if _, local := b.srv.sessions.Get(deviceID); !local {
		t.Fatal("the device should be local to B")
	}
	control := skycamv1.NewSkycamControlServiceClient(b.dial(t, testOperatorToken))
	resp, err := control.SendCommand(context.Background(), &skycamv1.SendCommandRequest{
		DeviceId:     deviceID,
		Command:      &skycamv1.Command{Kind: &skycamv1.Command_CaptureNow{CaptureNow: &skycamv1.CaptureNow{}}},
		AckTimeoutMs: 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetAcknowledged() {
		t.Fatalf("local command not acknowledged: %q", resp.GetError())
	}
}

// An offline device must fail immediately with FailedPrecondition, not wait out
// the ack timeout. That is what the presence key buys over bare pub/sub.
func TestOfflineDeviceFailsFastOnEitherReplica(t *testing.T) {
	a, _, pg := newReplicaPair(t)
	deviceID, token := newTestDevice()
	if err := pg.UpsertDevice(context.Background(), deviceID, token, "never connects"); err != nil {
		t.Fatal(err)
	}

	control := skycamv1.NewSkycamControlServiceClient(a.dial(t, testOperatorToken))
	start := time.Now()
	_, err := control.SendCommand(context.Background(), &skycamv1.SendCommandRequest{
		DeviceId:     deviceID,
		Command:      &skycamv1.Command{Kind: &skycamv1.Command_CaptureNow{CaptureNow: &skycamv1.CaptureNow{}}},
		AckTimeoutMs: 10000,
	})
	wantCode(t, err, codes.FailedPrecondition, "offline device")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("took %s; an offline device must fail fast, not wait for the ack timeout", elapsed)
	}
}

// Before the bus, this listed a third of the fleet on three replicas, which reads
// as an outage rather than a partial view.
func TestListConnectedDevicesSpansReplicas(t *testing.T) {
	a, b, pg := newReplicaPair(t)
	devA, streamA, cancelA := connectDevice(t, a, pg)
	defer cancelA()
	deviceAcks(t, streamA, true)
	devB, streamB, cancelB := connectDevice(t, b, pg)
	defer cancelB()
	deviceAcks(t, streamB, true)

	control := skycamv1.NewSkycamControlServiceClient(a.dial(t, testOperatorToken))
	resp, err := control.ListConnectedDevices(context.Background(),
		&skycamv1.ListConnectedDevicesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, d := range resp.GetDevices() {
		got[d.GetDeviceId()] = true
	}
	if !got[devA] {
		t.Error("A's own device is missing")
	}
	if !got[devB] {
		t.Error("B's device is missing: the list is not spanning replicas")
	}
}

// When a device moves replicas -- which happens on every deploy -- commands must
// follow it rather than going to the pod that used to hold it.
func TestCommandFollowsADeviceThatMovesReplicas(t *testing.T) {
	a, b, pg := newReplicaPair(t)

	deviceID, streamA, cancelA := connectDevice(t, a, pg)
	deviceAcks(t, streamA, true)

	// The device reconnects to B, as it would after A was rolled.
	token := deviceID + "-token"
	clientB := skycamv1.NewSkycamServiceClient(b.dial(t, token))
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	streamB, err := clientB.DeviceSession(ctxB)
	if err != nil {
		t.Fatal(err)
	}
	if err := streamB.Send(&skycamv1.DeviceSessionRequest{
		Msg: &skycamv1.DeviceSessionRequest_Hello{Hello: &skycamv1.Hello{DeviceId: deviceID}},
	}); err != nil {
		t.Fatal(err)
	}
	seenB := deviceAcks(t, streamB, true)

	// Wait for the presence key to name B.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if h, err := b.bus.Holder(context.Background(), deviceID); err == nil && h == b.bus.ReplicaID() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancelA() // the old stream goes away

	control := skycamv1.NewSkycamControlServiceClient(a.dial(t, testOperatorToken))
	resp, err := control.SendCommand(context.Background(), &skycamv1.SendCommandRequest{
		DeviceId:     deviceID,
		Command:      &skycamv1.Command{Kind: &skycamv1.Command_CaptureNow{CaptureNow: &skycamv1.CaptureNow{}}},
		AckTimeoutMs: 5000,
	})
	if err != nil {
		t.Fatalf("command did not follow the device: %v", err)
	}
	if !resp.GetAcknowledged() {
		t.Fatalf("not acknowledged after the move: %q", resp.GetError())
	}
	select {
	case <-seenB:
	case <-time.After(3 * time.Second):
		t.Fatal("the new holder never received the command")
	}
}

// Concurrent commands share one ack subscription per replica, so each caller must
// get its own answer and not another's.
func TestConcurrentCrossReplicaCommandsGetTheirOwnAcks(t *testing.T) {
	a, b, pg := newReplicaPair(t)

	const devices = 4
	streams := make([]chan *skycamv1.Command, 0, devices)
	ids := make([]string, 0, devices)
	for i := 0; i < devices; i++ {
		id, stream, cancel := connectDevice(t, b, pg)
		defer cancel()
		streams = append(streams, deviceAcks(t, stream, true))
		ids = append(ids, id)
	}

	control := skycamv1.NewSkycamControlServiceClient(a.dial(t, testOperatorToken))
	type result struct {
		id   string
		resp *skycamv1.SendCommandResponse
		err  error
	}
	out := make(chan result, devices)
	for _, id := range ids {
		go func(id string) {
			resp, err := control.SendCommand(context.Background(), &skycamv1.SendCommandRequest{
				DeviceId:     id,
				Command:      &skycamv1.Command{Kind: &skycamv1.Command_CaptureNow{CaptureNow: &skycamv1.CaptureNow{}}},
				AckTimeoutMs: 8000,
			})
			out <- result{id, resp, err}
		}(id)
	}

	for i := 0; i < devices; i++ {
		r := <-out
		if r.err != nil {
			t.Fatalf("%s: %v", r.id, r.err)
		}
		if !r.resp.GetAcknowledged() {
			t.Fatalf("%s not acknowledged: %q", r.id, r.resp.GetError())
		}
	}
	for i, ch := range streams {
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("device %d never received its command", i)
		}
	}
}

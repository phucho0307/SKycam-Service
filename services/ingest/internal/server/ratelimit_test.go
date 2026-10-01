package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/auth"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/ratelimit"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/testenv"
)

// rateHarness starts the server with a deliberately tiny device budget, so the
// limit is reachable in a test without sending thousands of requests.
type rateHarness struct {
	pg     *store.Postgres
	dial   func(t *testing.T, token string) *grpc.ClientConn
	newCam func(t *testing.T) (deviceID, token string)
}

func newRateHarness(t *testing.T, deviceRPS float64, deviceBurst int) *rateHarness {
	t.Helper()
	return newRateHarnessWith(t, ratelimit.New(ratelimit.Config{RPS: deviceRPS, Burst: deviceBurst}))
}

// newRateHarnessWith starts one server with the given device limiter. Called
// twice with Redis limiters, it is two replicas sharing one Redis.
func newRateHarnessWith(t *testing.T, deviceRL ratelimit.Allower) *rateHarness {
	t.Helper()
	env := testenv.Setup(t)
	pg := store.NewPostgres(env.Pool)

	srv := New(env.Blobs, Stores{Frames: pg, Devices: pg, Telemetry: pg, Settings: pg, Grants: pg},
		Limits{MaxFitsBytes: 64 << 20, MaxPreviewBytes: 2 << 20, SessionIdleTimeout: 90 * time.Second},
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	authn := auth.New(pg, testOperatorToken, time.Second).WithRateLimits(
		deviceRL,
		nil, // operators unlimited here; a separate test covers them
	)
	gs := grpc.NewServer(
		grpc.UnaryInterceptor(authn.UnaryInterceptor()),
		grpc.StreamInterceptor(authn.StreamInterceptor()),
	)
	skycamv1.RegisterSkycamServiceServer(gs, srv)
	skycamv1.RegisterSkycamControlServiceServer(gs, srv.Control())

	lis := bufconn.Listen(1 << 20)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	return &rateHarness{
		pg: pg,
		dial: func(t *testing.T, token string) *grpc.ClientConn {
			t.Helper()
			conn, err := grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithPerRPCCredentials(tokenCreds{token: token}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			return conn
		},
		newCam: func(t *testing.T) (string, string) {
			t.Helper()
			id, tok := newTestDevice()
			if err := pg.UpsertDevice(context.Background(), id, tok, "rate limit tests"); err != nil {
				t.Fatal(err)
			}
			return id, tok
		},
	}
}

// statusCall is a cheap authenticated RPC, so the test measures the limiter
// rather than the work a handler does.
func statusCall(ctx context.Context, c skycamv1.SkycamServiceClient, deviceID string) error {
	_, err := c.GetUploadStatus(ctx, &skycamv1.GetUploadStatusRequest{
		FrameId:  "00000000-0000-0000-0000-000000000001",
		DeviceId: deviceID,
	})
	return err
}

func TestDeviceIsThrottledPastItsBurst(t *testing.T) {
	h := newRateHarness(t, 0.01, 5) // refill negligible during the test
	id, tok := h.newCam(t)
	client := skycamv1.NewSkycamServiceClient(h.dial(t, tok))
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := statusCall(ctx, client, id); err != nil {
			t.Fatalf("request %d within the burst failed: %v", i+1, err)
		}
	}
	err := statusCall(ctx, client, id)
	if got := statusCodeOf(err); got != codes.ResourceExhausted {
		t.Fatalf("past the burst: got %v (%v), want ResourceExhausted", got, err)
	}
}

// ResourceExhausted, not Unavailable: gRPC clients treat Unavailable as "retry
// straight away", which is the opposite of what an over-quota caller should do.
func TestThrottledCallReturnsResourceExhaustedNotUnavailable(t *testing.T) {
	h := newRateHarness(t, 0.01, 1)
	id, tok := h.newCam(t)
	client := skycamv1.NewSkycamServiceClient(h.dial(t, tok))
	ctx := context.Background()

	_ = statusCall(ctx, client, id)
	err := statusCall(ctx, client, id)
	if got := statusCodeOf(err); got == codes.Unavailable {
		t.Fatal("Unavailable tells the client to retry immediately; that is wrong for a rate limit")
	}
	if got := statusCodeOf(err); got != codes.ResourceExhausted {
		t.Fatalf("got %v, want ResourceExhausted", got)
	}
}

// The point of the whole feature: one runaway camera must not affect the others.
func TestOneRunawayCameraDoesNotAffectAnother(t *testing.T) {
	h := newRateHarness(t, 0.01, 3)
	ctx := context.Background()

	badID, badTok := h.newCam(t)
	goodID, goodTok := h.newCam(t)
	bad := skycamv1.NewSkycamServiceClient(h.dial(t, badTok))
	good := skycamv1.NewSkycamServiceClient(h.dial(t, goodTok))

	// The broken camera burns its budget and then keeps hammering.
	for i := 0; i < 20; i++ {
		_ = statusCall(ctx, bad, badID)
	}
	if got := statusCodeOf(statusCall(ctx, bad, badID)); got != codes.ResourceExhausted {
		t.Fatalf("the runaway should be throttled, got %v", got)
	}

	// The healthy one is untouched.
	for i := 0; i < 3; i++ {
		if err := statusCall(ctx, good, goodID); err != nil {
			t.Fatalf("healthy camera request %d failed: %v", i+1, err)
		}
	}
}

func TestTokensRefillSoAThrottledDeviceRecovers(t *testing.T) {
	// 5 rps refills one token every 200ms: short enough to wait for, and long
	// enough that two back-to-back calls land inside it even under -race, which
	// slows each call ~10x. At 50 rps (20ms) the "immediate" second call
	// sometimes found a fresh token under -race and the test flaked.
	h := newRateHarness(t, 5, 1)
	id, tok := h.newCam(t)
	client := skycamv1.NewSkycamServiceClient(h.dial(t, tok))
	ctx := context.Background()

	if err := statusCall(ctx, client, id); err != nil {
		t.Fatalf("first call failed: %v", err)
	}
	if got := statusCodeOf(statusCall(ctx, client, id)); got != codes.ResourceExhausted {
		t.Fatalf("second immediate call: got %v, want ResourceExhausted", got)
	}

	time.Sleep(300 * time.Millisecond) // a token refills
	if err := statusCall(ctx, client, id); err != nil {
		t.Fatalf("after refill the device should be served again: %v", err)
	}
}

// A stream costs one token at open, not one per message -- otherwise a long
// DeviceSession or a chunked upload would throttle itself mid-transfer.
func TestOpeningAStreamCostsOneToken(t *testing.T) {
	h := newRateHarness(t, 0.01, 2)
	id, tok := h.newCam(t)
	client := skycamv1.NewSkycamServiceClient(h.dial(t, tok))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.DeviceSession(ctx)
	if err != nil {
		t.Fatalf("opening the session failed: %v", err)
	}
	if err := stream.Send(&skycamv1.DeviceSessionRequest{
		Msg: &skycamv1.DeviceSessionRequest_Hello{Hello: &skycamv1.Hello{DeviceId: id}},
	}); err != nil {
		t.Fatalf("hello failed: %v", err)
	}
	// Many messages on the already-open stream, none of which should cost a token.
	for i := 0; i < 50; i++ {
		if err := stream.Send(&skycamv1.DeviceSessionRequest{
			Msg: &skycamv1.DeviceSessionRequest_Heartbeat{Heartbeat: &skycamv1.Heartbeat{}},
		}); err != nil {
			t.Fatalf("heartbeat %d failed: %v", i, err)
		}
	}
	// One token is left, so a separate RPC still works.
	if err := statusCall(context.Background(), client, id); err != nil {
		t.Fatalf("the second token should still be available: %v", err)
	}
}

func TestUnlimitedWhenRPSIsZero(t *testing.T) {
	h := newRateHarness(t, 0, 1)
	id, tok := h.newCam(t)
	client := skycamv1.NewSkycamServiceClient(h.dial(t, tok))
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		if err := statusCall(ctx, client, id); err != nil {
			t.Fatalf("request %d failed with limiting disabled: %v", i+1, err)
		}
	}
}

// The internal service account must not be throttled: it is the platform
// talking to itself over loopback, and limiting it would throttle the GUI.
func TestSharedOperatorTokenIsNotRateLimited(t *testing.T) {
	env := testenv.Setup(t)
	pg := store.NewPostgres(env.Pool)
	srv := New(env.Blobs, Stores{Frames: pg, Devices: pg, Telemetry: pg, Settings: pg, Grants: pg},
		Limits{MaxFitsBytes: 1 << 20, MaxPreviewBytes: 1 << 20, SessionIdleTimeout: time.Minute},
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	// A budget of one, for both audiences.
	authn := auth.New(pg, testOperatorToken, time.Second).WithRateLimits(
		ratelimit.New(ratelimit.Config{RPS: 0.01, Burst: 1}),
		ratelimit.New(ratelimit.Config{RPS: 0.01, Burst: 1}),
	)
	gs := grpc.NewServer(
		grpc.UnaryInterceptor(authn.UnaryInterceptor()),
		grpc.StreamInterceptor(authn.StreamInterceptor()),
	)
	skycamv1.RegisterSkycamControlServiceServer(gs, srv.Control())
	lis := bufconn.Listen(1 << 20)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(tokenCreds{token: testOperatorToken}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	client := skycamv1.NewSkycamControlServiceClient(conn)

	for i := 0; i < 10; i++ {
		if _, err := client.ListConnectedDevices(context.Background(),
			&skycamv1.ListConnectedDevicesRequest{}); err != nil {
			t.Fatalf("service-account call %d was throttled: %v", i+1, err)
		}
	}
}

// statusCodeOf keeps the assertions short.
func statusCodeOf(err error) codes.Code { return status.Code(err) }

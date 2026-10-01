package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/auth"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/testenv"
)

const testOperatorToken = "operator-token-for-tests"

// Each harness gets its own device id and token. Tests share one database, so
// a fixed device would let rows from one test (settings, frames) change what
// another test sees on connect.
func newTestDevice() (deviceID, token string) {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	id := "cam-" + hex.EncodeToString(buf)
	return id, id + "-token"
}

// tokenCreds attaches a bearer token to every call, the way a device client does.
type tokenCreds struct{ token string }

func (c tokenCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.token}, nil
}
func (tokenCreds) RequireTransportSecurity() bool { return false }

type harness struct {
	env     *testenv.Env
	srv     *Server
	device  string
	token   string
	client  skycamv1.SkycamServiceClient // authenticated as this harness's device
	control skycamv1.SkycamControlServiceClient
	// dial and dialConn open extra clients with a chosen token, for auth tests.
	dial     func(t *testing.T, token string) skycamv1.SkycamServiceClient
	dialConn func(t *testing.T, token string) *grpc.ClientConn
}

// newHarness starts the real server over an in-memory connection, with the same
// auth interceptors production uses.
func newHarness(t *testing.T) *harness {
	t.Helper()
	return newHarnessWithIdle(t, 90*time.Second)
}

func newHarnessWithIdle(t *testing.T, idle time.Duration) *harness {
	t.Helper()
	env := testenv.Setup(t)
	pg := store.NewPostgres(env.Pool)
	ctx := context.Background()

	deviceID, deviceToken := newTestDevice()
	if err := pg.UpsertDevice(ctx, deviceID, deviceToken, "integration tests"); err != nil {
		t.Fatalf("register device: %v", err)
	}

	srv := New(env.Blobs, Stores{Frames: pg, Devices: pg, Telemetry: pg, Settings: pg, Grants: pg},
		Limits{MaxFitsBytes: 64 << 20, MaxPreviewBytes: 2 << 20, SessionIdleTimeout: idle},
		slog.New(slog.NewTextHandler(io.Discard, nil)))

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

	dialWith := func(t *testing.T, token string) *grpc.ClientConn {
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
	}

	return &harness{
		env:     env,
		srv:     srv,
		device:  deviceID,
		token:   deviceToken,
		client:  skycamv1.NewSkycamServiceClient(dialWith(t, deviceToken)),
		control: skycamv1.NewSkycamControlServiceClient(dialWith(t, testOperatorToken)),
		dial: func(t *testing.T, token string) skycamv1.SkycamServiceClient {
			return skycamv1.NewSkycamServiceClient(dialWith(t, token))
		},
		dialConn: dialWith,
	}
}

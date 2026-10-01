package auth

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// A Kubernetes gRPC probe carries no credentials. Found by the deployment
// rehearsal: with the health service behind the auth interceptor, every probe
// got Unauthenticated and no ingest pod would ever have become Ready.
func TestHealthCheckNeedsNoCredentialsButOtherMethodsDo(t *testing.T) {
	a := New(nil, "operator-token", 0) // no device store: never reached here

	gs := grpc.NewServer(
		grpc.UnaryInterceptor(a.UnaryInterceptor()),
		grpc.StreamInterceptor(a.StreamInterceptor()),
	)
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(gs, hs)
	lis := bufconn.Listen(1 << 16)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	resp, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("unauthenticated health check refused: %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status %v, want SERVING", resp.Status)
	}

	// Control: the exemption is exact. Every other method still needs a token.
	for _, m := range []string{
		"/skycam.v1.SkycamService/GetUploadStatus",
		"/skycam.v1.SkycamControlService/SendCommand",
		"/grpc.health.v1.Health/CheckButNotReally",
		"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
	} {
		_, err := a.authorize(context.Background(), m)
		if status.Code(err) != codes.Unauthenticated {
			t.Errorf("%s without a token: %v, want Unauthenticated", m, err)
		}
	}
}

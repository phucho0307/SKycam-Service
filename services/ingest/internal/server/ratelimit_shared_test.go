package server

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/ratelimit"
)

// Two gRPC servers, each with its own limiter object -- as two pods would have
// -- sharing only Redis. One camera alternates between them. With in-process
// buckets it would get two bursts; shared, it gets one.
func TestRateLimitIsSharedAcrossReplicas(t *testing.T) {
	url := os.Getenv("INGEST_TEST_REDIS_URL")
	if url == "" {
		url = "redis://localhost:6379/13"
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("no Redis at %s: %v", url, err)
	}

	const burst = 6
	prefix := "t-" + uuid.NewString()[:8]
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := ratelimit.Config{RPS: 0.01, Burst: burst}
	podA := newRateHarnessWith(t, ratelimit.NewRedis(rdb, cfg, prefix, quiet))
	podB := newRateHarnessWith(t, ratelimit.NewRedis(rdb, cfg, prefix, quiet))

	id, tok := podA.newCam(t) // both pods read the same Postgres
	clients := []skycamv1.SkycamServiceClient{
		skycamv1.NewSkycamServiceClient(podA.dial(t, tok)),
		skycamv1.NewSkycamServiceClient(podB.dial(t, tok)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	allowed, refused := 0, 0
	for i := range 2 * burst {
		err := statusCall(ctx, clients[i%2], id)
		switch statusCodeOf(err) {
		case codes.OK:
			allowed++
		case codes.ResourceExhausted:
			refused++
		default:
			t.Fatalf("request %d: unexpected %v", i, err)
		}
	}
	if allowed != burst || refused != burst {
		t.Fatalf("across two replicas: allowed %d, refused %d; want %d and %d (one shared burst)",
			allowed, refused, burst, burst)
	}
}

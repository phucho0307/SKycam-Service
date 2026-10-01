package ratelimit

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Against a real Redis: atomicity of the script and key expiry are properties
// of Redis, so a fake would only assert my beliefs about it.
//
//	INGEST_TEST_REDIS_URL=redis://localhost:6379/13 go test ./internal/ratelimit/
func testRedis(t *testing.T) *redis.Client {
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
	return rdb
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// Each test gets its own prefix, so tests never share a bucket.
func prefix() string { return "t-" + uuid.NewString()[:8] }

func count(l Allower, key string, n int) int {
	ok := 0
	for range n {
		if l.Allow(context.Background(), key) {
			ok++
		}
	}
	return ok
}

func TestRedisBurstThenRefuse(t *testing.T) {
	l := NewRedis(testRedis(t), Config{RPS: 0.001, Burst: 5}, prefix(), quiet)
	if got := count(l, "cam-1", 20); got != 5 {
		t.Fatalf("allowed %d of 20, want exactly the burst of 5", got)
	}
}

// The reason this type exists. Two replicas with in-process buckets would allow
// 2x the burst; sharing Redis they must allow exactly one burst between them.
func TestTwoReplicasShareOneBucket(t *testing.T) {
	rdb := testRedis(t)
	p := prefix()
	a := NewRedis(rdb, Config{RPS: 0.001, Burst: 10}, p, quiet)
	b := NewRedis(rdb, Config{RPS: 0.001, Burst: 10}, p, quiet)

	got := 0
	for range 10 {
		if a.Allow(context.Background(), "cam-1") {
			got++
		}
		if b.Allow(context.Background(), "cam-1") {
			got++
		}
	}
	if got != 10 {
		t.Fatalf("two replicas allowed %d, want 10 (one shared burst)", got)
	}

	// Control: the in-process limiter really does double it. Without this the
	// test above would pass for a limiter that simply refused everything past 5.
	la, lb := New(Config{RPS: 0.001, Burst: 10}), New(Config{RPS: 0.001, Burst: 10})
	local := count(la, "cam-1", 10) + count(lb, "cam-1", 10)
	if local != 20 {
		t.Fatalf("control: in-process replicas allowed %d, expected 20", local)
	}
}

// Atomicity. Fire many concurrent requests from several "replicas"; if the
// read and the write were separate calls, two would read the same last token
// and both spend it, and the total would exceed the burst.
func TestConcurrentCallersNeverOverspend(t *testing.T) {
	rdb := testRedis(t)
	p := prefix()
	const burst = 25
	replicas := make([]*RedisLimiter, 4)
	for i := range replicas {
		replicas[i] = NewRedis(rdb, Config{RPS: 0.001, Burst: burst}, p, quiet)
	}

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if replicas[i%len(replicas)].Allow(context.Background(), "cam-hot") {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if n := allowed.Load(); n != burst {
		t.Fatalf("200 concurrent requests across 4 replicas allowed %d, want exactly %d", n, burst)
	}
}

func TestRefillsAtTheConfiguredRate(t *testing.T) {
	// 20/s refills one token every 50ms. Sleep 4 tokens' worth, with a margin
	// either side so the assertion is about the rate and not scheduler jitter.
	l := NewRedis(testRedis(t), Config{RPS: 20, Burst: 5}, prefix(), quiet)
	count(l, "cam-1", 5)
	if l.Allow(context.Background(), "cam-1") {
		t.Fatal("bucket should be empty after the burst")
	}
	time.Sleep(210 * time.Millisecond)
	got := count(l, "cam-1", 10)
	if got < 3 || got > 5 {
		t.Fatalf("after ~4 tokens of refill, allowed %d, want 3..5", got)
	}
}

// Partial tokens must accumulate across calls. If the stored count were ever
// rounded to an integer, a bucket polled more often than it refills would store
// 0 each time and never refill at all.
func TestFractionalRefillIsNotTruncated(t *testing.T) {
	l := NewRedis(testRedis(t), Config{RPS: 4, Burst: 1}, prefix(), quiet)
	if !l.Allow(context.Background(), "cam-1") {
		t.Fatal("first request should be allowed")
	}
	// Two refills of ~0.5 token each must add up to one.
	time.Sleep(130 * time.Millisecond)
	l.Allow(context.Background(), "cam-1") // refused, but stores ~0.5
	time.Sleep(160 * time.Millisecond)
	if !l.Allow(context.Background(), "cam-1") {
		t.Fatal("two half-token refills were lost; fractional tokens are being truncated")
	}
}

func TestKeysAreSeparate(t *testing.T) {
	l := NewRedis(testRedis(t), Config{RPS: 0.001, Burst: 2}, prefix(), quiet)
	count(l, "cam-1", 5)
	if !l.Allow(context.Background(), "cam-2") {
		t.Fatal("cam-1 draining its bucket must not affect cam-2")
	}
}

// Expiry replaces the in-process sweep, and must only ever forget a bucket
// with no debt: the key lives until the bucket would be full again.
func TestKeyExpiresOnlyOnceTheDebtIsPaid(t *testing.T) {
	rdb := testRedis(t)
	p := prefix()
	l := NewRedis(rdb, Config{RPS: 1, Burst: 3}, p, quiet)
	count(l, "cam-1", 3)

	ttl, err := rdb.TTL(context.Background(), l.key("cam-1")).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl < 3*time.Second || ttl > l.FullAfter() {
		t.Fatalf("TTL %v, want between the refill time (3s) and %v", ttl, l.FullAfter())
	}
}

// Fails open: the limiter protects the service from one caller and must never
// be the reason every caller is refused.
func TestFailsOpenWhenRedisIsDown(t *testing.T) {
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond})
	defer dead.Close()
	l := NewRedis(dead, Config{RPS: 0.001, Burst: 1}, prefix(), quiet)

	start := time.Now()
	if got := count(l, "cam-1", 5); got != 5 {
		t.Fatalf("with Redis down allowed %d of 5, want all (fail open)", got)
	}
	// And quickly: a slow failure would add latency to every RPC.
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("5 calls against a dead Redis took %v", d)
	}
}

func TestZeroRateDisables(t *testing.T) {
	l := NewRedis(testRedis(t), Config{RPS: 0, Burst: 1}, prefix(), quiet)
	if l.Enabled() {
		t.Fatal("RPS 0 must disable")
	}
	if got := count(l, "cam-1", 50); got != 50 {
		t.Fatalf("disabled limiter allowed %d of 50", got)
	}
}

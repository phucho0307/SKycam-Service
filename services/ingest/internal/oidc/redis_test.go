package oidc

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// INGEST_TEST_REDIS_URL=redis://localhost:6379/13 go test ./internal/oidc/
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

func prefix() string { return "t-" + uuid.NewString()[:8] }

// The reason this type exists: login handled by one replica, callback by
// another.
func TestFlowStartedOnOneReplicaFinishesOnAnother(t *testing.T) {
	rdb, p, ctx := testRedis(t), prefix(), context.Background()
	podA := NewRedisFlows(rdb, time.Minute, p)
	podB := NewRedisFlows(rdb, time.Minute, p)

	want := Flow{Verifier: "verifier-123", Nonce: "nonce-456", ReturnTo: "https://dev.observatory.services"}
	if err := podA.Put(ctx, "state-1", want); err != nil {
		t.Fatal(err)
	}
	got, err := podB.Take(ctx, "state-1")
	if err != nil {
		t.Fatalf("pod B could not finish pod A's flow: %v", err)
	}
	if got.Verifier != want.Verifier || got.Nonce != want.Nonce || got.ReturnTo != want.ReturnTo {
		t.Fatalf("round trip changed the flow: got %+v", got)
	}

	// Control: the in-memory store genuinely fails this, which is the bug.
	memA, memB := NewPending(time.Minute), NewPending(time.Minute)
	_ = memA.Put(ctx, "state-1", want)
	if _, err := memB.Take(ctx, "state-1"); !errors.Is(err, ErrUnknownState) {
		t.Fatalf("control: in-memory pod B should not know pod A's state, got %v", err)
	}
}

func TestRedisStateIsSingleUse(t *testing.T) {
	s, ctx := NewRedisFlows(testRedis(t), time.Minute, prefix()), context.Background()
	_ = s.Put(ctx, "state-1", Flow{Verifier: "v"})
	if _, err := s.Take(ctx, "state-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Take(ctx, "state-1"); !errors.Is(err, ErrUnknownState) {
		t.Fatalf("second take: %v, want ErrUnknownState", err)
	}
}

// Many replayed callbacks racing on several pods: exactly one may win. This is
// what GETDEL buys over GET-then-DEL.
func TestConcurrentTakesOnlyOneWins(t *testing.T) {
	rdb, p, ctx := testRedis(t), prefix(), context.Background()
	_ = NewRedisFlows(rdb, time.Minute, p).Put(ctx, "state-race", Flow{Verifier: "v"})

	pods := []*RedisFlows{NewRedisFlows(rdb, time.Minute, p), NewRedisFlows(rdb, time.Minute, p), NewRedisFlows(rdb, time.Minute, p)}
	var wins atomic.Int64
	var wg sync.WaitGroup
	for i := range 60 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := pods[i%len(pods)].Take(ctx, "state-race"); err == nil {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if n := wins.Load(); n != 1 {
		t.Fatalf("%d callbacks completed one sign-in, want exactly 1", n)
	}
}

func TestRedisFlowExpires(t *testing.T) {
	s, ctx := NewRedisFlows(testRedis(t), time.Second, prefix()), context.Background()
	_ = s.Put(ctx, "old", Flow{Verifier: "v"})
	time.Sleep(1100 * time.Millisecond)

	_, expired := s.Take(ctx, "old")
	_, unknown := s.Take(ctx, "never-existed")
	if !errors.Is(expired, ErrUnknownState) || !errors.Is(unknown, ErrUnknownState) {
		t.Fatalf("expired=%v unknown=%v, want both ErrUnknownState", expired, unknown)
	}
}

func TestRedisPutRefusesToOverwrite(t *testing.T) {
	s, ctx := NewRedisFlows(testRedis(t), time.Minute, prefix()), context.Background()
	if err := s.Put(ctx, "dup", Flow{Verifier: "first"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "dup", Flow{Verifier: "second"}); err == nil {
		t.Fatal("a state collision must fail, not overwrite someone else's login")
	}
	f, _ := s.Take(ctx, "dup")
	if f.Verifier != "first" {
		t.Fatalf("stored flow was overwritten: %q", f.Verifier)
	}
}

// A store outage is reported as an error, not as ErrUnknownState, so the
// handler can answer 500 rather than tell the user to start again.
func TestRedisOutageIsNotUnknownState(t *testing.T) {
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 50 * time.Millisecond, MaxRetries: -1})
	defer dead.Close()
	s := NewRedisFlows(dead, time.Minute, prefix())
	_, err := s.Take(context.Background(), "state-1")
	if err == nil || errors.Is(err, ErrUnknownState) {
		t.Fatalf("outage gave %v; want a real error distinct from ErrUnknownState", err)
	}
}

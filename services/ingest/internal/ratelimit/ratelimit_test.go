package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// withClock returns a limiter whose time can be advanced, so the tests assert
// refill behaviour without sleeping.
func withClock(cfg Config) (*Limiter, func(time.Duration)) {
	l := New(cfg)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l.now = func() time.Time { return now }
	return l, func(d time.Duration) { now = now.Add(d) }
}

func allowN(l *Limiter, key string, n int) int {
	allowed := 0
	for i := 0; i < n; i++ {
		if l.Allow(context.Background(), key) {
			allowed++
		}
	}
	return allowed
}

func TestBurstIsAllowedThenTheRateBinds(t *testing.T) {
	l, _ := withClock(Config{RPS: 2, Burst: 10})

	// A recovering device spends its whole bucket at once. This is the case the
	// spool exists for, and it must not be throttled.
	if got := allowN(l, "cam-1", 10); got != 10 {
		t.Fatalf("burst of 10 allowed %d, want 10", got)
	}
	// The eleventh has nothing left.
	if l.Allow(context.Background(), "cam-1") {
		t.Fatal("request past the burst should be refused")
	}
}

func TestTokensRefillAtTheConfiguredRate(t *testing.T) {
	l, advance := withClock(Config{RPS: 2, Burst: 10})
	allowN(l, "cam-1", 10) // drain

	advance(time.Second) // 2 rps => 2 tokens
	if got := allowN(l, "cam-1", 5); got != 2 {
		t.Fatalf("after 1s at 2 rps, allowed %d, want 2", got)
	}

	advance(2 * time.Second) // 4 more
	if got := allowN(l, "cam-1", 10); got != 4 {
		t.Fatalf("after 2s at 2 rps, allowed %d, want 4", got)
	}
}

func TestRefillIsCappedAtBurst(t *testing.T) {
	l, advance := withClock(Config{RPS: 2, Burst: 10})
	allowN(l, "cam-1", 10)

	// An hour of idling must not accumulate an hour's worth of tokens, or a
	// device that sleeps all day gets an unlimited burst at dusk.
	advance(time.Hour)
	if got := allowN(l, "cam-1", 100); got != 10 {
		t.Fatalf("after idling, allowed %d, want the burst cap of 10", got)
	}
}

func TestKeysAreIndependent(t *testing.T) {
	l, _ := withClock(Config{RPS: 2, Burst: 5})

	if got := allowN(l, "cam-1", 5); got != 5 {
		t.Fatalf("cam-1 allowed %d, want 5", got)
	}
	if l.Allow(context.Background(), "cam-1") {
		t.Fatal("cam-1 should be drained")
	}
	// One camera exhausting its bucket must not affect any other. This is the
	// entire point: contain the blast radius of one broken device.
	if got := allowN(l, "cam-2", 5); got != 5 {
		t.Fatalf("cam-2 allowed %d, want 5 — one device affected another", got)
	}
}

// The argument for choosing a token bucket over a fixed window, as a test: a
// fixed window would allow 2x the limit across a boundary. This must not.
func TestNoDoubleRateAcrossAWindowBoundary(t *testing.T) {
	const rps, burst = 10, 10
	l, advance := withClock(Config{RPS: rps, Burst: burst})

	// Drain at the end of "minute one".
	allowN(l, "cam-1", burst)
	// Cross what a fixed-window implementation would treat as a reset.
	advance(100 * time.Millisecond)

	// A fixed window would hand back a whole fresh allowance here (10), giving
	// 20 requests in 100ms. A token bucket gives only what has refilled: 1.
	got := allowN(l, "cam-1", burst)
	if got > 2 {
		t.Fatalf("allowed %d immediately after draining; a boundary must not reset the bucket", got)
	}
}

func TestDisabledLimiterAllowsEverything(t *testing.T) {
	l, _ := withClock(Config{RPS: 0, Burst: 10})
	if l.Enabled() {
		t.Fatal("RPS 0 should disable the limiter")
	}
	if got := allowN(l, "cam-1", 1000); got != 1000 {
		t.Fatalf("disabled limiter allowed %d of 1000", got)
	}
	if l.Len() != 0 {
		t.Errorf("a disabled limiter should allocate nothing, got %d buckets", l.Len())
	}
}

func TestNilLimiterIsUsable(t *testing.T) {
	var l *Limiter
	if !l.Allow(context.Background(), "cam-1") {
		t.Fatal("a nil limiter must allow, so callers need no nil check")
	}
	if l.Enabled() || l.Len() != 0 {
		t.Error("a nil limiter should report disabled and empty")
	}
}

func TestIdleBucketsAreEvicted(t *testing.T) {
	l, advance := withClock(Config{RPS: 100, Burst: 10, IdleTTL: time.Minute})

	for i := 0; i < 50; i++ {
		l.Allow(context.Background(), fmt.Sprintf("cam-%d", i))
	}
	if l.Len() != 50 {
		t.Fatalf("expected 50 buckets, got %d", l.Len())
	}

	// Idle past the TTL, then one write triggers the sweep.
	advance(2 * time.Minute)
	l.Allow(context.Background(), "cam-new")
	if got := l.Len(); got != 1 {
		t.Fatalf("expected only the fresh bucket to survive, got %d", got)
	}
}

// A caller must not be able to clear its own debt by going quiet: if eviction
// forgot a draining bucket, waiting slightly less than the TTL would reset it.
func TestADrainingBucketIsNotEvicted(t *testing.T) {
	l, advance := withClock(Config{RPS: 0.001, Burst: 5, IdleTTL: time.Minute})
	allowN(l, "greedy", 5) // drained, and refills very slowly

	advance(2 * time.Minute)
	l.Allow(context.Background(), "someone-else") // triggers a sweep

	// The greedy bucket must still be empty, not forgotten and recreated full.
	if got := allowN(l, "greedy", 5); got != 0 {
		t.Fatalf("a drained bucket was reset by going idle: allowed %d", got)
	}
}

func TestConcurrentCallersGetExactlyTheBurst(t *testing.T) {
	const burst = 100
	l := New(Config{RPS: 0.0001, Burst: burst}) // refill negligible during the test

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Allow(context.Background(), "cam-1") {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// Exactly the burst, not more: the bucket must not be double-counted under
	// concurrency, and not fewer: no token may be lost to a race.
	if allowed != burst {
		t.Fatalf("500 concurrent callers got %d tokens, want exactly %d", allowed, burst)
	}
}

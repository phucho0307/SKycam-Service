// Package ratelimit caps how fast one caller may make requests.
//
// A token bucket, not a fixed window. The reason is specific to this system: a
// camera that has been offline drains its spool in a burst, and that burst is
// *legitimate* — it is the whole point of writing frames to disk before
// uploading. A token bucket expresses "0.6 req/s is normal, but let a recovering
// device spend 60 requests at once" as two independent knobs. A fixed window
// cannot say that, and it also lets a client send 2x the limit across a window
// boundary (100 at 11:59:59.9 plus 100 at 12:00:00.1 are both legal), which is
// exactly the runaway this exists to stop.
//
// This is backpressure's complement, not a replacement for it. The bounded
// command queue refuses when it is genuinely full; this refuses when a caller is
// taking more than its share whether or not anything is full.
package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type Config struct {
	// RPS is the sustained refill rate. Zero disables limiting entirely.
	RPS float64
	// Burst is the bucket capacity: how many requests a caller may make back to
	// back before the refill rate binds.
	Burst int
	// IdleTTL is how long an unused bucket is kept. Buckets are per caller, so
	// without eviction the map grows with every device id ever seen — including
	// ones that were deleted or renamed.
	IdleTTL time.Duration
}

// Limiter holds one token bucket per key.
type Limiter struct {
	cfg Config

	mu        sync.Mutex
	buckets   map[string]*bucket
	lastSweep time.Time

	// now is injectable so tests can advance time instead of sleeping.
	now func() time.Time
}

type bucket struct {
	lim      *rate.Limiter
	lastSeen time.Time
}

func New(cfg Config) *Limiter {
	if cfg.Burst <= 0 {
		cfg.Burst = 1
	}
	if cfg.IdleTTL <= 0 {
		cfg.IdleTTL = 30 * time.Minute
	}
	return &Limiter{
		cfg:     cfg,
		buckets: make(map[string]*bucket),
		now:     time.Now,
	}
}

// Enabled reports whether this limiter does anything.
func (l *Limiter) Enabled() bool { return l != nil && l.cfg.RPS > 0 }

// Allow takes one token for key, returning false if the bucket is empty.
//
// Non-blocking on purpose: a gRPC handler that waited for a token would hold a
// goroutine and burn the caller's deadline. Refusing immediately with
// ResourceExhausted lets the client back off, which it is already built to do.
func (l *Limiter) Allow(_ context.Context, key string) bool {
	if !l.Enabled() {
		return true
	}
	now := l.now()

	l.mu.Lock()
	l.maybeSweepLocked(now)
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{lim: rate.NewLimiter(rate.Limit(l.cfg.RPS), l.cfg.Burst)}
		l.buckets[key] = b
	}
	b.lastSeen = now
	lim := b.lim
	l.mu.Unlock()

	// AllowN takes the time explicitly, which is what makes this testable
	// without sleeping, and keeps the refill maths out of this package.
	return lim.AllowN(now, 1)
}

// Len is the number of live buckets, for tests and metrics.
func (l *Limiter) Len() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// maybeSweepLocked drops buckets nobody has used recently.
//
// Swept on write rather than from a background goroutine: the map only grows
// when a new key appears, so there is nothing to clean up when nothing is
// happening. Rate-limited to once per IdleTTL/2 so a busy server is not walking
// the map on every request.
func (l *Limiter) maybeSweepLocked(now time.Time) {
	if now.Sub(l.lastSweep) < l.cfg.IdleTTL/2 {
		return
	}
	l.lastSweep = now
	for k, b := range l.buckets {
		// A full bucket has no outstanding debt, so forgetting it changes
		// nothing. One that is still draining must be kept, or a caller could
		// reset its own limit by going quiet for slightly less than the TTL.
		if now.Sub(b.lastSeen) > l.cfg.IdleTTL && b.lim.TokensAt(now) >= float64(l.cfg.Burst) {
			delete(l.buckets, k)
		}
	}
}

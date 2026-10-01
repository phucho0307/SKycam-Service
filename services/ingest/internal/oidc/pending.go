package oidc

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrUnknownState means the callback presented a state we did not issue, have
// already consumed, or that expired. All three are treated identically: the
// caller learns only that the flow is not resumable.
var ErrUnknownState = errors.New("unknown or expired state")

// Flow is what we remember between the redirect out and the callback back.
type Flow struct {
	Verifier string
	Nonce    string
	// ReturnTo is where to send the browser afterwards. Validated when the flow
	// is created, never taken from the callback -- an open redirect here would
	// hand our freshly minted token to whatever host an attacker names.
	ReturnTo  string
	CreatedAt time.Time
}

// FlowStore remembers in-flight sign-ins between the redirect out and the
// callback back. Two implementations: Pending (one process) and RedisFlows
// (shared by every replica).
//
// Take must be atomic get-and-delete. That is what makes a state single-use,
// and so what stops a replayed callback completing a second sign-in.
type FlowStore interface {
	Put(ctx context.Context, state string, f Flow) error
	Take(ctx context.Context, state string) (Flow, error)
}

var (
	_ FlowStore = (*Pending)(nil)
	_ FlowStore = (*RedisFlows)(nil)
)

// Pending holds in-flight sign-ins in memory, keyed by state.
//
// Correct for exactly one replica. With several, a user starts the flow on one
// pod and the callback lands on another, which has never heard of the state, and
// the sign-in fails. Use RedisFlows there. It is *not* a cache that may silently
// drop entries -- an eviction here is a failed login.
type Pending struct {
	ttl time.Duration
	mu  sync.Mutex
	m   map[string]Flow
}

func NewPending(ttl time.Duration) *Pending {
	if ttl <= 0 {
		// Long enough for a slow human with a password manager and 2FA; short
		// enough that an abandoned flow is not resumable later.
		ttl = 10 * time.Minute
	}
	return &Pending{ttl: ttl, m: make(map[string]Flow)}
}

func (p *Pending) Put(_ context.Context, state string, f Flow) error {
	f.CreatedAt = time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sweepLocked()
	p.m[state] = f
	return nil
}

// Take returns the flow and removes it: a state is single-use, so a replayed
// callback cannot complete a second sign-in.
func (p *Pending) Take(_ context.Context, state string) (Flow, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, ok := p.m[state]
	if !ok {
		return Flow{}, ErrUnknownState
	}
	delete(p.m, state)
	if time.Since(f.CreatedAt) > p.ttl {
		return Flow{}, ErrUnknownState
	}
	return f, nil
}

func (p *Pending) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.m)
}

// sweepLocked drops expired entries. Called on write rather than from a
// background goroutine: abandoned flows are the only way this map grows, and
// they are bounded by login attempts.
func (p *Pending) sweepLocked() {
	cutoff := time.Now().Add(-p.ttl)
	for k, v := range p.m {
		if v.CreatedAt.Before(cutoff) {
			delete(p.m, k)
		}
	}
}

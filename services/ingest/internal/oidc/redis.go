package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisFlows keeps in-flight sign-ins in Redis, so a login that starts on one
// replica can finish on another.
//
// Why Redis and not sticky sessions: stickiness would have to survive the
// browser leaving for the identity provider and coming back, which means a
// cookie-based affinity rule in Traefik, and it still breaks when that pod
// restarts mid-login. A shared store makes every pod able to finish any flow.
//
// What is stored is the PKCE verifier and the nonce, for at most TTL. Both are
// useless once the flow completes or expires, and the key is the state -- 32
// random bytes -- so it cannot be enumerated.
type RedisFlows struct {
	rdb    *redis.Client
	ttl    time.Duration
	prefix string
}

func NewRedisFlows(rdb *redis.Client, ttl time.Duration, prefix string) *RedisFlows {
	if ttl <= 0 {
		ttl = 10 * time.Minute // same default, and same reasoning, as Pending
	}
	return &RedisFlows{rdb: rdb, ttl: ttl, prefix: prefix}
}

func (r *RedisFlows) key(state string) string { return fmt.Sprintf("%s:oidc:flow:%s", r.prefix, state) }

// Put records the flow with an expiry. Redis enforces the TTL, so there is no
// sweep and no way for abandoned flows to accumulate.
func (r *RedisFlows) Put(ctx context.Context, state string, f Flow) error {
	f.CreatedAt = time.Now()
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	// NX: a state is random, so a collision means something is wrong, and
	// silently overwriting another user's in-flight login is the worst outcome.
	ok, err := r.rdb.SetNX(ctx, r.key(state), b, r.ttl).Result()
	if err != nil {
		return fmt.Errorf("store sign-in flow: %w", err)
	}
	if !ok {
		return errors.New("sign-in state collision")
	}
	return nil
}

// Take is GETDEL: one atomic command, so two callbacks racing with the same
// state cannot both read it before either deletes it. A GET followed by a DEL
// would reopen exactly the replay this exists to prevent.
func (r *RedisFlows) Take(ctx context.Context, state string) (Flow, error) {
	if state == "" {
		return Flow{}, ErrUnknownState
	}
	b, err := r.rdb.GetDel(ctx, r.key(state)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Flow{}, ErrUnknownState
	}
	if err != nil {
		return Flow{}, fmt.Errorf("load sign-in flow: %w", err)
	}
	var f Flow
	if err := json.Unmarshal(b, &f); err != nil {
		return Flow{}, fmt.Errorf("decode sign-in flow: %w", err)
	}
	return f, nil
}

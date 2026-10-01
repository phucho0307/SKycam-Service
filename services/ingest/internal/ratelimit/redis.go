package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Allower is what the interceptor needs from a limiter. Two implementations:
// Limiter (one process) and RedisLimiter (shared by every replica).
type Allower interface {
	Allow(ctx context.Context, key string) bool
	Enabled() bool
}

var (
	_ Allower = (*Limiter)(nil)
	_ Allower = (*RedisLimiter)(nil)
)

// RedisLimiter is the same token bucket, held in Redis so that every replica
// draws from one bucket per caller.
//
// Why it exists: the in-process Limiter gives each replica its own buckets, so
// N replicas quietly allow N times the configured rate. That is not a
// throughput bug, it is a correctness one -- the configured number stops
// meaning anything the moment the Deployment scales.
//
// The whole read-refill-take-write runs as one Lua script, because Redis runs a
// script atomically. Done as separate GET and SET calls, two replicas could both
// read "1 token left" and both spend it.
type RedisLimiter struct {
	rdb    *redis.Client
	cfg    Config
	prefix string
	log    *slog.Logger

	// Rate-limits the "Redis is failing" log line itself. Without it an outage
	// logs once per request, and the log volume becomes a second outage.
	lastWarn atomic.Int64
}

// bucketScript is a token bucket stored as a hash {tokens, ts}.
//
// The clock is Redis's own (TIME), not the caller's. Replicas' clocks drift, and
// a replica whose clock ran ahead would refill every bucket it touched.
//
// Expiry replaces the in-process sweep: a bucket that has been left alone for
// burst/rate seconds has refilled completely, and a missing key is read as a
// full bucket. So an expired key is *exactly* equivalent to the one it replaced,
// which is the same "only forget a bucket with no debt" rule Limiter enforces by
// hand. A caller cannot clear its debt by going quiet, because the key does not
// expire until the debt is paid.
//
// Only integers come back out: Redis truncates a Lua number in a script's
// *return value*, so the script returns 1/0, never the token count. Numbers
// passed as *arguments* (the HSET below) are stored at full precision; Lua's
// own tostring() would be worse, keeping 14 significant digits of the timestamp.
const bucketScript = `
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local t     = redis.call('TIME')
local now   = tonumber(t[1]) + tonumber(t[2]) / 1000000

local v      = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(v[1])
local ts     = tonumber(v[2])
if tokens == nil then
  tokens = burst
  ts = now
end

local elapsed = now - ts
if elapsed < 0 then elapsed = 0 end
tokens = math.min(burst, tokens + elapsed * rate)

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', KEYS[1], math.ceil(burst / rate) + 1)
return allowed
`

var bucketLua = redis.NewScript(bucketScript)

// NewRedis returns a limiter sharing buckets through rdb. prefix separates
// audiences (devices from operators) and deployments sharing one Redis.
func NewRedis(rdb *redis.Client, cfg Config, prefix string, log *slog.Logger) *RedisLimiter {
	if cfg.Burst <= 0 {
		cfg.Burst = 1
	}
	return &RedisLimiter{rdb: rdb, cfg: cfg, prefix: prefix, log: log}
}

func (r *RedisLimiter) Enabled() bool { return r != nil && r.cfg.RPS > 0 }

// Allow takes one token for key.
//
// Fails OPEN: if Redis cannot answer, the request is allowed. The limiter
// protects the service from a misbehaving caller; it must not become the reason
// a healthy fleet cannot upload. A Redis outage therefore means "briefly
// unlimited", which is the state every deployment was in before this existed.
// Authentication is unaffected -- it never depended on Redis.
//
// The short timeout bounds what a slow Redis adds to every RPC. It is taken from
// the request context, so a caller that has already given up costs nothing.
func (r *RedisLimiter) Allow(ctx context.Context, key string) bool {
	if !r.Enabled() {
		return true
	}
	ctx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()

	// EvalSha first, falling back to EVAL on NOSCRIPT: the script body goes over
	// the wire once per Redis restart rather than once per request.
	n, err := bucketLua.Run(ctx, r.rdb, []string{r.key(key)},
		r.cfg.RPS, r.cfg.Burst).Int()
	if err != nil {
		r.warn(key, err)
		return true
	}
	return n == 1
}

func (r *RedisLimiter) key(k string) string { return fmt.Sprintf("%s:rl:%s", r.prefix, k) }

func (r *RedisLimiter) warn(key string, err error) {
	if errors.Is(err, context.Canceled) {
		return // the caller went away; not a Redis problem
	}
	now := time.Now().UnixNano()
	last := r.lastWarn.Load()
	if now-last < int64(10*time.Second) || !r.lastWarn.CompareAndSwap(last, now) {
		return
	}
	r.log.Warn("rate limiter unavailable; failing open", "key", key, "err", err)
}

// FullAfter is how long an untouched bucket takes to refill, and so how long
// its key lives. Exposed for tests and for sizing Redis memory.
func (r *RedisLimiter) FullAfter() time.Duration {
	if !r.Enabled() {
		return 0
	}
	return time.Duration(math.Ceil(float64(r.cfg.Burst)/r.cfg.RPS)+1) * time.Second
}

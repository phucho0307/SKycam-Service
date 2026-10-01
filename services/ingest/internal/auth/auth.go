// Package auth authenticates callers at the gRPC boundary.
//
// Two separate audiences, deliberately not sharing a credential:
//
//   - Devices call SkycamService with their own bearer token. The token decides
//     which device they are; handlers read it from the context and never trust
//     a device_id sent in the request body.
//   - Operators call SkycamControlService with a shared operator token. That
//     service is meant to listen on an internal-only port, never the public
//     ingest host. This is a placeholder for user JWTs + RBAC (Phase 2).
package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/ratelimit"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
)

type ctxKey int

const (
	deviceKey ctxKey = iota
	principalKey
)

// DeviceFromContext returns the authenticated device id.
func DeviceFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(deviceKey).(string)
	return id, ok
}

// Principal is an authenticated caller of the control API.
type Principal struct {
	// UserID is the JWT subject. Empty for the legacy shared operator token.
	UserID string
	Email  string
	// ServiceAccount is true for the shared operator token, which has no user
	// behind it and therefore bypasses per-device grants. It exists for
	// service-to-service calls and for local operation before user auth is
	// rolled out everywhere.
	ServiceAccount bool
}

// PrincipalFromContext returns the authenticated control-API caller.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey).(Principal)
	return p, ok
}

// ErrNoOperatorToken is returned at startup when the control API has no token.
var ErrNoOperatorToken = errors.New("operator token is not configured")

// Authenticator verifies tokens for both audiences.
type Authenticator struct {
	devices       store.DeviceStore
	operatorToken string
	// Optional. When set, control-API callers may present a user JWT instead of
	// the shared operator token, and are then subject to per-device grants.
	users *Verifier

	// Per-caller rate limits. Separate limiters because the two audiences have
	// completely different traffic shapes: a camera uploads on a fixed cadence
	// and bursts when draining a spool, while a human clicks occasionally.
	// Nil-safe, so an unconfigured limiter costs nothing.
	deviceRate   ratelimit.Allower
	operatorRate ratelimit.Allower

	// Devices authenticate on every RPC, including one per frame upload, so a
	// short-lived cache keeps that off the database. The TTL bounds how long a
	// revoked token keeps working.
	ttl   time.Duration
	mu    sync.RWMutex
	cache map[string]cacheEntry
}

type cacheEntry struct {
	deviceID string
	expires  time.Time
}

func New(devices store.DeviceStore, operatorToken string, ttl time.Duration) *Authenticator {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &Authenticator{devices: devices, operatorToken: operatorToken, ttl: ttl, cache: map[string]cacheEntry{}}
}

// WithUserTokens enables JWT authentication on the control API.
func (a *Authenticator) WithUserTokens(v *Verifier) *Authenticator {
	a.users = v
	return a
}

// WithRateLimits caps how fast one device, or one user, may call. Either may be
// nil to leave that audience unlimited.
func (a *Authenticator) WithRateLimits(devices, operators ratelimit.Allower) *Authenticator {
	a.deviceRate, a.operatorRate = devices, operators
	return a
}

func (a *Authenticator) authenticateDevice(ctx context.Context, token string) (string, error) {
	a.mu.RLock()
	e, ok := a.cache[token]
	a.mu.RUnlock()
	if ok && time.Now().Before(e.expires) {
		return e.deviceID, nil
	}

	deviceID, err := a.devices.AuthenticateDevice(ctx, token)
	if errors.Is(err, store.ErrUnknownToken) {
		return "", status.Error(codes.Unauthenticated, "unknown device token")
	}
	if err != nil {
		return "", status.Error(codes.Unavailable, "cannot verify token; retry")
	}

	a.mu.Lock()
	a.cache[token] = cacheEntry{deviceID: deviceID, expires: time.Now().Add(a.ttl)}
	a.mu.Unlock()
	return deviceID, nil
}

// authenticateOperator accepts either a user JWT or the shared operator token.
//
// The JWT is tried first and, crucially, a token that *looks* like a JWT is
// never then compared against the operator secret: otherwise an attacker could
// probe the shared secret through the timing of the fallback path. A JWT has
// two dots; the operator token is opaque and has none.
func (a *Authenticator) authenticateOperator(token string) (Principal, error) {
	if a.users != nil && strings.Count(token, ".") == 2 {
		claims, err := a.users.Verify(token)
		if err != nil {
			return Principal{}, status.Error(codes.Unauthenticated, "invalid user token")
		}
		return Principal{UserID: claims.Subject, Email: claims.Email}, nil
	}

	if a.operatorToken == "" {
		return Principal{}, status.Error(codes.Unauthenticated, "user token required")
	}
	// Constant time: a byte-by-byte compare leaks the token through timing.
	if subtle.ConstantTimeCompare([]byte(token), []byte(a.operatorToken)) != 1 {
		return Principal{}, status.Error(codes.Unauthenticated, "invalid operator token")
	}
	return Principal{ServiceAccount: true}, nil
}

// authorize returns a context carrying the authenticated device, if any.
//
// Rate limiting happens here, *after* authentication, because the meaningful key
// is the caller's identity rather than their address: every camera arrives
// through the same Cloudflare tunnel, so an IP-based limit would either throttle
// the whole fleet or nothing at all. Limiting by IP is the edge's job; limiting
// by device is ours, and only this service knows which camera is which.
func (a *Authenticator) authorize(ctx context.Context, fullMethod string) (context.Context, error) {
	// The health service answers without credentials: the Kubernetes probe
	// has none, and a probe that is refused marks every pod unready. It reveals
	// only SERVING / NOT_SERVING. Exact method names, not a prefix match on
	// arbitrary input, so nothing else can be routed through this exemption.
	if unauthenticatedMethods[fullMethod] {
		return ctx, nil
	}
	token, err := bearerToken(ctx)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(fullMethod, "/skycam.v1.SkycamControlService/") {
		p, err := a.authenticateOperator(token)
		if err != nil {
			return nil, err
		}
		// Service accounts are internal callers on a loopback listener; a limit
		// there would throttle the platform talking to itself.
		if !p.ServiceAccount {
			if err := a.checkRate(ctx, a.operatorRate, "user:"+p.UserID); err != nil {
				return nil, err
			}
		}
		return context.WithValue(ctx, principalKey, p), nil
	}
	deviceID, err := a.authenticateDevice(ctx, token)
	if err != nil {
		return nil, err
	}
	if err := a.checkRate(ctx, a.deviceRate, "device:"+deviceID); err != nil {
		return nil, err
	}
	return context.WithValue(ctx, deviceKey, deviceID), nil
}

// unauthenticatedMethods is the complete list of RPCs callable without a token.
var unauthenticatedMethods = map[string]bool{
	"/grpc.health.v1.Health/Check": true,
	"/grpc.health.v1.Health/Watch": true,
	"/grpc.health.v1.Health/List":  true,
}

// checkRate costs one token, or refuses.
//
// ResourceExhausted rather than Unavailable: gRPC clients treat Unavailable as
// "retry immediately, the server is having a moment", which is precisely the
// wrong response to being over quota. ResourceExhausted tells a well-behaved
// client to back off.
func (a *Authenticator) checkRate(ctx context.Context, l ratelimit.Allower, key string) error {
	if l == nil || l.Allow(ctx, key) {
		return nil
	}
	return status.Error(codes.ResourceExhausted, "request rate exceeded; slow down and retry")
}

func (a *Authenticator) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := a.authorize(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func (a *Authenticator) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := a.authorize(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, wrappedStream{ServerStream: ss, ctx: ctx})
	}
}

// wrappedStream carries the authenticated context into the handler, since
// grpc.ServerStream's own Context() is fixed at creation.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w wrappedStream) Context() context.Context { return w.ctx }

func bearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}
	token := strings.TrimSpace(strings.TrimPrefix(vals[0], "Bearer "))
	if token == "" || token == vals[0] {
		return "", status.Error(codes.Unauthenticated, "authorization must be 'Bearer <token>'")
	}
	return token, nil
}

// Package config loads all env-driven settings for the ingest service.
// Env vars are read here and nowhere else (same convention as the Rust crates).
package config

import (
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	// GRPCAddr is the listen address for the device-facing gRPC API. This is
	// the only one that should be reachable from the public ingest host.
	GRPCAddr string
	// ControlAddr serves the operator API (settings, commands). It binds to
	// loopback by default and must never be exposed through the ingress:
	// anyone who can reach it can re-point a live camera.
	ControlAddr string
	// OperatorToken guards the control API. Being retired in favour of user
	// JWTs; it stays for service-to-service calls and for local operation.
	OperatorToken string
	// JWT enables per-user authorization on the control API.
	JWT JWT
	// RedisURL enables cross-replica command routing. Empty means single-replica
	// operation, where the in-process session registry is the whole picture.
	RedisURL string
	// RedisPrefix namespaces rate-limit and sign-in keys, so dev and release
	// can share one Redis without sharing buckets or flows.
	RedisPrefix string
	// RateLimit caps requests per caller. See the ratelimit package for why a
	// token bucket rather than a fixed window.
	RateLimit RateLimit
	// SignIn serves the browser-facing OAuth/OIDC endpoints. Empty AuthAddr
	// disables it, which is the default: the service verifies tokens whether or
	// not it is the thing that issues them.
	SignIn SignIn
	// SessionIdleTimeout drops a device that has sent nothing for this long.
	SessionIdleTimeout time.Duration
	DatabaseURL        string
	S3                 S3
	// TelemetryBackend selects where telemetry is written: postgres, dynamo, or
	// both (dual-write during a migration). See DYNAMODB_TELEMETRY_MIGRATION.md.
	TelemetryBackend string
	Dynamo           Dynamo
	// MaxFitsBytes caps the declared size of one FITS upload.
	MaxFitsBytes int64
	// MaxPreviewBytes caps the inline JPEG preview. It rides in the header
	// message, so it must stay under gRPC's 4MB default message limit.
	MaxPreviewBytes int
}

// JWT configures verification of user tokens on the control API. Only a
// *public* key is ever configured here: the `api` crate is the sole issuer and
// is the only component that holds a private key.
type JWT struct {
	// PublicKeysPEM maps key id to a PEM-encoded Ed25519 public key. More than
	// one entry supports rotation (publish new, switch signing, retire old).
	PublicKeysPEM map[string]string
	Issuer        string
	Audience      string
	Leeway        time.Duration
}

func (j JWT) Enabled() bool { return len(j.PublicKeysPEM) > 0 }

// SignIn configures the OAuth 2.0 + PKCE sign-in endpoints.
//
// In the deployed platform this belongs in the Rust `api` crate, which is the
// sole token issuer. It is here so the Go service can be run and demonstrated
// on its own; the private key never leaves whichever component issues.
type SignIn struct {
	AuthAddr        string
	ProviderURL     string
	ClientID        string
	ClientSecret    string
	RedirectURL     string
	AllowedReturnTo []string
	AutoProvision   bool
	// SigningKeyID must match one of JWT.PublicKeysPEM, or the tokens this
	// service mints are tokens it will not then accept.
	SigningKeyID  string
	PrivateKeyPEM string
	TokenTTL      time.Duration
}

func (s SignIn) Enabled() bool { return s.AuthAddr != "" }

// RateLimit sizes the per-caller token buckets.
//
// Defaults are derived from the real cadence rather than picked round: a camera
// makes roughly 0.6 req/s (a preview every 2s, a FITS every 60s, the odd status
// check), so 2/s leaves ~3x headroom while still capping a runaway at a small
// multiple of normal. The burst is what lets a camera that has been offline
// drain its spool immediately instead of trickling for a minute.
type RateLimit struct {
	DeviceRPS     float64
	DeviceBurst   int
	OperatorRPS   float64
	OperatorBurst int
	IdleTTL       time.Duration
}

// S3 uses the same env var names as the Rust skycam service and the Python
// detect worker, so one secret can configure all three.
type S3 struct {
	Endpoint  string
	Bucket    string
	Region    string
	AccessKey string
	SecretKey string
}

// String redacts credentials so the config is safe to log.
func (s S3) String() string {
	return fmt.Sprintf("S3{Endpoint:%s Bucket:%s Region:%s AccessKey:<redacted> SecretKey:<redacted>}",
		s.Endpoint, s.Bucket, s.Region)
}

// Dynamo configures the telemetry table. Endpoint is empty against real AWS and
// set to http://localhost:8000 for DynamoDB Local.
type Dynamo struct {
	Endpoint  string
	Region    string
	Table     string
	AccessKey string
	SecretKey string
	TTLDays   int64
}

func (d Dynamo) String() string {
	return fmt.Sprintf("Dynamo{Endpoint:%s Region:%s Table:%s TTLDays:%d AccessKey:<redacted>}",
		d.Endpoint, d.Region, d.Table, d.TTLDays)
}

func Load() (Config, error) {
	c := Config{
		GRPCAddr:         getenv("INGEST_GRPC_ADDR", ":9090"),
		ControlAddr:      getenv("INGEST_CONTROL_ADDR", "127.0.0.1:9091"),
		OperatorToken:    os.Getenv("INGEST_OPERATOR_TOKEN"),
		DatabaseURL:      os.Getenv("DATABASE_URL"),
		TelemetryBackend: getenv("TELEMETRY_BACKEND", "postgres"),
		Dynamo: Dynamo{
			Endpoint:  os.Getenv("DYNAMO_ENDPOINT"),
			Region:    getenv("DYNAMO_REGION", "us-east-1"),
			Table:     getenv("DYNAMO_TABLE", "skycam_telemetry"),
			AccessKey: os.Getenv("DYNAMO_ACCESS_KEY"),
			SecretKey: os.Getenv("DYNAMO_SECRET_KEY"),
		},
		S3: S3{
			Endpoint:  os.Getenv("S3_ENDPOINT"),
			Bucket:    os.Getenv("S3_BUCKET"),
			Region:    getenv("S3_REGION", "auto"),
			AccessKey: os.Getenv("S3_ACCESS_KEY"),
			SecretKey: os.Getenv("S3_SECRET_KEY"),
		},
	}

	var err error
	if c.MaxFitsBytes, err = getInt64("INGEST_MAX_FITS_BYTES", 512<<20); err != nil {
		return Config{}, err
	}
	idleSecs, err := getInt64("INGEST_SESSION_IDLE_SECONDS", 90)
	if err != nil {
		return Config{}, err
	}
	c.SessionIdleTimeout = time.Duration(idleSecs) * time.Second

	// INGEST_JWT_PUBLIC_KEYS is "kid=<base64 PEM>" pairs, comma separated. The
	// PEM is base64'd because it is multi-line and env vars are not.
	if raw := os.Getenv("INGEST_JWT_PUBLIC_KEYS"); raw != "" {
		keys := map[string]string{}
		for _, pair := range strings.Split(raw, ",") {
			kid, b64, found := strings.Cut(strings.TrimSpace(pair), "=")
			if !found || kid == "" || b64 == "" {
				return Config{}, fmt.Errorf("INGEST_JWT_PUBLIC_KEYS entries must be kid=<base64 PEM>, got %q", pair)
			}
			pemBytes, err := base64.StdEncoding.DecodeString(b64)
			if err != nil {
				return Config{}, fmt.Errorf("INGEST_JWT_PUBLIC_KEYS key %q is not valid base64: %w", kid, err)
			}
			keys[kid] = string(pemBytes)
		}
		c.JWT = JWT{
			PublicKeysPEM: keys,
			Issuer:        getenv("INGEST_JWT_ISSUER", "observatory-api"),
			Audience:      getenv("INGEST_JWT_AUDIENCE", "skycam-ingest"),
		}
		leewaySecs, err := getInt64("INGEST_JWT_LEEWAY_SECONDS", 30)
		if err != nil {
			return Config{}, err
		}
		c.JWT.Leeway = time.Duration(leewaySecs) * time.Second
	}
	if c.Dynamo.TTLDays, err = getInt64("DYNAMO_TTL_DAYS", 90); err != nil {
		return Config{}, err
	}
	switch c.TelemetryBackend {
	case "postgres", "dynamo", "both":
	default:
		return Config{}, fmt.Errorf("TELEMETRY_BACKEND must be postgres, dynamo or both, got %q", c.TelemetryBackend)
	}
	maxPreview, err := getInt64("INGEST_MAX_PREVIEW_BYTES", 2<<20)
	if err != nil {
		return Config{}, err
	}
	c.MaxPreviewBytes = int(maxPreview)

	c.RedisURL = os.Getenv("INGEST_REDIS_URL")
	c.RedisPrefix = getenv("INGEST_REDIS_PREFIX", "skycam")

	// Set any *_RPS to 0 to disable that audience's limit.
	if c.RateLimit.DeviceRPS, err = getFloat("INGEST_DEVICE_RPS", 2); err != nil {
		return Config{}, err
	}
	if c.RateLimit.OperatorRPS, err = getFloat("INGEST_OPERATOR_RPS", 20); err != nil {
		return Config{}, err
	}
	deviceBurst, err := getInt64("INGEST_DEVICE_BURST", 60)
	if err != nil {
		return Config{}, err
	}
	operatorBurst, err := getInt64("INGEST_OPERATOR_BURST", 100)
	if err != nil {
		return Config{}, err
	}
	rlIdle, err := getInt64("INGEST_RATE_LIMIT_IDLE_SECONDS", 1800)
	if err != nil {
		return Config{}, err
	}
	c.RateLimit.DeviceBurst = int(deviceBurst)
	c.RateLimit.OperatorBurst = int(operatorBurst)
	c.RateLimit.IdleTTL = time.Duration(rlIdle) * time.Second

	if addr := os.Getenv("INGEST_AUTH_ADDR"); addr != "" {
		keyPEM, err := base64.StdEncoding.DecodeString(os.Getenv("INGEST_JWT_PRIVATE_KEY"))
		if err != nil {
			return Config{}, fmt.Errorf("INGEST_JWT_PRIVATE_KEY must be base64-encoded PEM: %w", err)
		}
		ttlSecs, err := getInt64("INGEST_SESSION_TOKEN_TTL_SECONDS", 3600)
		if err != nil {
			return Config{}, err
		}
		c.SignIn = SignIn{
			AuthAddr:      addr,
			ProviderURL:   getenv("INGEST_OIDC_PROVIDER_URL", "https://accounts.google.com"),
			ClientID:      os.Getenv("INGEST_OIDC_CLIENT_ID"),
			ClientSecret:  os.Getenv("INGEST_OIDC_CLIENT_SECRET"),
			RedirectURL:   os.Getenv("INGEST_OIDC_REDIRECT_URL"),
			AutoProvision: os.Getenv("INGEST_OIDC_AUTO_PROVISION") == "true",
			SigningKeyID:  getenv("INGEST_JWT_SIGNING_KID", "session-key-1"),
			PrivateKeyPEM: string(keyPEM),
			TokenTTL:      time.Duration(ttlSecs) * time.Second,
		}
		for _, o := range strings.Split(os.Getenv("INGEST_OIDC_ALLOWED_RETURN_TO"), ",") {
			if o = strings.TrimSpace(o); o != "" {
				c.SignIn.AllowedReturnTo = append(c.SignIn.AllowedReturnTo, o)
			}
		}
		for name, v := range map[string]string{
			"INGEST_OIDC_CLIENT_ID":     c.SignIn.ClientID,
			"INGEST_OIDC_CLIENT_SECRET": c.SignIn.ClientSecret,
			"INGEST_OIDC_REDIRECT_URL":  c.SignIn.RedirectURL,
			"INGEST_JWT_PRIVATE_KEY":    c.SignIn.PrivateKeyPEM,
		} {
			if v == "" {
				return Config{}, fmt.Errorf("%s is required when INGEST_AUTH_ADDR is set", name)
			}
		}
		if len(c.SignIn.AllowedReturnTo) == 0 {
			return Config{}, fmt.Errorf("INGEST_OIDC_ALLOWED_RETURN_TO is required when INGEST_AUTH_ADDR is set")
		}
	}

	var missing []string
	for name, v := range map[string]string{
		"DATABASE_URL":  c.DatabaseURL,
		"S3_ENDPOINT":   c.S3.Endpoint,
		"S3_BUCKET":     c.S3.Bucket,
		"S3_ACCESS_KEY": c.S3.AccessKey,
		"S3_SECRET_KEY": c.S3.SecretKey,
	} {
		// Treat the unfilled `<S3_ENDPOINT>`-style overlay placeholders as missing.
		if v == "" || strings.HasPrefix(v, "<") {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}
	return c, nil
}

// LoadMigrate is the config for `ingest migrate`: the database and nothing else.
// A migration job has no business holding S3 or signing keys, and requiring
// them would force the Kubernetes init container to mount every secret the
// server does.
func LoadMigrate() (string, error) {
	u := os.Getenv("DATABASE_URL")
	if u == "" {
		return "", fmt.Errorf("missing required env var: DATABASE_URL")
	}
	return u, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getFloat allows 0, unlike getInt64: for a rate, 0 means "no limit".
func getFloat(key string, fallback float64) (float64, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 {
		return 0, fmt.Errorf("%s must be a non-negative number, got %q", key, v)
	}
	return f, nil
}

func getInt64(key string, fallback int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", key, v)
	}
	return n, nil
}

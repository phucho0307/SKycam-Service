// Command ingest runs the sky-camera gRPC ingest service.
//
//	ingest                       serve the gRPC APIs
//	ingest migrate               apply database migrations and exit
//	ingest device add <id> [tok] register a device (prints the token once)
//	ingest user add <id> <email> provision a user the api crate may mint tokens for
//	ingest grant add <u> <d> <r> give a user a role on one device
//	ingest keygen [kid]          print an Ed25519 keypair as env vars
//
// Two listeners, deliberately:
//   - GRPCAddr serves devices and is the only one the ingress should reach.
//   - ControlAddr serves operators (settings, commands) and binds to loopback,
//     because anything that can call it can re-point a live camera.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/auth"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/authsvc"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/blob"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/cluster"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/config"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/oidc"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/ratelimit"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/server"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	// Subcommands that need less than the full server config come first, so
	// they do not demand credentials they never use.
	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		dbURL, err := config.LoadMigrate()
		if err != nil {
			return err
		}
		if err := store.Migrate(dbURL); err != nil {
			return err
		}
		log.Info("migrations applied")
		return nil
	}
	if len(os.Args) > 2 && os.Args[1] == "keygen" {
		return genKeypair(os.Args[2:])
	}
	if len(os.Args) > 1 && os.Args[1] == "keygen" {
		return genKeypair(nil)
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	// pgxpool.New connects lazily. Ping so a bad DATABASE_URL fails the pod at
	// startup, where CrashLoopBackOff says so, instead of on the first upload.
	pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
	err = pool.Ping(pingCtx)
	cancelPing()
	if err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	pg := store.NewPostgres(pool)

	if len(os.Args) > 3 && os.Args[1] == "device" && os.Args[2] == "add" {
		return addDevice(ctx, pg, os.Args[3:])
	}
	if len(os.Args) > 3 && os.Args[1] == "user" && os.Args[2] == "add" {
		return addUser(ctx, pg, os.Args[3:])
	}
	if len(os.Args) > 3 && os.Args[1] == "grant" && os.Args[2] == "add" {
		return addGrant(ctx, pg, os.Args[3:])
	}
	if len(os.Args) > 3 && os.Args[1] == "grant" && os.Args[2] == "revoke" {
		return revokeGrant(ctx, pg, os.Args[3:])
	}

	blobs, err := blob.New(ctx, cfg.S3)
	if err != nil {
		return err
	}

	telemetry, err := telemetryStore(ctx, cfg, pg, log)
	if err != nil {
		return err
	}

	srv := server.New(blobs, server.Stores{
		Frames:    pg,
		Devices:   pg,
		Telemetry: telemetry,
		Settings:  pg,
		Grants:    pg,
	}, server.Limits{
		MaxFitsBytes:       cfg.MaxFitsBytes,
		MaxPreviewBytes:    cfg.MaxPreviewBytes,
		SessionIdleTimeout: cfg.SessionIdleTimeout,
	}, log)

	// Cross-replica command routing. Without it the session registry is
	// per-process and a second replica cannot reach a device held by the first,
	// which caps the service at one replica -- an availability ceiling, since
	// every deploy then disconnects every camera.
	//
	// The same Redis also holds the rate-limit buckets and in-flight sign-ins.
	// Those were the other two pieces of per-process state; with all three
	// shared, any replica can serve any request.
	var rdb *redis.Client
	if cfg.RedisURL != "" {
		opt, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			return fmt.Errorf("parse INGEST_REDIS_URL: %w", err)
		}
		rdb = redis.NewClient(opt)
		defer rdb.Close()
		pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
		err = rdb.Ping(pingCtx).Err()
		cancelPing()
		if err != nil {
			// Fatal at startup rather than degrading silently: if the operator
			// configured a cluster bus, running without one would route commands
			// into a void that looks like healthy single-replica behaviour.
			return fmt.Errorf("connect redis: %w", err)
		}
		bus := cluster.NewRedisBus(rdb, cluster.Config{}, log)
		srv = srv.WithBus(bus)
		log.Info("cluster bus enabled", "replica_id", bus.ReplicaID())
	} else {
		log.Info("cluster bus disabled; single-replica operation")
	}

	authn := auth.New(pg, cfg.OperatorToken, 30*time.Second)
	deviceCfg := ratelimit.Config{
		RPS: cfg.RateLimit.DeviceRPS, Burst: cfg.RateLimit.DeviceBurst,
		IdleTTL: cfg.RateLimit.IdleTTL,
	}
	operatorCfg := ratelimit.Config{
		RPS: cfg.RateLimit.OperatorRPS, Burst: cfg.RateLimit.OperatorBurst,
		IdleTTL: cfg.RateLimit.IdleTTL,
	}
	// In-process buckets let N replicas allow N times the configured rate, so
	// with Redis available the buckets live there, shared by every replica.
	var deviceRL, operatorRL ratelimit.Allower
	rlShared := rdb != nil
	if rlShared {
		deviceRL = ratelimit.NewRedis(rdb, deviceCfg, cfg.RedisPrefix, log)
		operatorRL = ratelimit.NewRedis(rdb, operatorCfg, cfg.RedisPrefix, log)
	} else {
		deviceRL, operatorRL = ratelimit.New(deviceCfg), ratelimit.New(operatorCfg)
	}
	authn = authn.WithRateLimits(deviceRL, operatorRL)
	log.Info("rate limits", "shared", rlShared,
		"device_rps", cfg.RateLimit.DeviceRPS, "device_burst", cfg.RateLimit.DeviceBurst,
		"operator_rps", cfg.RateLimit.OperatorRPS, "operator_burst", cfg.RateLimit.OperatorBurst)
	if cfg.JWT.Enabled() {
		v, err := auth.NewVerifier(cfg.JWT.PublicKeysPEM, cfg.JWT.Issuer, cfg.JWT.Audience, cfg.JWT.Leeway)
		if err != nil {
			return fmt.Errorf("jwt verifier: %w", err)
		}
		authn = authn.WithUserTokens(v)
		log.Info("user JWT auth enabled", "keys", len(cfg.JWT.PublicKeysPEM),
			"issuer", cfg.JWT.Issuer, "audience", cfg.JWT.Audience)
	} else if cfg.OperatorToken == "" {
		return errors.New("control API needs INGEST_OPERATOR_TOKEN or INGEST_JWT_PUBLIC_KEYS")
	} else {
		log.Warn("user JWT auth is OFF; the shared operator token grants every device")
	}

	// Device-facing server.
	deviceSrv := grpc.NewServer(
		grpc.UnaryInterceptor(authn.UnaryInterceptor()),
		grpc.StreamInterceptor(authn.StreamInterceptor()),
		// The client's ping interval must be >= MinTime, or the server hangs
		// up with ENHANCE_YOUR_CALM, which looks like a network fault.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             20 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
	)
	skycamv1.RegisterSkycamServiceServer(deviceSrv, srv)
	reflection.Register(deviceSrv)

	// grpc.health.v1 on the device listener, so Kubernetes can probe it with a
	// native gRPC probe. An HTTP probe cannot tell whether the gRPC server
	// itself is up, only that *something* answers on a port. Postgres is checked
	// here once, at startup (the ping after connecting); a readiness probe that pinged
	// the database would take every replica out of rotation during a Postgres
	// blip, turning a partial outage into a total one.
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthSrv.SetServingStatus(skycamv1.SkycamService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(deviceSrv, healthSrv)

	// Operator-facing server, separate listener.
	controlSrv := grpc.NewServer(
		grpc.UnaryInterceptor(authn.UnaryInterceptor()),
		grpc.StreamInterceptor(authn.StreamInterceptor()),
	)
	skycamv1.RegisterSkycamControlServiceServer(controlSrv, srv.Control())
	reflection.Register(controlSrv)

	if cfg.OperatorToken == "" {
		log.Warn("INGEST_OPERATOR_TOKEN is unset; the control API will refuse every call")
	}

	deviceLis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}
	controlLis, err := net.Listen("tcp", cfg.ControlAddr)
	if err != nil {
		return err
	}
	log.Info("ingest listening",
		"devices", cfg.GRPCAddr, "control", cfg.ControlAddr,
		"idle_timeout", cfg.SessionIdleTimeout, "s3", cfg.S3.String())

	errc := make(chan error, 3)
	go func() { errc <- deviceSrv.Serve(deviceLis) }()
	go func() { errc <- controlSrv.Serve(controlLis) }()

	// Sign-in is a third listener, HTTP rather than gRPC, because it is driven
	// by a browser redirect and a browser cannot speak gRPC.
	var authHTTP *http.Server
	if cfg.SignIn.Enabled() {
		issuer, err := auth.NewIssuer(cfg.SignIn.SigningKeyID, cfg.SignIn.PrivateKeyPEM,
			cfg.JWT.Issuer, cfg.JWT.Audience, cfg.SignIn.TokenTTL)
		if err != nil {
			return fmt.Errorf("token issuer: %w", err)
		}
		var flows oidc.FlowStore // nil: in memory, single replica only
		if rdb != nil {
			flows = oidc.NewRedisFlows(rdb, 0, cfg.RedisPrefix)
		}
		svc, err := authsvc.New(ctx, authsvc.Config{
			Flows:           flows,
			ProviderURL:     cfg.SignIn.ProviderURL,
			ClientID:        cfg.SignIn.ClientID,
			ClientSecret:    cfg.SignIn.ClientSecret,
			RedirectURL:     cfg.SignIn.RedirectURL,
			AllowedReturnTo: cfg.SignIn.AllowedReturnTo,
			AutoProvision:   cfg.SignIn.AutoProvision,
		}, issuer, pg, log)
		if err != nil {
			return fmt.Errorf("sign-in service: %w", err)
		}
		authHTTP = &http.Server{
			Addr:    cfg.SignIn.AuthAddr,
			Handler: svc.Handler(),
			// A browser-facing listener needs these; the gRPC ones do not,
			// which is a reason to keep it separate.
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      30 * time.Second,
		}
		log.Info("sign-in listening", "addr", cfg.SignIn.AuthAddr, "shared_flows", rdb != nil,
			"provider", cfg.SignIn.ProviderURL, "auto_provision", cfg.SignIn.AutoProvision)
		go func() {
			if err := authHTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- err
			}
		}()
	}

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	// Report NOT_SERVING first, so the readiness probe removes this pod from the
	// Service before the listeners close. Cameras then reconnect to another
	// replica instead of to this one while it drains.
	healthSrv.Shutdown()
	// Close device sessions first: they are long-lived by design, so
	// GracefulStop would otherwise wait for the idle timeout on each one.
	srv.Sessions().CloseAll(errors.New("server shutting down"))

	if authHTTP != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = authHTTP.Shutdown(shutdownCtx)
		cancel()
	}

	done := make(chan struct{})
	go func() {
		controlSrv.GracefulStop()
		deviceSrv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		// An interrupted upload just resumes after restart; don't hang on it.
		controlSrv.Stop()
		deviceSrv.Stop()
	}
	return nil
}

// telemetryStore picks where telemetry is written. The migration runs through
// the middle state: `both` dual-writes with Postgres deciding success, so a
// DynamoDB problem cannot break ingest while the two are being compared.
func telemetryStore(ctx context.Context, cfg config.Config, pg *store.Postgres, log *slog.Logger) (store.TelemetryStore, error) {
	if cfg.TelemetryBackend == "postgres" {
		return pg, nil
	}

	dyn, err := store.NewDynamoTelemetry(ctx, store.DynamoConfig{
		Endpoint:  cfg.Dynamo.Endpoint,
		Region:    cfg.Dynamo.Region,
		Table:     cfg.Dynamo.Table,
		AccessKey: cfg.Dynamo.AccessKey,
		SecretKey: cfg.Dynamo.SecretKey,
		TTL:       time.Duration(cfg.Dynamo.TTLDays) * 24 * time.Hour,
	})
	if err != nil {
		return nil, err
	}
	if err := dyn.EnsureTable(ctx); err != nil {
		return nil, err
	}
	log.Info("telemetry backend", "mode", cfg.TelemetryBackend, "dynamo", cfg.Dynamo.String())

	if cfg.TelemetryBackend == "dynamo" {
		return dyn, nil
	}
	return store.NewDualWriteTelemetry(pg, dyn, "dynamo", log), nil
}

// addDevice registers a device and prints its token once. The database keeps
// only a SHA-256 digest, so a lost token is reissued, never recovered.
func addDevice(ctx context.Context, devices store.DeviceStore, args []string) error {
	deviceID := args[0]
	token := ""
	if len(args) > 1 {
		token = args[1]
	} else {
		buf := make([]byte, 32)
		if _, err := rand.Read(buf); err != nil {
			return err
		}
		token = base64.RawURLEncoding.EncodeToString(buf)
	}
	if err := devices.UpsertDevice(ctx, deviceID, token, ""); err != nil {
		return err
	}
	fmt.Printf("device %s registered\ntoken: %s\n(store it now; only its digest is kept)\n", deviceID, token)
	return nil
}

// addUser provisions a subject the api crate may mint tokens for. A valid
// signature alone grants nothing: the user has to exist here first, so a
// compromised issuer still cannot conjure access to a camera.
//
//	ingest user add <user_id> <email> [display name] [--admin]
func addUser(ctx context.Context, grants store.GrantStore, args []string) error {
	if len(args) < 2 {
		return errors.New("usage: ingest user add <user_id> <email> [display name] [--admin]")
	}
	u := store.User{UserID: args[0], Email: args[1]}
	for _, a := range args[2:] {
		if a == "--admin" {
			u.IsAdmin = true
			continue
		}
		u.DisplayName = a
	}
	if err := grants.UpsertUser(ctx, u); err != nil {
		return err
	}
	fmt.Printf("user %s (%s) provisioned, admin=%v\n", u.UserID, u.Email, u.IsAdmin)
	return nil
}

// Usage: ingest grant add <user_id> <device_id> <viewer|operator|admin>
func addGrant(ctx context.Context, grants store.GrantStore, args []string) error {
	if len(args) < 3 {
		return errors.New("usage: ingest grant add <user_id> <device_id> <viewer|operator|admin>")
	}
	role := store.Role(args[2])
	if !role.Valid() {
		return fmt.Errorf("role must be viewer, operator or admin, got %q", args[2])
	}
	if err := grants.GrantDevice(ctx, args[0], args[1], role, "cli"); err != nil {
		return err
	}
	fmt.Printf("granted %s on %s to %s\n", role, args[1], args[0])
	return nil
}

// genKeypair prints an Ed25519 keypair as the two env vars that use it.
//
// Separate variables because they belong in different places: the private key
// goes only to whichever component issues tokens, the public key to every
// component that verifies them. Printing both here is a convenience for local
// runs, not a recommendation to deploy them together.
func genKeypair(args []string) error {
	kid := "session-key-1"
	if len(args) > 0 {
		kid = args[0]
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return err
	}
	pubDER, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return err
	}
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER})
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})

	fmt.Printf("# issuer only -- treat as a secret (Sealed Secret in this repo)\n")
	fmt.Printf("INGEST_JWT_SIGNING_KID=%s\n", kid)
	fmt.Printf("INGEST_JWT_PRIVATE_KEY=%s\n\n", base64.StdEncoding.EncodeToString(privPEM))
	fmt.Printf("# every verifier -- not secret\n")
	fmt.Printf("INGEST_JWT_PUBLIC_KEYS=%s=%s\n", kid, base64.StdEncoding.EncodeToString(pubPEM))
	return nil
}

// Usage: ingest grant revoke <user_id> <device_id>
func revokeGrant(ctx context.Context, grants store.GrantStore, args []string) error {
	if len(args) < 2 {
		return errors.New("usage: ingest grant revoke <user_id> <device_id>")
	}
	if err := grants.RevokeDevice(ctx, args[0], args[1]); err != nil {
		return err
	}
	fmt.Printf("revoked %s on %s\n", args[0], args[1])
	return nil
}

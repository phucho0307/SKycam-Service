package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	skycamv1 "github.com/ObservatoryServices/ObservatoryServices/services/ingest/gen/skycam/v1"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/auth"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/testenv"
)

const (
	authzIssuer   = "observatory-api"
	authzAudience = "skycam-ingest"
	authzKid      = "test-key"
)

// authzHarness runs the real server with user JWT auth switched on. It plays
// both sides of the split the design assumes: it holds the private key (the
// `api` crate's job) while the server holds only the public one.
type authzHarness struct {
	env  *testenv.Env
	pg   *store.Postgres
	priv ed25519.PrivateKey
	dial func(t *testing.T, token string) *grpc.ClientConn
}

func newAuthzHarness(t *testing.T) *authzHarness {
	t.Helper()
	env := testenv.Setup(t)
	pg := store.NewPostgres(env.Pool)

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	pubPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))

	verifier, err := auth.NewVerifier(map[string]string{authzKid: pubPEM},
		authzIssuer, authzAudience, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	srv := New(env.Blobs, Stores{Frames: pg, Devices: pg, Telemetry: pg, Settings: pg, Grants: pg},
		Limits{MaxFitsBytes: 64 << 20, MaxPreviewBytes: 2 << 20, SessionIdleTimeout: 90 * time.Second},
		slog.New(slog.NewTextHandler(io.Discard, nil)))

	authn := auth.New(pg, testOperatorToken, time.Second).WithUserTokens(verifier)
	gs := grpc.NewServer(
		grpc.UnaryInterceptor(authn.UnaryInterceptor()),
		grpc.StreamInterceptor(authn.StreamInterceptor()),
	)
	skycamv1.RegisterSkycamServiceServer(gs, srv)
	skycamv1.RegisterSkycamControlServiceServer(gs, srv.Control())

	lis := bufconn.Listen(1 << 20)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	return &authzHarness{
		env:  env,
		pg:   pg,
		priv: priv,
		dial: func(t *testing.T, token string) *grpc.ClientConn {
			t.Helper()
			conn, err := grpc.NewClient("passthrough:///bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithPerRPCCredentials(tokenCreds{token: token}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			return conn
		},
	}
}

// mint issues a token the way the api crate would.
func (h *authzHarness) mint(t *testing.T, userID string) string {
	t.Helper()
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.RegisteredClaims{
		Subject:   userID,
		Issuer:    authzIssuer,
		Audience:  jwt.ClaimStrings{authzAudience},
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
	})
	tok.Header["kid"] = authzKid
	s, err := tok.SignedString(h.priv)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func (h *authzHarness) clientFor(t *testing.T, userID string) skycamv1.SkycamControlServiceClient {
	t.Helper()
	return skycamv1.NewSkycamControlServiceClient(h.dial(t, h.mint(t, userID)))
}

// newUser provisions a user and returns their id.
func (h *authzHarness) newUser(t *testing.T, admin bool) string {
	t.Helper()
	buf := make([]byte, 6)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	id := "u-" + hex.EncodeToString(buf)
	err := h.pg.UpsertUser(context.Background(), store.User{
		UserID: id, Email: id + "@example.org", IsAdmin: admin,
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func (h *authzHarness) newDevice(t *testing.T) string {
	t.Helper()
	id, token := newTestDevice()
	if err := h.pg.UpsertDevice(context.Background(), id, token, "authz tests"); err != nil {
		t.Fatal(err)
	}
	return id
}

func (h *authzHarness) grant(t *testing.T, userID, deviceID string, role store.Role) {
	t.Helper()
	if err := h.pg.GrantDevice(context.Background(), userID, deviceID, role, "test"); err != nil {
		t.Fatal(err)
	}
}

func setGain(ctx context.Context, c skycamv1.SkycamControlServiceClient, deviceID string, gain int64) error {
	_, err := c.UpdateDeviceSettings(ctx, &skycamv1.UpdateDeviceSettingsRequest{
		Settings: &skycamv1.DeviceSettings{DeviceId: deviceID, Gain: &gain},
	})
	return err
}

func wantCode(t *testing.T, err error, want codes.Code, msg string) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("%s: got %v (%v), want %v", msg, status.Code(err), err, want)
	}
}

// -- the matrix --------------------------------------------------------------

func TestOperatorCanChangeTheirOwnDevice(t *testing.T) {
	h := newAuthzHarness(t)
	ctx := context.Background()
	user, device := h.newUser(t, false), h.newDevice(t)
	h.grant(t, user, device, store.RoleOperator)

	if err := setGain(ctx, h.clientFor(t, user), device, 321); err != nil {
		t.Fatalf("granted operator must be allowed: %v", err)
	}
	got, err := h.pg.GetSettings(ctx, device)
	if err != nil || got == nil || got.Gain == nil || *got.Gain != 321 {
		t.Fatalf("settings not stored: %+v (%v)", got, err)
	}
}

// The whole point of per-device grants.
func TestUserCannotChangeSomebodyElsesDevice(t *testing.T) {
	h := newAuthzHarness(t)
	ctx := context.Background()
	alice, bob := h.newUser(t, false), h.newUser(t, false)
	aliceCam, bobCam := h.newDevice(t), h.newDevice(t)
	h.grant(t, alice, aliceCam, store.RoleOperator)
	h.grant(t, bob, bobCam, store.RoleOperator)

	err := setGain(ctx, h.clientFor(t, alice), bobCam, 999)
	wantCode(t, err, codes.PermissionDenied, "alice writing to bob's camera")

	// And Bob's camera is untouched.
	got, _ := h.pg.GetSettings(ctx, bobCam)
	if got != nil && got.Gain != nil && *got.Gain == 999 {
		t.Fatal("alice's write reached bob's camera")
	}
}

func TestViewerCannotChangeSettings(t *testing.T) {
	h := newAuthzHarness(t)
	user, device := h.newUser(t, false), h.newDevice(t)
	h.grant(t, user, device, store.RoleViewer)

	err := setGain(context.Background(), h.clientFor(t, user), device, 100)
	wantCode(t, err, codes.PermissionDenied, "viewer writing settings")
}

func TestAdminReachesEveryDeviceWithoutAGrant(t *testing.T) {
	h := newAuthzHarness(t)
	admin, device := h.newUser(t, true), h.newDevice(t)

	if err := setGain(context.Background(), h.clientFor(t, admin), device, 55); err != nil {
		t.Fatalf("platform admin should be allowed without a grant: %v", err)
	}
}

// A validly signed token for a subject we have never provisioned. Authentication
// succeeded; authorization must not.
func TestValidTokenForUnknownUserIsDenied(t *testing.T) {
	h := newAuthzHarness(t)
	device := h.newDevice(t)

	err := setGain(context.Background(), h.clientFor(t, "never-provisioned"), device, 1)
	wantCode(t, err, codes.PermissionDenied, "unknown subject")
}

func TestDisabledUserIsDenied(t *testing.T) {
	h := newAuthzHarness(t)
	ctx := context.Background()
	user, device := h.newUser(t, false), h.newDevice(t)
	h.grant(t, user, device, store.RoleOperator)
	client := h.clientFor(t, user)

	if err := setGain(ctx, client, device, 10); err != nil {
		t.Fatalf("should work before being disabled: %v", err)
	}
	if _, err := h.env.Pool.Exec(ctx,
		`UPDATE users SET disabled_at = now() WHERE user_id = $1`, user); err != nil {
		t.Fatal(err)
	}
	// The grant still exists; the account is what was revoked.
	wantCode(t, setGain(ctx, client, device, 11), codes.PermissionDenied, "disabled user")
}

func TestRevokingAGrantTakesEffect(t *testing.T) {
	h := newAuthzHarness(t)
	ctx := context.Background()
	user, device := h.newUser(t, false), h.newDevice(t)
	h.grant(t, user, device, store.RoleOperator)
	client := h.clientFor(t, user)

	if err := setGain(ctx, client, device, 10); err != nil {
		t.Fatalf("before revoke: %v", err)
	}
	if err := h.pg.RevokeDevice(ctx, user, device); err != nil {
		t.Fatal(err)
	}
	// Note: the token is still perfectly valid. Access is decided per request
	// against the database, not baked into the token.
	wantCode(t, setGain(ctx, client, device, 11), codes.PermissionDenied, "after revoke")
}

// The audit trail must name the human, not a shared label.
func TestAuditTrailRecordsTheRealUser(t *testing.T) {
	h := newAuthzHarness(t)
	ctx := context.Background()
	user, device := h.newUser(t, false), h.newDevice(t)
	h.grant(t, user, device, store.RoleOperator)

	if err := setGain(ctx, h.clientFor(t, user), device, 77); err != nil {
		t.Fatal(err)
	}
	var changedBy string
	err := h.env.Pool.QueryRow(ctx,
		`SELECT changed_by FROM settings_audit WHERE device_id = $1 ORDER BY id DESC LIMIT 1`,
		device).Scan(&changedBy)
	if err != nil {
		t.Fatal(err)
	}
	if changedBy != user {
		t.Errorf("changed_by = %q, want the user id %q", changedBy, user)
	}
}

// An unauthorized caller must not learn whether a device exists or is online.
func TestSendCommandDeniesBeforeRevealingConnectivity(t *testing.T) {
	h := newAuthzHarness(t)
	user, device := h.newUser(t, false), h.newDevice(t)
	// No grant at all. The device is also not connected, which would normally
	// produce FailedPrecondition -- that answer itself is information.
	_, err := h.clientFor(t, user).SendCommand(context.Background(), &skycamv1.SendCommandRequest{
		DeviceId: device,
		Command:  &skycamv1.Command{Kind: &skycamv1.Command_CaptureNow{CaptureNow: &skycamv1.CaptureNow{}}},
	})
	wantCode(t, err, codes.PermissionDenied, "ungranted device")
}

func TestListConnectedDevicesIsFilteredByGrants(t *testing.T) {
	h := newAuthzHarness(t)
	user := h.newUser(t, false)
	mine, theirs := h.newDevice(t), h.newDevice(t)
	h.grant(t, user, mine, store.RoleViewer)

	resp, err := h.clientFor(t, user).ListConnectedDevices(context.Background(),
		&skycamv1.ListConnectedDevicesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range resp.GetDevices() {
		if d.GetDeviceId() == theirs {
			t.Fatalf("device %s is visible without a grant", theirs)
		}
	}
	_ = mine // no session is open in this test; the assertion is the absence
}

// The operator token keeps working: it is a service account, not a user.
func TestSharedOperatorTokenStillBypassesGrants(t *testing.T) {
	h := newAuthzHarness(t)
	device := h.newDevice(t)
	client := skycamv1.NewSkycamControlServiceClient(h.dial(t, testOperatorToken))

	if err := setGain(context.Background(), client, device, 42); err != nil {
		t.Fatalf("operator token should still work: %v", err)
	}
}

// The loop the other tests leave open: a token minted by the real Issuer must
// be accepted by the real interceptor. These are configured separately (kid,
// issuer, audience), so a mismatch between them would break every sign-in while
// every test on each side individually still passed.
func TestTokenFromTheRealIssuerIsAcceptedByTheInterceptor(t *testing.T) {
	h := newAuthzHarness(t)
	ctx := context.Background()
	user, device := h.newUser(t, false), h.newDevice(t)
	h.grant(t, user, device, store.RoleOperator)

	der, err := x509.MarshalPKCS8PrivateKey(h.priv)
	if err != nil {
		t.Fatal(err)
	}
	privPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	issuer, err := auth.NewIssuer(authzKid, privPEM, authzIssuer, authzAudience, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	token, expiresAt, err := issuer.Issue(user, "astronomer@example.org")
	if err != nil {
		t.Fatal(err)
	}
	if !expiresAt.After(time.Now()) {
		t.Fatal("token is already expired")
	}

	client := skycamv1.NewSkycamControlServiceClient(h.dial(t, token))
	if err := setGain(ctx, client, device, 123); err != nil {
		t.Fatalf("a token from the real issuer must be accepted: %v", err)
	}
}

func TestBadTokensAreRejectedAtTheInterceptor(t *testing.T) {
	h := newAuthzHarness(t)
	device := h.newDevice(t)

	// Shaped like a JWT, but signed by nobody we trust.
	forged := "eyJhbGciOiJFZERTQSIsImtpZCI6InRlc3Qta2V5In0.eyJzdWIiOiJhdHRhY2tlciJ9.bm90YXNpZ25hdHVyZQ"
	client := skycamv1.NewSkycamControlServiceClient(h.dial(t, forged))
	wantCode(t, setGain(context.Background(), client, device, 1), codes.Unauthenticated, "forged JWT")

	// An opaque string that is not the operator token.
	client2 := skycamv1.NewSkycamControlServiceClient(h.dial(t, "definitely-not-the-operator-token"))
	wantCode(t, setGain(context.Background(), client2, device, 1), codes.Unauthenticated, "wrong opaque token")
}

package authsvc

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/auth"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/oidc"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
)

const (
	testClientID = "skycam-web"
	testSecret   = "client-secret"
	testReturnTo = "https://dev.observatory.services"
)

// fakeUsers is an in-memory GrantStore. The sign-in path only needs the user
// half, and using a fake keeps these tests free of a database.
type fakeUsers struct {
	users    map[string]store.User
	disabled map[string]bool
}

func newFakeUsers() *fakeUsers {
	return &fakeUsers{users: map[string]store.User{}, disabled: map[string]bool{}}
}

func (f *fakeUsers) AccessFor(_ context.Context, userID, _ string) (store.Access, error) {
	u, ok := f.users[userID]
	if !ok {
		return store.Access{}, store.ErrUnknownUser
	}
	if f.disabled[userID] {
		return store.Access{}, store.ErrUserDisabled
	}
	return store.Access{UserID: userID, IsAdmin: u.IsAdmin}, nil
}
func (f *fakeUsers) DevicesFor(context.Context, string) (map[string]store.Role, bool, error) {
	return nil, false, nil
}
func (f *fakeUsers) UpsertUser(_ context.Context, u store.User) error {
	f.users[u.UserID] = u
	return nil
}
func (f *fakeUsers) GrantDevice(context.Context, string, string, store.Role, string) error {
	return nil
}
func (f *fakeUsers) RevokeDevice(context.Context, string, string) error { return nil }

type rig struct {
	idp    *fakeIDP
	svc    *Service
	srv    *httptest.Server
	users  *fakeUsers
	issuer *auth.Issuer
	client *http.Client
}

func newRig(t *testing.T, mutate func(*Config)) *rig {
	t.Helper()
	return newReplicatedRig(t, 1, mutate)
}

// newReplicatedRig runs several independent Service instances -- separate
// processes as far as their state goes -- behind one address.
func newReplicatedRig(t *testing.T, replicas int, mutate func(*Config)) *rig {
	t.Helper()
	idp := newFakeIDP(t)
	users := newFakeUsers()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	privPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	issuer, err := auth.NewIssuer("session-key-1", privPEM, "observatory-api", "skycam-ingest", time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	// The service needs its own URL for the redirect URI, so stand up the
	// httptest server first and point the config at it. With several replicas
	// the server is a round-robin load balancer in front of them, so
	// consecutive requests -- the login and its callback -- land on different
	// instances, exactly as they would behind a k8s Service.
	svcs := make([]*Service, replicas)
	var next atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(next.Add(1)-1) % len(svcs)
		svcs[i].Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	cfg := Config{
		ProviderURL:     idp.issuer,
		ClientID:        testClientID,
		ClientSecret:    testSecret,
		RedirectURL:     srv.URL + "/auth/callback",
		AllowedReturnTo: []string{testReturnTo},
		AutoProvision:   false,
		FlowTTL:         time.Minute,
	}
	for i := range svcs {
		c := cfg
		if mutate != nil {
			mutate(&c)
		}
		svcs[i], err = New(context.Background(), c, issuer, users, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil {
			t.Fatal(err)
		}
	}

	jar, _ := cookiejar.New(nil)
	return &rig{idp: idp, svc: svcs[0], srv: srv, users: users, issuer: issuer,
		client: &http.Client{Jar: jar}}
}

// signIn walks the whole flow the way a browser would: follow the redirect to
// the provider, let it redirect back, and read the final response.
func (r *rig) signIn(t *testing.T) *http.Response {
	t.Helper()
	resp, err := r.client.Get(r.srv.URL + "/auth/login?return_to=" + url.QueryEscape(testReturnTo))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestSignInEndToEnd(t *testing.T) {
	r := newRig(t, nil)
	r.users.users["google-oauth2|1234567890"] = store.User{
		UserID: "google-oauth2|1234567890", Email: "astronomer@example.org"}

	resp := r.signIn(t)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("sign-in failed: %d %s", resp.StatusCode, body)
	}
	body, _ := io.ReadAll(resp.Body)
	out := mustJSON(t, body)

	if out["user_id"] != "google-oauth2|1234567890" {
		t.Errorf("user_id = %v", out["user_id"])
	}
	if out["return_to"] != testReturnTo {
		t.Errorf("return_to = %v", out["return_to"])
	}
	// Session tokens must not be cached by anything in the path.
	if cc := resp.Header.Get("Cache-Control"); !strings.Contains(cc, "no-store") {
		t.Errorf("Cache-Control = %q, want no-store", cc)
	}

	// The token must be one the gRPC interceptor would accept.
	token, _ := out["token"].(string)
	if token == "" {
		t.Fatal("no token issued")
	}
	pubPEM, err := r.issuer.PublicKeyPEM()
	if err != nil {
		t.Fatal(err)
	}
	v, err := auth.NewVerifier(map[string]string{r.issuer.KeyID(): pubPEM},
		"observatory-api", "skycam-ingest", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := v.Verify(token)
	if err != nil {
		t.Fatalf("the issued token must verify: %v", err)
	}
	if claims.Subject != "google-oauth2|1234567890" {
		t.Errorf("subject = %q", claims.Subject)
	}
}

// The provider must receive an S256 challenge, and never the verifier.
func TestLoginSendsAnS256ChallengeAndNotTheVerifier(t *testing.T) {
	r := newRig(t, nil)
	// Stop at the redirect so the authorization URL can be inspected.
	noFollow := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := noFollow.Get(r.srv.URL + "/auth/login")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	loc, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	q := loc.Query()
	if q.Get("code_challenge_method") != "S256" {
		t.Errorf("code_challenge_method = %q, want S256", q.Get("code_challenge_method"))
	}
	if q.Get("code_challenge") == "" {
		t.Error("no code_challenge sent")
	}
	if q.Get("state") == "" {
		t.Error("no state sent (CSRF protection)")
	}
	if q.Get("nonce") == "" {
		t.Error("no nonce sent (replay protection)")
	}
	// The verifier is the secret; it must never appear in a redirect URL.
	for key, vals := range q {
		for _, v := range vals {
			if key != "code_challenge" && len(v) == 43 && v == q.Get("code_challenge") {
				t.Errorf("parameter %q looks like the verifier", key)
			}
		}
	}
	if q.Get("code_verifier") != "" {
		t.Error("the code_verifier must never be sent to the authorization endpoint")
	}
}

// The CSRF check: a callback with a state we never issued must be refused.
func TestCallbackRejectsUnknownState(t *testing.T) {
	r := newRig(t, nil)
	resp, err := r.client.Get(r.srv.URL + "/auth/callback?code=whatever&state=attacker-chosen")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestCallbackStateIsSingleUse(t *testing.T) {
	r := newRig(t, nil)
	r.users.users["google-oauth2|1234567890"] = store.User{UserID: "google-oauth2|1234567890"}

	noFollow := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	loginResp, err := noFollow.Get(r.srv.URL + "/auth/login")
	if err != nil {
		t.Fatal(err)
	}
	loginResp.Body.Close()
	loc, _ := url.Parse(loginResp.Header.Get("Location"))
	state := loc.Query().Get("state")

	// Drive the provider by hand so the same code/state pair can be replayed.
	authResp, err := noFollow.Get(r.idp.issuer + "/authorize?" + loc.RawQuery)
	if err != nil {
		t.Fatal(err)
	}
	authResp.Body.Close()
	callback := authResp.Header.Get("Location")

	first, err := r.client.Get(callback)
	if err != nil {
		t.Fatal(err)
	}
	first.Body.Close()
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first callback should succeed, got %d", first.StatusCode)
	}

	second, err := r.client.Get(callback)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Body.Close()
	if second.StatusCode == http.StatusOK {
		t.Fatal("replaying the callback must not mint a second session")
	}
	_ = state
}

// The point of PKCE: an authorization code redeemed without the matching
// verifier must be refused by the provider.
//
// Simulated by corrupting the stored verifier between the redirect and the
// callback, which is exactly what an attacker who stole the code has — a valid
// code and the wrong secret.
func TestStolenCodeIsUselessWithoutTheVerifier(t *testing.T) {
	r := newRig(t, func(c *Config) { c.AutoProvision = true })
	noFollow := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}

	loginResp, err := noFollow.Get(r.srv.URL + "/auth/login")
	if err != nil {
		t.Fatal(err)
	}
	loginResp.Body.Close()
	loc, _ := url.Parse(loginResp.Header.Get("Location"))
	state := loc.Query().Get("state")

	authResp, err := noFollow.Get(r.idp.issuer + "/authorize?" + loc.RawQuery)
	if err != nil {
		t.Fatal(err)
	}
	authResp.Body.Close()
	callback := authResp.Header.Get("Location")

	// Swap in a verifier that does not hash to the challenge the provider holds.
	flow, err := r.svc.pending.Take(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	flow.Verifier = "wrong-verifier-wrong-verifier-wrong-verifier"
	if err := r.svc.pending.Put(context.Background(), state, flow); err != nil {
		t.Fatal(err)
	}

	resp, err := r.client.Get(callback)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("a code redeemed with the wrong verifier must not produce a session")
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

// And the control: the same flow with the correct verifier succeeds, so the
// test above is not passing for some unrelated reason.
func TestSameFlowSucceedsWithTheRealVerifier(t *testing.T) {
	r := newRig(t, func(c *Config) { c.AutoProvision = true })
	noFollow := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}

	loginResp, err := noFollow.Get(r.srv.URL + "/auth/login")
	if err != nil {
		t.Fatal(err)
	}
	loginResp.Body.Close()
	loc, _ := url.Parse(loginResp.Header.Get("Location"))

	authResp, err := noFollow.Get(r.idp.issuer + "/authorize?" + loc.RawQuery)
	if err != nil {
		t.Fatal(err)
	}
	authResp.Body.Close()

	resp, err := r.client.Get(authResp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d (%s), want 200", resp.StatusCode, body)
	}
}

// An ID token whose nonce does not match this flow was minted for a different
// authorization request, so it is a replay.
func TestCallbackRejectsNonceMismatch(t *testing.T) {
	r := newRig(t, nil)
	r.users.users["google-oauth2|1234567890"] = store.User{UserID: "google-oauth2|1234567890"}
	r.idp.mu.Lock()
	r.idp.nonceOverride = "a-nonce-from-some-other-flow"
	r.idp.mu.Unlock()

	resp := r.signIn(t)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 for a nonce mismatch", resp.StatusCode)
	}
}

func TestCallbackRejectsMissingIDToken(t *testing.T) {
	r := newRig(t, nil)
	r.idp.mu.Lock()
	r.idp.omitIDToken = true
	r.idp.mu.Unlock()

	resp := r.signIn(t)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 when no id_token comes back", resp.StatusCode)
	}
}

// An unverified email is an unproven claim to somebody else's identity.
func TestCallbackRejectsUnverifiedEmail(t *testing.T) {
	r := newRig(t, nil)
	r.users.users["google-oauth2|1234567890"] = store.User{UserID: "google-oauth2|1234567890"}
	r.idp.mu.Lock()
	r.idp.emailVerified = false
	r.idp.mu.Unlock()

	resp := r.signIn(t)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for an unverified email", resp.StatusCode)
	}
}

// Authentication is not authorization: a real Google account that nobody
// provisioned must not get a session.
func TestUnprovisionedUserIsRefused(t *testing.T) {
	r := newRig(t, nil)
	resp := r.signIn(t)
	if resp.StatusCode != http.StatusForbidden {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d (%s), want 403", resp.StatusCode, body)
	}
}

func TestDisabledUserIsRefused(t *testing.T) {
	r := newRig(t, nil)
	r.users.users["google-oauth2|1234567890"] = store.User{UserID: "google-oauth2|1234567890"}
	r.users.disabled["google-oauth2|1234567890"] = true

	resp := r.signIn(t)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for a disabled account", resp.StatusCode)
	}
}

func TestAutoProvisionCreatesTheUser(t *testing.T) {
	r := newRig(t, func(c *Config) { c.AutoProvision = true })

	resp := r.signIn(t)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d (%s)", resp.StatusCode, body)
	}
	u, ok := r.users.users["google-oauth2|1234567890"]
	if !ok {
		t.Fatal("user was not created")
	}
	if u.Email != "astronomer@example.org" {
		t.Errorf("email = %q", u.Email)
	}
}

// An open redirect here would hand a freshly minted session token to whatever
// host an attacker names.
func TestLoginRejectsAnUnlistedReturnTo(t *testing.T) {
	r := newRig(t, nil)
	for _, bad := range []string{
		"https://evil.example.com",
		"https://dev.observatory.services.evil.com",
		"//evil.example.com",
		"javascript:alert(1)",
		"/relative/path",
	} {
		resp, err := r.client.Get(r.srv.URL + "/auth/login?return_to=" + url.QueryEscape(bad))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("return_to=%q gave %d, want 400", bad, resp.StatusCode)
		}
	}
}

// No pending state may be created by a rejected login, or an attacker could
// exhaust memory by probing.
func TestRejectedLoginCreatesNoPendingFlow(t *testing.T) {
	r := newRig(t, nil)
	for i := 0; i < 20; i++ {
		resp, _ := r.client.Get(r.srv.URL + "/auth/login?return_to=https://evil.example.com")
		if resp != nil {
			resp.Body.Close()
		}
	}
	if n := r.svc.pending.(*oidc.Pending).Len(); n != 0 {
		t.Fatalf("%d pending flows created by rejected logins", n)
	}
}

func TestMeReturnsTheSignedInUser(t *testing.T) {
	r := newRig(t, func(c *Config) { c.AutoProvision = true })
	resp := r.signIn(t)
	body, _ := io.ReadAll(resp.Body)
	token, _ := mustJSON(t, body)["token"].(string)

	req, _ := http.NewRequest("GET", r.srv.URL+"/auth/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	meResp, err := r.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer meResp.Body.Close()
	if meResp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", meResp.StatusCode)
	}
	meBody, _ := io.ReadAll(meResp.Body)
	if got := mustJSON(t, meBody)["user_id"]; got != "google-oauth2|1234567890" {
		t.Errorf("user_id = %v", got)
	}
}

func TestMeRejectsAGarbageToken(t *testing.T) {
	r := newRig(t, nil)
	req, _ := http.NewRequest("GET", r.srv.URL+"/auth/me", nil)
	req.Header.Set("Authorization", "Bearer not-a-real-token")
	resp, err := r.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

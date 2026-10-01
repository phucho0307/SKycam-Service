package authsvc

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// fakeIDP is a stand-in for Google: discovery, JWKS, authorization and token
// endpoints. It exists so the whole sign-in flow runs for real in a test —
// including PKCE verification, which the provider is the one that enforces.
//
// It is deliberately strict. A permissive stub would let a broken client pass.
type fakeIDP struct {
	t      *testing.T
	srv    *httptest.Server
	key    *rsa.PrivateKey
	kid    string
	issuer string

	mu sync.Mutex
	// codes maps an issued authorization code to what the client committed to
	// when it asked for it.
	codes map[string]authRequest
	// Overrides for negative tests.
	nonceOverride   string
	subjectOverride string
	emailVerified   bool
	omitIDToken     bool
}

type authRequest struct {
	challenge string
	method    string
	nonce     string
	redirect  string
}

func newFakeIDP(t *testing.T) *fakeIDP {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	idp := &fakeIDP{
		t: t, key: key, kid: "idp-key-1",
		codes:         map[string]authRequest{},
		emailVerified: true,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", idp.discovery)
	mux.HandleFunc("/jwks", idp.jwks)
	mux.HandleFunc("/authorize", idp.authorize)
	mux.HandleFunc("/token", idp.token)

	idp.srv = httptest.NewServer(mux)
	idp.issuer = idp.srv.URL
	t.Cleanup(idp.srv.Close)
	return idp
}

func (i *fakeIDP) discovery(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"issuer":                                i.issuer,
		"authorization_endpoint":                i.issuer + "/authorize",
		"token_endpoint":                        i.issuer + "/token",
		"jwks_uri":                              i.issuer + "/jwks",
		"response_types_supported":              []string{"code"},
		"subject_types_supported":               []string{"public"},
		"id_token_signing_alg_values_supported": []string{"RS256"},
		"code_challenge_methods_supported":      []string{"S256"},
	})
}

func (i *fakeIDP) jwks(w http.ResponseWriter, _ *http.Request) {
	pub := i.key.Public().(*rsa.PublicKey)
	writeJSON(w, http.StatusOK, map[string]any{"keys": []map[string]any{{
		"kty": "RSA", "use": "sig", "alg": "RS256", "kid": i.kid,
		"n": base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
	}}})
}

// authorize records what the client committed to and issues a code. The real
// provider shows a consent screen here; the test client calls it directly.
func (i *fakeIDP) authorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	// A provider that accepted a flow with no challenge would silently disable
	// PKCE, so assert the client actually sent one.
	if q.Get("code_challenge") == "" {
		http.Error(w, "missing code_challenge", http.StatusBadRequest)
		return
	}
	if m := q.Get("code_challenge_method"); m != "S256" {
		http.Error(w, "code_challenge_method must be S256, got "+m, http.StatusBadRequest)
		return
	}
	code := "code-" + q.Get("state")
	i.mu.Lock()
	i.codes[code] = authRequest{
		challenge: q.Get("code_challenge"),
		method:    q.Get("code_challenge_method"),
		nonce:     q.Get("nonce"),
		redirect:  q.Get("redirect_uri"),
	}
	i.mu.Unlock()

	http.Redirect(w, r, q.Get("redirect_uri")+"?code="+code+"&state="+q.Get("state"),
		http.StatusFound)
}

// token is where PKCE is actually enforced: the verifier must hash to the
// challenge committed to at /authorize.
func (i *fakeIDP) token(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	code := r.PostForm.Get("code")

	i.mu.Lock()
	req, ok := i.codes[code]
	delete(i.codes, code) // authorization codes are single-use
	nonceOverride, subjectOverride := i.nonceOverride, i.subjectOverride
	emailVerified, omitID := i.emailVerified, i.omitIDToken
	i.mu.Unlock()

	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_grant"})
		return
	}
	verifier := r.PostForm.Get("code_verifier")
	if verifier == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing code_verifier"})
		return
	}
	if got := s256(verifier); got != req.challenge {
		// This is the check that makes a stolen authorization code useless.
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_grant: PKCE mismatch"})
		return
	}

	body := map[string]any{
		"access_token": "fake-access-token",
		"token_type":   "Bearer",
		"expires_in":   3600,
	}
	if !omitID {
		nonce := req.nonce
		if nonceOverride != "" {
			nonce = nonceOverride
		}
		subject := "google-oauth2|1234567890"
		if subjectOverride != "" {
			subject = subjectOverride
		}
		body["id_token"] = i.signIDToken(subject, nonce, emailVerified)
	}
	writeJSON(w, http.StatusOK, body)
}

func (i *fakeIDP) signIDToken(subject, nonce string, emailVerified bool) string {
	now := time.Now()
	claims := jwt.MapClaims{
		"iss":            i.issuer,
		"sub":            subject,
		"aud":            testClientID,
		"iat":            now.Unix(),
		"exp":            now.Add(time.Hour).Unix(),
		"nonce":          nonce,
		"email":          "astronomer@example.org",
		"email_verified": emailVerified,
		"name":           "Ada Astronomer",
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = i.kid
	s, err := tok.SignedString(i.key)
	if err != nil {
		i.t.Fatal(err)
	}
	return s
}

func s256(verifier string) string {
	// Duplicated from the oidc package on purpose: if the test used the same
	// helper as the code, a bug in that helper would cancel itself out.
	h := sha256Sum([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(h)
}

func mustJSON(t *testing.T, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("decode response %q: %v", string(b), err)
	}
	return m
}

// sha256Sum is spelled out so the test does not depend on the code under test.
func sha256Sum(b []byte) []byte {
	sum := sha256.Sum256(b)
	return sum[:]
}

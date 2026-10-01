package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	testIssuer   = "observatory-api"
	testAudience = "skycam-ingest"
)

type testKey struct {
	kid  string
	priv ed25519.PrivateKey
	pem  string
}

func newKey(t *testing.T, kid string) testKey {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return testKey{kid: kid, priv: priv,
		pem: string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))}
}

// sign mints a token; claims can be adjusted before signing.
func (k testKey) sign(t *testing.T, mutate func(*userClaims), header map[string]any) string {
	t.Helper()
	now := time.Now()
	c := userClaims{
		Email: "astronomer@example.org",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "user-1",
			Issuer:    testIssuer,
			Audience:  jwt.ClaimStrings{testAudience},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
		},
	}
	if mutate != nil {
		mutate(&c)
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, c)
	if k.kid != "" {
		tok.Header["kid"] = k.kid
	}
	for hk, hv := range header {
		tok.Header[hk] = hv
	}
	s, err := tok.SignedString(k.priv)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func newVerifier(t *testing.T, keys ...testKey) *Verifier {
	t.Helper()
	m := map[string]string{}
	for _, k := range keys {
		m[k.kid] = k.pem
	}
	v, err := NewVerifier(m, testIssuer, testAudience, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestVerifyAcceptsAWellFormedToken(t *testing.T) {
	k := newKey(t, "k1")
	v := newVerifier(t, k)

	claims, err := v.Verify(k.sign(t, nil, nil))
	if err != nil {
		t.Fatalf("expected the token to verify: %v", err)
	}
	if claims.Subject != "user-1" {
		t.Errorf("subject = %q, want user-1", claims.Subject)
	}
	if claims.Email != "astronomer@example.org" {
		t.Errorf("email = %q", claims.Email)
	}
}

func TestVerifyRejects(t *testing.T) {
	k := newKey(t, "k1")
	other := newKey(t, "k1") // same kid, different key
	v := newVerifier(t, k)

	cases := []struct {
		name  string
		token string
	}{
		{"expired", k.sign(t, func(c *userClaims) {
			c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-2 * time.Hour))
		}, nil)},
		{"not yet valid", k.sign(t, func(c *userClaims) {
			c.NotBefore = jwt.NewNumericDate(time.Now().Add(time.Hour))
		}, nil)},
		{"wrong issuer", k.sign(t, func(c *userClaims) { c.Issuer = "evil" }, nil)},
		{"wrong audience", k.sign(t, func(c *userClaims) {
			c.Audience = jwt.ClaimStrings{"some-other-service"}
		}, nil)},
		{"no subject", k.sign(t, func(c *userClaims) { c.Subject = "" }, nil)},
		{"signed by another key", other.sign(t, nil, nil)},
		{"unknown kid", k.sign(t, nil, map[string]any{"kid": "does-not-exist"})},
		{"garbage", "not.a.token"},
		{"empty", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := v.Verify(tc.token); err == nil {
				t.Fatal("expected rejection, got success")
			}
		})
	}
}

// A token with no exp never stops working, so a stolen one is permanent.
func TestVerifyRejectsTokenWithNoExpiry(t *testing.T) {
	k := newKey(t, "k1")
	v := newVerifier(t, k)
	if _, err := v.Verify(k.sign(t, func(c *userClaims) { c.ExpiresAt = nil }, nil)); err == nil {
		t.Fatal("a token without an expiry must be rejected")
	}
}

// The classic JWT attack: swap the algorithm to "none" and drop the signature.
func TestVerifyRejectsAlgNone(t *testing.T) {
	k := newKey(t, "k1")
	v := newVerifier(t, k)

	tok := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.RegisteredClaims{
		Subject:   "attacker",
		Issuer:    testIssuer,
		Audience:  jwt.ClaimStrings{testAudience},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})
	tok.Header["kid"] = "k1"
	s, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(s); err == nil {
		t.Fatal("alg=none must be rejected")
	}
}

// Algorithm confusion: sign with HMAC using the PUBLIC key as the shared
// secret. The public key is, by definition, public — so if the verifier let the
// token pick the algorithm, anyone could mint an admin token.
func TestVerifyRejectsHMACSignedWithThePublicKey(t *testing.T) {
	k := newKey(t, "k1")
	v := newVerifier(t, k)

	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   "attacker",
		Issuer:    testIssuer,
		Audience:  jwt.ClaimStrings{testAudience},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})
	tok.Header["kid"] = "k1"
	s, err := tok.SignedString([]byte(k.pem))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(s); err == nil {
		t.Fatal("HS256 signed with the public key must be rejected")
	}
}

func TestVerifierSupportsKeyRotation(t *testing.T) {
	oldKey := newKey(t, "2026-01")
	newKeyPair := newKey(t, "2026-07")
	v := newVerifier(t, oldKey, newKeyPair)

	// Both are live during the overlap window.
	for _, k := range []testKey{oldKey, newKeyPair} {
		if _, err := v.Verify(k.sign(t, nil, nil)); err != nil {
			t.Errorf("kid %s should verify during rotation: %v", k.kid, err)
		}
	}

	// After retiring the old key, tokens carrying its kid stop working.
	retired := newVerifier(t, newKeyPair)
	if _, err := retired.Verify(oldKey.sign(t, nil, nil)); err == nil {
		t.Error("a retired key must stop verifying")
	}
}

// With several keys configured, a token without a kid must not be tried against
// each one in turn -- that would make retiring a key meaningless.
func TestVerifyRequiresKidWhenSeveralKeysExist(t *testing.T) {
	k1 := newKey(t, "k1")
	k2 := newKey(t, "k2")

	single := newVerifier(t, testKey{kid: "", priv: k1.priv, pem: k1.pem})
	noKid := testKey{priv: k1.priv, pem: k1.pem}.sign(t, nil, nil)
	if _, err := single.Verify(noKid); err != nil {
		t.Errorf("one key and no kid should be accepted: %v", err)
	}

	many := newVerifier(t, k1, k2)
	if _, err := many.Verify(noKid); err == nil {
		t.Error("several keys and no kid must be rejected")
	}
}

func TestNewVerifierRequiresIssuerAudienceAndKeys(t *testing.T) {
	k := newKey(t, "k1")
	keys := map[string]string{k.kid: k.pem}

	if _, err := NewVerifier(nil, testIssuer, testAudience, 0); err == nil {
		t.Error("no keys must be an error")
	}
	if _, err := NewVerifier(keys, "", testAudience, 0); err == nil {
		t.Error("empty issuer must be an error")
	}
	if _, err := NewVerifier(keys, testIssuer, "", 0); err == nil {
		t.Error("empty audience must be an error")
	}
	if _, err := NewVerifier(map[string]string{"k": "not pem"}, testIssuer, testAudience, 0); err == nil {
		t.Error("malformed PEM must be an error")
	}
}

// Package oidc implements sign-in with an OpenID Connect provider using the
// authorization code flow with PKCE.
//
// Three separate random values do three separate jobs, and conflating them is
// the usual source of bugs:
//
//   - code_verifier (PKCE, RFC 7636) proves the client redeeming the code is
//     the same one that started the flow. It defends against an intercepted
//     authorization code — a real risk on mobile and SPA redirects, and cheap
//     insurance for a confidential client too.
//   - state (RFC 6749 §10.12) defends against CSRF: someone tricking a signed-in
//     user's browser into completing *the attacker's* login, silently binding
//     the victim's session to the attacker's account.
//   - nonce (OIDC core) binds the ID token to this authorization request, so a
//     token captured elsewhere cannot be replayed into our callback.
//
// PKCE does not replace state, and state does not replace nonce. All three.
package oidc

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
)

// PKCE holds one flow's proof-of-possession pair.
type PKCE struct {
	// Verifier is the secret. It never leaves this service.
	Verifier string
	// Challenge is SHA-256(verifier), safe to put in a redirect URL.
	Challenge string
}

// NewPKCE generates a verifier and its S256 challenge.
//
// 32 random bytes base64url-encoded is 43 characters, the minimum RFC 7636
// allows and ~256 bits of entropy. base64url's alphabet is a subset of the
// "unreserved" characters the RFC requires, so no escaping is needed.
func NewPKCE() (PKCE, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return PKCE{}, fmt.Errorf("generate code verifier: %w", err)
	}
	verifier := base64.RawURLEncoding.EncodeToString(buf)
	return PKCE{Verifier: verifier, Challenge: S256Challenge(verifier)}, nil
}

// S256Challenge is BASE64URL(SHA256(ASCII(verifier))).
//
// The `plain` method, where the challenge *is* the verifier, is deliberately not
// offered: it gives no protection at all against anyone who can see the
// authorization request.
func S256Challenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// RandomToken returns a URL-safe random string for state and nonce values.
func RandomToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate random token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// SameToken compares two opaque tokens in constant time. Used for state, which
// an attacker gets to choose half of, so a byte-by-byte compare would leak the
// expected value through timing.
func SameToken(a, b string) bool {
	return len(a) != 0 && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

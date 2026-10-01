package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Issuer mints the platform's own session tokens.
//
// The identity provider's ID token is NOT handed to the rest of the platform.
// It is consumed once, at sign-in, and exchanged for a token we control: our
// issuer, our audience, our lifetime, our key. That means an expiry we choose
// rather than Google's, no dependency on the provider being reachable to
// validate a request, and one token format across services.
//
// In the deployed platform this belongs in the `api` crate, which is the sole
// issuer. It lives here so the Go service is self-contained and the flow is
// testable end to end; the *verification* side (Verifier) is the part that
// stays here permanently.
type Issuer struct {
	kid      string
	priv     ed25519.PrivateKey
	issuer   string
	audience string
	ttl      time.Duration
}

func NewIssuer(kid, privatePEM, issuer, audience string, ttl time.Duration) (*Issuer, error) {
	if kid == "" {
		return nil, errors.New("a key id is required, so keys can be rotated")
	}
	if issuer == "" || audience == "" {
		return nil, errors.New("issuer and audience are required")
	}
	if ttl <= 0 {
		// Short, because nothing about a token can be revoked once minted.
		// Authorization is re-read from the database per request, so a short
		// lifetime costs little and bounds the damage from a stolen token.
		ttl = time.Hour
	}
	priv, err := parseEd25519PrivateKey(privatePEM)
	if err != nil {
		return nil, err
	}
	return &Issuer{kid: kid, priv: priv, issuer: issuer, audience: audience, ttl: ttl}, nil
}

func parseEd25519PrivateKey(pemStr string) (ed25519.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errors.New("private key is not PEM encoded")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	priv, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("not an Ed25519 private key (%T)", key)
	}
	return priv, nil
}

// Issue mints a session token for a user. It carries identity only: what the
// subject may *do* is looked up per request, so a grant revoked a second after
// this token is issued takes effect immediately.
func (i *Issuer) Issue(userID, email string) (token string, expiresAt time.Time, err error) {
	now := time.Now()
	exp := now.Add(i.ttl)
	t := jwt.NewWithClaims(jwt.SigningMethodEdDSA, userClaims{
		Email: email,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			Issuer:    i.issuer,
			Audience:  jwt.ClaimStrings{i.audience},
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(exp),
			ID:        mustJTI(),
		},
	})
	t.Header["kid"] = i.kid
	s, err := t.SignedString(i.priv)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign token: %w", err)
	}
	return s, exp, nil
}

// PublicKeyPEM is the key a verifier needs, so a deployment can be wired up
// from one generated keypair.
func (i *Issuer) PublicKeyPEM() (string, error) {
	der, err := x509.MarshalPKIXPublicKey(i.priv.Public())
	if err != nil {
		return "", err
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})), nil
}

func (i *Issuer) KeyID() string    { return i.kid }
func (i *Issuer) Issuer() string   { return i.issuer }
func (i *Issuer) Audience() string { return i.audience }

// mustJTI gives each token a unique id, so a future revocation list has
// something to name. Not consulted today -- the hook is cheap, adding it later
// to already-issued tokens is not.
func mustJTI() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

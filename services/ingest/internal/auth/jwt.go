package auth

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Verifier checks user tokens minted elsewhere.
//
// Asymmetric on purpose: the `api` crate is the only issuer and holds the
// private key; every service that needs to *verify* holds only a public key. A
// shared HMAC secret would make every verifier capable of minting tokens, so a
// read-only service leaking its config would hand over the ability to forge an
// admin. Ed25519 because the keys and signatures are small and there are no
// parameters to get wrong (no curve choice, no padding mode).
type Verifier struct {
	// Keyed by `kid` so keys can be rotated: publish the new public key, start
	// signing with it, retire the old one once no live token carries it.
	keys     map[string]ed25519.PublicKey
	issuer   string
	audience string
	leeway   time.Duration
}

// Claims is the subset of the token this service acts on. Everything here is
// *asserted* by the issuer; what the subject may actually do is decided by the
// database, never by the token.
type Claims struct {
	Subject string
	Email   string
}

var (
	ErrNoVerifier   = errors.New("no JWT verification key configured")
	ErrInvalidToken = errors.New("invalid token")
)

// NewVerifier takes PEM-encoded Ed25519 public keys by key id.
func NewVerifier(keysPEM map[string]string, issuer, audience string, leeway time.Duration) (*Verifier, error) {
	if len(keysPEM) == 0 {
		return nil, ErrNoVerifier
	}
	if issuer == "" || audience == "" {
		// Without both, a token minted for a different service in the same
		// platform would be accepted here. They are not optional.
		return nil, errors.New("issuer and audience are required")
	}
	if leeway <= 0 {
		leeway = 30 * time.Second
	}
	keys := make(map[string]ed25519.PublicKey, len(keysPEM))
	for kid, p := range keysPEM {
		k, err := parseEd25519PublicKey(p)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", kid, err)
		}
		keys[kid] = k
	}
	return &Verifier{keys: keys, issuer: issuer, audience: audience, leeway: leeway}, nil
}

func parseEd25519PublicKey(pemStr string) (ed25519.PublicKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return nil, errors.New("not PEM encoded")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	ed, ok := pub.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("not an Ed25519 key (%T)", pub)
	}
	return ed, nil
}

// Verify validates the signature and every registered claim, and returns the
// subject. It deliberately reveals nothing about *why* a token failed.
func (v *Verifier) Verify(tokenStr string) (Claims, error) {
	var claims userClaims

	tok, err := jwt.ParseWithClaims(tokenStr, &claims, v.keyFor,
		// The critical one. Without it an attacker picks the algorithm: "none"
		// skips verification entirely, and HS256 would have the library verify
		// an HMAC using our *public* key as the shared secret — which is public.
		jwt.WithValidMethods([]string{"EdDSA"}),
		jwt.WithIssuer(v.issuer),
		jwt.WithAudience(v.audience),
		jwt.WithLeeway(v.leeway),
		// Tokens without an expiry never stop working; a stolen one is then
		// permanent. Require it rather than treating absent as infinite.
		jwt.WithExpirationRequired(),
	)
	if err != nil || !tok.Valid {
		return Claims{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if claims.Subject == "" {
		return Claims{}, fmt.Errorf("%w: no subject", ErrInvalidToken)
	}
	// Email is informational only -- it is never used to decide access, because
	// an email can be reassigned while a subject id cannot.
	return Claims{Subject: claims.Subject, Email: claims.Email}, nil
}

// userClaims adds the one non-registered claim this service reads.
type userClaims struct {
	Email string `json:"email,omitempty"`
	jwt.RegisteredClaims
}

func (v *Verifier) keyFor(t *jwt.Token) (any, error) {
	kid, _ := t.Header["kid"].(string)
	if kid == "" {
		// Exactly one key configured and no kid: accept it, so a single-key
		// deployment need not set one. With several, a kid is mandatory --
		// trying each key in turn would make rotation silently meaningless.
		if len(v.keys) == 1 {
			for _, k := range v.keys {
				return k, nil
			}
		}
		return nil, errors.New("token has no kid and several keys are configured")
	}
	k, ok := v.keys[kid]
	if !ok {
		return nil, fmt.Errorf("unknown kid %q", kid)
	}
	return k, nil
}

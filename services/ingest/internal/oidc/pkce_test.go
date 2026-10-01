package oidc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"regexp"
	"testing"
	"time"
)

// RFC 7636 §4.1: 43..128 characters from [A-Za-z0-9-._~].
var verifierCharset = regexp.MustCompile(`^[A-Za-z0-9\-._~]+$`)

func TestVerifierMeetsRFC7636(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		p, err := NewPKCE()
		if err != nil {
			t.Fatal(err)
		}
		if n := len(p.Verifier); n < 43 || n > 128 {
			t.Fatalf("verifier length %d, RFC 7636 requires 43..128", n)
		}
		if !verifierCharset.MatchString(p.Verifier) {
			t.Fatalf("verifier %q contains characters outside the unreserved set", p.Verifier)
		}
		if seen[p.Verifier] {
			t.Fatal("generated the same verifier twice")
		}
		seen[p.Verifier] = true
	}
}

func TestChallengeIsBase64URLSHA256OfTheVerifier(t *testing.T) {
	p, err := NewPKCE()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(p.Verifier))
	want := base64.RawURLEncoding.EncodeToString(sum[:])
	if p.Challenge != want {
		t.Fatalf("challenge = %q, want %q", p.Challenge, want)
	}
	// Unpadded: '=' would have to be percent-encoded in a URL, and RFC 7636
	// specifies base64url without padding.
	if got := p.Challenge; got[len(got)-1] == '=' {
		t.Fatal("challenge must not be padded")
	}
}

// The challenge must not be usable as the verifier -- that is the `plain`
// method, which offers no protection at all.
func TestChallengeDiffersFromVerifier(t *testing.T) {
	p, err := NewPKCE()
	if err != nil {
		t.Fatal(err)
	}
	if p.Challenge == p.Verifier {
		t.Fatal("challenge equals verifier; that is the plain method")
	}
	if S256Challenge(p.Challenge) == p.Challenge {
		t.Fatal("hashing is not being applied")
	}
}

func TestSameTokenRejectsEmptyAndMismatched(t *testing.T) {
	a, err := RandomToken()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := RandomToken()
	if !SameToken(a, a) {
		t.Error("identical tokens should match")
	}
	if SameToken(a, b) {
		t.Error("different tokens must not match")
	}
	// An absent state must never compare equal to an absent expectation --
	// otherwise a callback with no state at all would pass the CSRF check.
	if SameToken("", "") {
		t.Error("empty tokens must not match")
	}
	if SameToken(a, "") || SameToken("", a) {
		t.Error("empty must not match a real token")
	}
}

func TestPendingStateIsSingleUse(t *testing.T) {
	p := NewPending(time.Minute)
	p.Put(context.Background(), "state-1", Flow{Verifier: "v", Nonce: "n"})

	if _, err := p.Take(context.Background(), "state-1"); err != nil {
		t.Fatalf("first take should succeed: %v", err)
	}
	// A replayed callback must not complete a second sign-in.
	if _, err := p.Take(context.Background(), "state-1"); err == nil {
		t.Fatal("state must be single-use")
	}
}

func TestPendingRejectsUnknownAndExpiredIdentically(t *testing.T) {
	p := NewPending(10 * time.Millisecond)
	p.Put(context.Background(), "old", Flow{Verifier: "v"})
	time.Sleep(30 * time.Millisecond)

	_, expiredErr := p.Take(context.Background(), "old")
	_, unknownErr := p.Take(context.Background(), "never-existed")
	if expiredErr == nil || unknownErr == nil {
		t.Fatal("both must fail")
	}
	// Same error, so a caller cannot distinguish "expired" from "never issued".
	if expiredErr.Error() != unknownErr.Error() {
		t.Errorf("expired %v and unknown %v should be indistinguishable", expiredErr, unknownErr)
	}
}

func TestPendingSweepsAbandonedFlows(t *testing.T) {
	p := NewPending(10 * time.Millisecond)
	for i := 0; i < 50; i++ {
		tok, _ := RandomToken()
		p.Put(context.Background(), tok, Flow{Verifier: "v"})
	}
	if p.Len() != 50 {
		t.Fatalf("expected 50 pending flows, got %d", p.Len())
	}
	time.Sleep(30 * time.Millisecond)

	// Abandoned flows are the only way this map grows; a write sweeps them.
	p.Put(context.Background(), "fresh", Flow{Verifier: "v"})
	if got := p.Len(); got != 1 {
		t.Fatalf("expected only the fresh flow to remain, got %d", got)
	}
}

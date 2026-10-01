// Package authsvc serves the browser-facing sign-in endpoints.
//
//	GET /auth/login     start the flow; redirects to the identity provider
//	GET /auth/callback  the provider redirects back here with a code
//	GET /auth/me        who the presented token belongs to (debugging aid)
//
// This is a *confidential* client: the code-for-token exchange happens here,
// server side, with the client secret, and the browser never sees a provider
// token. PKCE is used anyway — it costs one hash and defends against an
// intercepted authorization code, which a client secret does not.
//
// In the deployed platform this belongs in the Rust `api` crate (the sole token
// issuer). It is in Go here so the service is self-contained and the whole flow
// can be exercised end to end against a stub provider.
package authsvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	coreoidc "github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"

	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/auth"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/oidc"
	"github.com/ObservatoryServices/ObservatoryServices/services/ingest/internal/store"
)

type Config struct {
	ProviderURL  string // e.g. https://accounts.google.com
	ClientID     string
	ClientSecret string
	RedirectURL  string
	// AllowedReturnTo are the exact origins a completed sign-in may return the
	// browser to. An allow-list, never a pattern: an open redirect here hands a
	// freshly minted session token to whatever host an attacker names.
	AllowedReturnTo []string
	// AutoProvision creates a user row on first sign-in. Off by default: with it
	// off, anyone with a Google account authenticates but only provisioned users
	// get in, which is what an observatory wants.
	AutoProvision bool
	FlowTTL       time.Duration
	// Flows holds in-flight sign-ins. Nil means in memory, which is correct only
	// with one replica: the callback can land on a different pod from the login.
	Flows oidc.FlowStore
}

type Service struct {
	cfg      Config
	verifier *coreoidc.IDTokenVerifier
	oauth    oauth2.Config
	pending  oidc.FlowStore
	issuer   *auth.Issuer
	// selfVerifier checks tokens this service issued, using the public half of
	// the issuing key — the same check the gRPC interceptor performs.
	selfVerifier *auth.Verifier
	users        store.GrantStore
	log          *slog.Logger
}

func New(ctx context.Context, cfg Config, issuer *auth.Issuer, users store.GrantStore, log *slog.Logger) (*Service, error) {
	if cfg.ClientID == "" || cfg.ClientSecret == "" || cfg.RedirectURL == "" {
		return nil, errors.New("client id, client secret and redirect URL are required")
	}
	if len(cfg.AllowedReturnTo) == 0 {
		return nil, errors.New("at least one allowed return-to origin is required")
	}
	// Discovery fetches the provider's endpoints and JWKS URL. go-oidc handles
	// key caching and rotation; hand-rolling that is where the bugs live.
	provider, err := coreoidc.NewProvider(ctx, cfg.ProviderURL)
	if err != nil {
		return nil, fmt.Errorf("oidc discovery for %s: %w", cfg.ProviderURL, err)
	}
	pubPEM, err := issuer.PublicKeyPEM()
	if err != nil {
		return nil, fmt.Errorf("derive public key: %w", err)
	}
	selfVerifier, err := auth.NewVerifier(map[string]string{issuer.KeyID(): pubPEM},
		issuer.Issuer(), issuer.Audience(), 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("build self verifier: %w", err)
	}

	flows := cfg.Flows
	if flows == nil {
		flows = oidc.NewPending(cfg.FlowTTL)
	}

	return &Service{
		cfg: cfg,
		// Checks signature against JWKS, issuer, audience and expiry. The nonce
		// is ours to check, and we do, below.
		verifier: provider.Verifier(&coreoidc.Config{ClientID: cfg.ClientID}),
		oauth: oauth2.Config{
			ClientID:     cfg.ClientID,
			ClientSecret: cfg.ClientSecret,
			Endpoint:     provider.Endpoint(),
			RedirectURL:  cfg.RedirectURL,
			Scopes:       []string{coreoidc.ScopeOpenID, "email", "profile"},
		},
		pending:      flows,
		issuer:       issuer,
		selfVerifier: selfVerifier,
		users:        users,
		log:          log,
	}, nil
}

func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /auth/login", s.handleLogin)
	mux.HandleFunc("GET /auth/callback", s.handleCallback)
	mux.HandleFunc("GET /auth/me", s.handleMe)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

// handleLogin starts the flow: generate the PKCE pair, state and nonce, stash
// them, and redirect.
func (s *Service) handleLogin(w http.ResponseWriter, r *http.Request) {
	returnTo := r.URL.Query().Get("return_to")
	if returnTo == "" {
		returnTo = s.cfg.AllowedReturnTo[0]
	}
	if !s.returnToAllowed(returnTo) {
		// Rejected *before* anything is stored, so a probe for an open redirect
		// cannot even create state.
		http.Error(w, "return_to is not an allowed destination", http.StatusBadRequest)
		return
	}

	pkce, err := oidc.NewPKCE()
	if err != nil {
		s.fail(w, "start sign-in", err)
		return
	}
	state, err := oidc.RandomToken()
	if err != nil {
		s.fail(w, "start sign-in", err)
		return
	}
	nonce, err := oidc.RandomToken()
	if err != nil {
		s.fail(w, "start sign-in", err)
		return
	}

	if err := s.pending.Put(r.Context(), state,
		oidc.Flow{Verifier: pkce.Verifier, Nonce: nonce, ReturnTo: returnTo}); err != nil {
		// Refuse rather than redirect: a flow we failed to record is a callback
		// we will reject, after the user has typed their password.
		s.fail(w, "start sign-in", err)
		return
	}

	// The verifier is NOT sent; only its hash. That is the whole point of PKCE.
	authURL := s.oauth.AuthCodeURL(state,
		coreoidc.Nonce(nonce),
		oauth2.SetAuthURLParam("code_challenge", pkce.Challenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	)
	http.Redirect(w, r, authURL, http.StatusFound)
}

// handleCallback completes the flow.
func (s *Service) handleCallback(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// The provider reports user-facing failures (consent denied) here.
	if e := q.Get("error"); e != "" {
		http.Error(w, "sign-in was not completed: "+e, http.StatusUnauthorized)
		return
	}

	// State first: this is the CSRF check, and it also tells us which flow we
	// are in. Taking it removes it, so a replayed callback cannot succeed twice.
	flow, err := s.pending.Take(r.Context(), q.Get("state"))
	if errors.Is(err, oidc.ErrUnknownState) {
		http.Error(w, "sign-in request is unknown or has expired; start again", http.StatusBadRequest)
		return
	}
	if err != nil {
		// The store is down, which is our fault, not the user's: a 5xx, not a
		// "start again" that would send them round the same failing loop.
		s.fail(w, "complete sign-in", err)
		return
	}

	code := q.Get("code")
	if code == "" {
		http.Error(w, "no authorization code", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	// Redeeming the code proves possession of the verifier. A stolen code is
	// useless without it.
	tok, err := s.oauth.Exchange(ctx, code,
		oauth2.SetAuthURLParam("code_verifier", flow.Verifier))
	if err != nil {
		s.log.Warn("token exchange failed", "err", err)
		http.Error(w, "sign-in failed", http.StatusUnauthorized)
		return
	}

	rawID, ok := tok.Extra("id_token").(string)
	if !ok || rawID == "" {
		// An access token alone says nothing verifiable about *who* signed in.
		http.Error(w, "provider returned no id_token", http.StatusUnauthorized)
		return
	}
	idToken, err := s.verifier.Verify(ctx, rawID)
	if err != nil {
		s.log.Warn("id token rejected", "err", err)
		http.Error(w, "sign-in failed", http.StatusUnauthorized)
		return
	}
	// Ours to check: go-oidc verifies signature, issuer, audience and expiry,
	// but the nonce is application state and it cannot know ours.
	if !oidc.SameToken(idToken.Nonce, flow.Nonce) {
		s.log.Warn("id token nonce mismatch")
		http.Error(w, "sign-in failed", http.StatusUnauthorized)
		return
	}

	var claims struct {
		Email         string `json:"email"`
		EmailVerified bool   `json:"email_verified"`
		Name          string `json:"name"`
	}
	if err := idToken.Claims(&claims); err != nil {
		s.fail(w, "read id token claims", err)
		return
	}
	// An unverified email is an unproven claim to an identity. Accepting it
	// would let anyone who can create an account at the provider assert
	// somebody else's address.
	if claims.Email != "" && !claims.EmailVerified {
		http.Error(w, "the provider has not verified this email address", http.StatusForbidden)
		return
	}

	// Subject, never email: an email can be reassigned to a different person,
	// a subject id cannot.
	userID := idToken.Subject
	if err := s.resolveUser(ctx, userID, claims.Email, claims.Name); err != nil {
		if errors.Is(err, store.ErrUnknownUser) {
			http.Error(w, "this account is not provisioned for the observatory", http.StatusForbidden)
			return
		}
		s.fail(w, "resolve user", err)
		return
	}

	session, expiresAt, err := s.issuer.Issue(userID, claims.Email)
	if err != nil {
		s.fail(w, "issue session token", err)
		return
	}
	s.log.Info("sign-in complete", "user_id", userID)

	writeJSON(w, http.StatusOK, map[string]any{
		"token":      session,
		"expires_at": expiresAt.UTC().Format(time.RFC3339),
		"user_id":    userID,
		"email":      claims.Email,
		"return_to":  flow.ReturnTo,
	})
}

// resolveUser enforces the provisioning policy. Authentication has already
// succeeded at this point; this decides whether that identity is allowed in.
func (s *Service) resolveUser(ctx context.Context, userID, email, name string) error {
	if s.cfg.AutoProvision {
		return s.users.UpsertUser(ctx, store.User{UserID: userID, Email: email, DisplayName: name})
	}
	// AccessFor with an empty device answers "does this user exist and are they
	// enabled?" without needing a device in hand.
	_, err := s.users.AccessFor(ctx, userID, "")
	if errors.Is(err, store.ErrUserDisabled) {
		return store.ErrUnknownUser // same answer to the caller either way
	}
	return err
}

// handleMe echoes the identity behind a token. Useful for a frontend and for
// confirming a token works; it deliberately reports no permissions, because
// those depend on which device is being asked about.
func (s *Service) handleMe(w http.ResponseWriter, r *http.Request) {
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if tok == "" || tok == r.Header.Get("Authorization") {
		http.Error(w, "Authorization: Bearer <token> required", http.StatusUnauthorized)
		return
	}
	claims, err := s.issuerVerifier().Verify(tok)
	if err != nil {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user_id": claims.Subject, "email": claims.Email})
}

// issuerVerifier builds a verifier from the issuer's own public key, so /auth/me
// checks tokens exactly as the gRPC interceptor does.
func (s *Service) issuerVerifier() *auth.Verifier { return s.selfVerifier }

func (s *Service) returnToAllowed(returnTo string) bool {
	u, err := url.Parse(returnTo)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return false
	}
	origin := u.Scheme + "://" + u.Host
	for _, allowed := range s.cfg.AllowedReturnTo {
		if strings.EqualFold(origin, strings.TrimRight(allowed, "/")) {
			return true
		}
	}
	return false
}

func (s *Service) fail(w http.ResponseWriter, what string, err error) {
	s.log.Error(what+" failed", "err", err)
	http.Error(w, "sign-in failed", http.StatusInternalServerError)
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	// Session tokens must never land in a shared cache or in browser history.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

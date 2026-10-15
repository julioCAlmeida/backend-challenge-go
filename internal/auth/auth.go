// Package auth validates OAuth 2.0 access tokens issued by the external IdP
// (Keycloak) and exposes the authenticated principal.
//
// Validation: RS256 signature against the IdP JWKS (cached, refreshed on unknown
// kid), issuer, expiry/not-before, audience (OIDC_AUDIENCE) and token type
// "Bearer" (ID/refresh tokens are refused).
package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Roles (Keycloak realm roles) of the permission model.
const (
	// RoleProvider: a game provider; may submit and read its own wager transactions.
	RoleProvider = "wagering-provider"
	// RoleWalletOperator: the internal service; may open/read/reconcile wallets
	// and read any transaction. It cannot submit provider operations.
	RoleWalletOperator = "wallet-operator"
)

var (
	ErrMissingToken = errors.New("missing bearer token")
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("expired token")
)

// Principal is the authenticated caller.
type Principal struct {
	Subject  string
	ClientID string
	// ProviderID comes from the `provider_id` claim, set by the IdP per client.
	// It is the only source of the provider identity.
	ProviderID string
	Roles      []string
}

func (p Principal) HasRole(r string) bool { return slices.Contains(p.Roles, r) }

// IsProvider reports whether the principal acts as a game provider.
func (p Principal) IsProvider() bool { return p.HasRole(RoleProvider) && p.ProviderID != "" }

// IsOperator reports whether the principal is the internal wallet service.
func (p Principal) IsOperator() bool { return p.HasRole(RoleWalletOperator) }

// Config of the verifier.
type Config struct {
	IssuerURL string
	JWKSURL   string
	Audience  string
}

// Verifier validates access tokens.
type Verifier struct {
	cfg      Config
	verifier *oidc.IDTokenVerifier
}

// NewVerifier builds a verifier; keys are fetched lazily from the JWKS URL,
// which may differ from the issuer host (e.g. inside Docker networks).
func NewVerifier(cfg Config) *Verifier {
	keys := oidc.NewRemoteKeySet(context.Background(), cfg.JWKSURL)
	v := oidc.NewVerifier(cfg.IssuerURL, keys, &oidc.Config{
		SkipClientIDCheck:    true, // audience is checked explicitly below
		SupportedSigningAlgs: []string{oidc.RS256},
	})
	return &Verifier{cfg: cfg, verifier: v}
}

type claims struct {
	Type        string `json:"typ"`
	AZP         string `json:"azp"`
	ClientID    string `json:"client_id"`
	ProviderID  string `json:"provider_id"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// Verify validates a raw JWT access token.
func (v *Verifier) Verify(ctx context.Context, raw string) (Principal, error) {
	tok, err := v.verifier.Verify(ctx, raw)
	if err != nil {
		var expired *oidc.TokenExpiredError
		if errors.As(err, &expired) {
			return Principal{}, ErrExpiredToken
		}
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if !slices.Contains(tok.Audience, v.cfg.Audience) {
		return Principal{}, fmt.Errorf("%w: audience %v does not include %s", ErrInvalidToken, tok.Audience, v.cfg.Audience)
	}
	var c claims
	if err := tok.Claims(&c); err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if !strings.EqualFold(c.Type, "Bearer") {
		return Principal{}, fmt.Errorf("%w: token type %q is not an access token", ErrInvalidToken, c.Type)
	}
	client := c.AZP
	if client == "" {
		client = c.ClientID
	}
	return Principal{Subject: tok.Subject, ClientID: client, ProviderID: c.ProviderID, Roles: c.RealmAccess.Roles}, nil
}

// WaitForJWKS checks at startup that the IdP keys are reachable.
func WaitForJWKS(ctx context.Context, jwksURL string) error {
	client := &http.Client{Timeout: 3 * time.Second}
	backoff := 250 * time.Millisecond
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, jwksURL, nil)
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			err = fmt.Errorf("status %d", resp.StatusCode)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("identity provider keys unavailable at %s: %w", jwksURL, errors.Join(err, ctx.Err()))
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
		}
	}
}

type principalKey struct{}

// WithPrincipal stores the principal in the context.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// FromContext returns the authenticated principal.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

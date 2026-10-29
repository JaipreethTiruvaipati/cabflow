package api

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

// employee is read-only. admin manages its own tenant, and operator (our
// on-call folks) can also pause other tenants.
const (
	RoleEmployee = "employee"
	RoleAdmin    = "admin"
	RoleOperator = "operator"
)

type Principal struct {
	TenantID uuid.UUID
	Roles    []string
}

func (p Principal) Has(roles ...string) bool {
	for _, role := range roles {
		if slices.Contains(p.Roles, role) {
			return true
		}
	}
	return false
}

type Claims struct {
	TenantID string   `json:"tenant_id"`
	Roles    []string `json:"roles"`
	jwt.RegisteredClaims
}

// Auth checks HS256 bearer tokens; issuer and audience have to match too.
type Auth struct {
	Secret   []byte
	Issuer   string
	Audience string
}

func (a Auth) principal(r *http.Request) (Principal, error) {
	raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return Principal{}, errors.New("missing bearer token")
	}
	var claims Claims
	_, err := jwt.ParseWithClaims(raw, &claims, func(*jwt.Token) (any, error) { return a.Secret, nil },
		jwt.WithValidMethods([]string{"HS256"}), jwt.WithIssuer(a.Issuer), jwt.WithAudience(a.Audience), jwt.WithExpirationRequired())
	if err != nil {
		return Principal{}, err
	}
	tenant, err := uuid.Parse(claims.TenantID)
	if err != nil {
		return Principal{}, err
	}
	return Principal{TenantID: tenant, Roles: claims.Roles}, nil
}

type principalKey struct{}

// require lets the request through only with a valid token that has one of
// the roles.
func (a Auth) require(next http.HandlerFunc, roles ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := a.principal(r)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if !p.Has(roles...) {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		next(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	}
}

func principalFrom(r *http.Request) Principal {
	p, _ := r.Context().Value(principalKey{}).(Principal)
	return p
}

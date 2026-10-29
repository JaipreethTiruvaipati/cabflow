// Command token prints a signed JWT for local testing.
//
//	go run ./cmd/token -tenant <uuid> -roles admin
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/JaipreethTiruvaipati/cabflow/internal/api"
)

func main() {
	secret := flag.String("secret", os.Getenv("JWT_SECRET"), "HMAC secret (default $JWT_SECRET)")
	tenant := flag.String("tenant", "00000000-0000-0000-0000-000000000001", "tenant UUID")
	roles := flag.String("roles", "admin", "comma-separated roles: employee, admin, operator")
	ttl := flag.Duration("ttl", 24*time.Hour, "token lifetime")
	flag.Parse()
	if _, err := uuid.Parse(*tenant); err != nil || *secret == "" {
		fmt.Fprintln(os.Stderr, "need -secret (or JWT_SECRET) and a UUID -tenant")
		os.Exit(2)
	}
	claims := api.Claims{TenantID: *tenant, Roles: strings.Split(*roles, ","), RegisteredClaims: jwt.RegisteredClaims{
		Issuer: "tripora", Audience: jwt.ClaimStrings{"tripora-api"}, Subject: "local-user",
		IssuedAt: jwt.NewNumericDate(time.Now()), ExpiresAt: jwt.NewNumericDate(time.Now().Add(*ttl)),
	}}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(*secret))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(signed)
}

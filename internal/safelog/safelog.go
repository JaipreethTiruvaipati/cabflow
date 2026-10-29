// Package safelog strips errors down to something we can log. Postgres
// errors happily quote the offending row (phone numbers, addresses...).
package safelog

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// Error returns a log-safe description of err: the SQLSTATE for database
// errors, otherwise just the error type.
func Error(err error) string {
	var pgErr *pgconn.PgError
	switch {
	case err == nil:
		return ""
	case errors.As(err, &pgErr):
		return "pg:" + pgErr.Code
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	default:
		// The type (*net.OpError etc.) is usually enough to know what broke,
		// and unlike the message it can't contain user data.
		inner := err
		for next := errors.Unwrap(inner); next != nil; next = errors.Unwrap(inner) {
			inner = next
		}
		return fmt.Sprintf("%T", inner)
	}
}

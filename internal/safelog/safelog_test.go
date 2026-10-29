package safelog

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestErrorNeverEchoesData(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "23505", Message: "duplicate key", Detail: "Key (phone)=(+91 98765 43210) already exists."}
	cases := map[error]string{
		fmt.Errorf("insert job: %w", pgErr):                        "pg:23505",
		fmt.Errorf("finish: %w", context.DeadlineExceeded):         "timeout",
		fmt.Errorf("shutdown: %w", context.Canceled):               "cancelled",
		fmt.Errorf("send to +91 98765 43210: %w", fmt.Errorf("x")): "*errors.errorString",
	}
	for err, want := range cases {
		if got := Error(err); got != want || strings.Contains(got, "98765") {
			t.Errorf("Error(%q) = %q, want %q", err, got, want)
		}
	}
}

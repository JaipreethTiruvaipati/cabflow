package main

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/JaipreethTiruvaipati/cabflow/internal/testdb"
)

func TestPruneDeliveries(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Pool(t, testdb.New(t), nil)
	if _, err := pool.Exec(ctx, schema); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO deliveries (idempotency_key, tenant_id, received_at)
		SELECT i::text, $1, statement_timestamp() - interval '8 days' FROM generate_series(1, 5001) i;
		`, uuid.New()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO deliveries (idempotency_key, tenant_id) VALUES ('recent', $1)`, uuid.New()); err != nil {
		t.Fatal(err)
	}
	lock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err := lock.Exec(ctx, `SELECT pg_advisory_xact_lock(4242, 2)`); err != nil {
		t.Fatal(err)
	}
	if n, err := pruneDeliveries(ctx, pool); err != nil || n != 0 {
		t.Fatalf("another cleaner owns the lock: %d (%v)", n, err)
	}
	if err := lock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for _, want := range []int64{5000, 1, 0} {
		if n, err := pruneDeliveries(ctx, pool); err != nil || n != want {
			t.Fatalf("want %d expired receipts removed, got %d (%v)", want, n, err)
		}
	}
	var key string
	if err := pool.QueryRow(ctx, `SELECT idempotency_key FROM deliveries`).Scan(&key); err != nil || key != "recent" {
		t.Fatalf("recent dedupe receipt must stay: %q (%v)", key, err)
	}
}

// Package testdb hands each test its own scratch database. Tests skip when
// TEST_DATABASE_URL isn't set, e.g.
//
//	docker compose up -d postgres
//	TEST_DATABASE_URL=postgres://tripora:tripora@localhost:5432/tripora?sslmode=disable go test ./...
package testdb

import (
	"context"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/JaipreethTiruvaipati/cabflow/internal/store"
)

// New creates a fresh, migrated database and drops it after the test.
func New(t testing.TB) string {
	t.Helper()
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	name := "tripora_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = admin.Close(ctx)
	})
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	dbURL := u.String()
	if err := store.New(Pool(t, dbURL, nil)).Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return dbURL
}

// Pool opens a pool for one fake node. Passing a gate lets the test freeze
// that node's DB access, which from the cluster's point of view looks the
// same as SIGSTOP (or kill -9, if we never resume it).
func Pool(t testing.TB, dbURL string, gate *Gate) *pgxpool.Pool {
	t.Helper()
	config, err := store.PoolConfig(dbURL, 10)
	if err != nil {
		t.Fatal(err)
	}
	if gate != nil {
		config.BeforeAcquire = func(ctx context.Context, _ *pgx.Conn) bool { return gate.wait(ctx) }
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

type Gate struct {
	mu     sync.Mutex
	frozen chan struct{}
}

// Freeze blocks the node's DB calls until Resume.
func (g *Gate) Freeze() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.frozen == nil {
		g.frozen = make(chan struct{})
	}
}

func (g *Gate) Resume() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.frozen != nil {
		close(g.frozen)
		g.frozen = nil
	}
}

func (g *Gate) Frozen() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.frozen != nil
}

func (g *Gate) wait(ctx context.Context) bool {
	g.mu.Lock()
	frozen := g.frozen
	g.mu.Unlock()
	if frozen == nil {
		return true
	}
	select {
	case <-frozen:
		return true
	case <-ctx.Done():
		return false
	}
}

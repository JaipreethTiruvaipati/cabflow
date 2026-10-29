// Package store is everything PostgreSQL. All time checks use the database
// clock, so a node with a skewed clock can't fire early or steal a lease.
package store

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/JaipreethTiruvaipati/cabflow/internal/domain"
)

//go:embed schema.sql
var schema string

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// PoolConfig kills idle transactions after 5s. Otherwise a frozen node could
// sit on row locks and block the takeover.
func PoolConfig(url string, maxConns int32) (*pgxpool.Config, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	config.MaxConns = maxConns
	config.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	config.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	return config, nil
}

// Migrate takes an advisory lock because three nodes booting at once will
// otherwise race on CREATE TABLE.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(4242)`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, schema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Now returns the database clock, not the node's.
func (s *Store) Now(ctx context.Context) (time.Time, error) {
	var now time.Time
	err := s.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)
	return now.UTC(), err
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func notFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	return err
}

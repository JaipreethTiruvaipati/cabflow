package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// Leases maps shard to epoch. The epoch is our fencing token.
type Leases map[int]int64

func (s *Store) Heartbeat(ctx context.Context, node string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO nodes (id, heartbeat_at) VALUES ($1, clock_timestamp())
		ON CONFLICT (id) DO UPDATE SET heartbeat_at = clock_timestamp()`, node)
	return err
}

func (s *Store) LiveNodes(ctx context.Context, ttl time.Duration) (int, error) {
	var count int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM nodes WHERE heartbeat_at > clock_timestamp() - make_interval(secs => $1)`,
		ttl.Seconds()).Scan(&count)
	return count, err
}

// RenewLeases deliberately won't revive an expired lease, even if nobody took
// it. We have to re-acquire it, which bumps the epoch and fences off anything
// we started before the pause.
func (s *Store) RenewLeases(ctx context.Context, node string, ttl time.Duration) (Leases, error) {
	rows, err := s.pool.Query(ctx, `UPDATE shard_leases SET expires_at = clock_timestamp() + make_interval(secs => $2)
		WHERE owner = $1 AND expires_at > clock_timestamp() RETURNING shard, epoch`, node, ttl.Seconds())
	if err != nil {
		return nil, err
	}
	return collectLeases(rows)
}

// AcquireLeases uses SKIP LOCKED so nodes starting together don't queue up
// behind each other.
func (s *Store) AcquireLeases(ctx context.Context, node string, n int, ttl time.Duration) (Leases, error) {
	rows, err := s.pool.Query(ctx, `UPDATE shard_leases SET owner = $1, epoch = epoch + 1,
			expires_at = clock_timestamp() + make_interval(secs => $3)
		WHERE shard IN (
			SELECT shard FROM shard_leases WHERE owner IS NULL OR expires_at <= clock_timestamp()
			ORDER BY random() LIMIT $2 FOR UPDATE SKIP LOCKED)
		RETURNING shard, epoch`, node, n, ttl.Seconds())
	if err != nil {
		return nil, err
	}
	return collectLeases(rows)
}

// ReleaseLeases expires the lease now instead of making the next owner wait
// out the TTL.
func (s *Store) ReleaseLeases(ctx context.Context, node string, shards []int) error {
	_, err := s.pool.Exec(ctx, `UPDATE shard_leases SET owner = NULL, expires_at = clock_timestamp()
		WHERE owner = $1 AND shard = ANY($2)`, node, shards)
	return err
}

func collectLeases(rows pgx.Rows) (Leases, error) {
	leases := make(Leases)
	var shard int
	var epoch int64
	_, err := pgx.ForEachRow(rows, []any{&shard, &epoch}, func() error {
		leases[shard] = epoch
		return nil
	})
	return leases, err
}

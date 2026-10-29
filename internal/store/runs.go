package store

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/JaipreethTiruvaipati/cabflow/internal/domain"
)

// ClaimedRun is a run this node now owns, plus the job fields needed to run it.
type ClaimedRun struct {
	domain.Run
	Job       domain.Job
	Fence     int64     // shard epoch at claim time; finishing requires it to be current
	ClaimedAt time.Time // database clock at claim time
}

// Planner is called inside the claim tx: should this occurrence run (misfire
// policy), and when is the next one?
type Planner func(ClaimedRun) (run bool, next time.Time, err error)

// Claim grabs up to limit due runs from the shards we lease, at most
// perTenant per tenant, skipping paused tenants and tenants at their cap.
//
// On a run's first attempt we also insert the job's next occurrence here, in
// the same tx. If we did it later, a crash in between would silently end a
// recurring schedule.
func (s *Store) Claim(ctx context.Context, node string, limit, perTenant int, plan Planner) (runs []ClaimedRun, skipped, throttled int, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, 0, 0, err
	}
	defer tx.Rollback(ctx)

	// Only shards we still lease. FOR SHARE makes a takeover wait for us.
	rows, err := tx.Query(ctx, `SELECT shard, epoch FROM shard_leases
		WHERE owner = $1 AND expires_at > clock_timestamp() FOR SHARE`, node)
	if err != nil {
		return nil, 0, 0, err
	}
	leases, err := collectLeases(rows)
	if err != nil || len(leases) == 0 {
		return nil, 0, 0, err
	}
	shards := make([]int, 0, len(leases))
	for shard := range leases {
		shards = append(shards, shard)
	}

	candidates, err := dueCandidates(ctx, tx, shards, limit, perTenant)
	if err != nil || len(candidates) == 0 {
		return nil, 0, 0, err
	}
	ids, throttled, err := applyTenantCaps(ctx, tx, candidates)
	if err != nil || len(ids) == 0 {
		return nil, 0, throttled, err
	}

	claimed, err := markRunning(ctx, tx, node, ids)
	if err != nil {
		return nil, 0, 0, err
	}

	ready := make([]ClaimedRun, 0, len(claimed))
	batch := &pgx.Batch{}
	for _, run := range claimed {
		if run.Manual || run.Attempt > 1 {
			ready = append(ready, run)
			continue
		}
		shouldRun, next, err := plan(run)
		if err != nil {
			return nil, 0, 0, err
		}
		if !next.IsZero() {
			batch.Queue(`INSERT INTO runs (id, job_id, tenant_id, shard, priority, scheduled_at, idempotency_key, status, next_attempt_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, 'PENDING', $6) ON CONFLICT (idempotency_key) DO NOTHING`,
				uuid.New(), run.Job.ID, run.TenantID, run.Job.Shard, run.Job.Priority, next, domain.OccurrenceKey(run.Job.ID, next))
		}
		if !shouldRun {
			batch.Queue(`UPDATE runs SET status = 'SKIPPED', finished_at = clock_timestamp(), owner = NULL, fence = NULL,
				lease_until = NULL, last_error = 'MISFIRE_SKIPPED' WHERE id = $1`, run.ID)
			skipped++
			continue
		}
		ready = append(ready, run)
	}
	if batch.Len() > 0 {
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return nil, 0, 0, err
		}
	}
	return ready, skipped, throttled, tx.Commit(ctx)
}

type candidate struct {
	id     uuid.UUID
	tenant uuid.UUID
}

// dueCandidates interleaves tenants (everyone's 1st run, then everyone's 2nd)
// so a tenant with 50k due alerts can't push a small tenant to the back.
func dueCandidates(ctx context.Context, tx pgx.Tx, shards []int, limit, perTenant int) ([]candidate, error) {
	rows, err := tx.Query(ctx, `WITH due AS (
			SELECT r.id, r.tenant_id,
				row_number() OVER (PARTITION BY r.tenant_id ORDER BY r.priority DESC, r.next_attempt_at) AS turn
			FROM runs r JOIN tenants t ON t.id = r.tenant_id
			WHERE r.shard = ANY($1) AND r.status IN ('PENDING', 'RETRYING')
				AND r.next_attempt_at <= clock_timestamp() AND NOT t.paused)
		SELECT id, tenant_id FROM due WHERE turn <= $2 ORDER BY turn, random() LIMIT $3`, shards, perTenant, limit)
	if err != nil {
		return nil, err
	}
	var result []candidate
	var c candidate
	_, err = pgx.ForEachRow(rows, []any{&c.id, &c.tenant}, func() error {
		result = append(result, c)
		return nil
	})
	return result, err
}

// applyTenantCaps trims candidates to each tenant's free slots. Without the
// advisory locks two nodes could both see a free slot and overshoot the cap.
// They're taken in hash order so nodes never deadlock on each other.
func applyTenantCaps(ctx context.Context, tx pgx.Tx, candidates []candidate) ([]uuid.UUID, int, error) {
	seen := make(map[uuid.UUID]bool)
	var tenants []uuid.UUID
	for _, c := range candidates {
		if !seen[c.tenant] {
			seen[c.tenant] = true
			tenants = append(tenants, c.tenant)
		}
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(k) FROM (
			SELECT DISTINCT hashtextextended(t::text, 0) AS k FROM unnest($1::uuid[]) AS t ORDER BY k) locks`, tenants); err != nil {
		return nil, 0, err
	}
	rows, err := tx.Query(ctx, `SELECT t.id, t.max_running -
			(SELECT count(*) FROM runs r WHERE r.tenant_id = t.id AND r.status = 'RUNNING')
		FROM tenants t WHERE t.id = ANY($1)`, tenants)
	if err != nil {
		return nil, 0, err
	}
	free := make(map[uuid.UUID]int)
	var tenant uuid.UUID
	var slots int
	if _, err := pgx.ForEachRow(rows, []any{&tenant, &slots}, func() error {
		free[tenant] = slots
		return nil
	}); err != nil {
		return nil, 0, err
	}
	ids, throttled := pickWithinCaps(candidates, free)
	return ids, throttled, nil
}

// pickWithinCaps is kept free of SQL on purpose so it can be tested without
// a database. Order is preserved; throttled counts what a full tenant lost.
func pickWithinCaps(candidates []candidate, free map[uuid.UUID]int) (ids []uuid.UUID, throttled int) {
	left := make(map[uuid.UUID]int, len(free))
	for tenant, slots := range free {
		left[tenant] = slots
	}
	for _, c := range candidates {
		if left[c.tenant] > 0 {
			left[c.tenant]--
			ids = append(ids, c.id)
		} else {
			throttled++
		}
	}
	return ids, throttled
}

func markRunning(ctx context.Context, tx pgx.Tx, node string, ids []uuid.UUID) ([]ClaimedRun, error) {
	rows, err := tx.Query(ctx, `UPDATE runs r SET status = 'RUNNING', attempt = r.attempt + 1, owner = $1, fence = l.epoch,
			started_at = COALESCE(r.started_at, clock_timestamp()),
			lease_until = clock_timestamp() + make_interval(secs => j.max_runtime_ms / 1000.0 + 5)
		FROM shard_leases l, jobs j
		WHERE r.id = ANY($2) AND r.status IN ('PENDING', 'RETRYING') AND l.shard = r.shard AND j.id = r.job_id
		RETURNING r.id, r.job_id, r.tenant_id, r.scheduled_at, r.idempotency_key, r.manual, r.status, r.attempt,
			r.next_attempt_at, r.started_at, l.epoch, clock_timestamp(),
			j.type, j.payload, COALESCE(j.cron, ''), j.timezone, j.misfire_policy, j.priority, j.max_runtime_ms, j.shard`, node, ids)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (ClaimedRun, error) {
		var c ClaimedRun
		err := row.Scan(&c.ID, &c.JobID, &c.TenantID, &c.ScheduledAt, &c.IdempotencyKey, &c.Manual, &c.Status, &c.Attempt,
			&c.NextAttemptAt, &c.StartedAt, &c.Fence, &c.ClaimedAt,
			&c.Job.Type, &c.Job.Payload, &c.Job.Cron, &c.Job.Timezone, &c.Job.MisfirePolicy, &c.Job.Priority, &c.Job.MaxRuntimeMS, &c.Job.Shard)
		c.Job.ID, c.Job.TenantID = c.JobID, c.TenantID
		return c, err
	})
}

// Outcome is how an attempt ended.
type Outcome struct {
	Status  domain.RunStatus // SUCCEEDED, RETRYING or DEAD
	RetryIn time.Duration
	Error   string
}

// Finish is the fenced write. If our lease expired or the shard moved to a
// newer epoch, nothing is written and we get ErrStaleLease. That's what stops
// a node waking up from a long GC pause from overwriting someone else's work.
func (s *Store) Finish(ctx context.Context, node string, run ClaimedRun, outcome Outcome) error {
	tag, err := s.pool.Exec(ctx, `WITH lease AS (
			SELECT 1 FROM shard_leases WHERE shard = $1 AND owner = $2 AND epoch = $3 AND expires_at > clock_timestamp() FOR SHARE)
		UPDATE runs SET status = $5::text, owner = NULL, fence = NULL, lease_until = NULL,
			finished_at = CASE WHEN $5::text IN ('SUCCEEDED', 'DEAD') THEN clock_timestamp() END,
			next_attempt_at = CASE WHEN $5::text = 'RETRYING' THEN clock_timestamp() + make_interval(secs => $6) ELSE next_attempt_at END,
			last_error = NULLIF($7, '')
		WHERE id = $4 AND status = 'RUNNING' AND owner = $2 AND fence = $3 AND attempt = $8 AND EXISTS (SELECT 1 FROM lease)`,
		run.Job.Shard, node, run.Fence, run.ID, string(outcome.Status), outcome.RetryIn.Seconds(), outcome.Error, run.Attempt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrStaleLease
	}
	return nil
}

// Recover picks up runs orphaned on our shards: claimed under an older epoch,
// or stuck past max runtime plus a few seconds.
func (s *Store) Recover(ctx context.Context, node string, maxAttempts int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE runs r SET
			status = CASE WHEN r.attempt >= $2 THEN 'DEAD' ELSE 'RETRYING' END,
			finished_at = CASE WHEN r.attempt >= $2 THEN clock_timestamp() END,
			next_attempt_at = clock_timestamp() + interval '1 second',
			last_error = 'LEASE_LOST', owner = NULL, fence = NULL, lease_until = NULL
		FROM shard_leases l
		WHERE l.owner = $1 AND l.expires_at > clock_timestamp() AND r.shard = l.shard AND r.status = 'RUNNING'
			AND (r.fence < l.epoch OR r.lease_until < clock_timestamp())`, node, maxAttempts)
	return tag.RowsAffected(), err
}

// Stats feeds the /metrics gauges. Counts are cluster-wide.
type Stats struct{ Due, Running, Retrying, Dead, Paused int64 }

func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var stats Stats
	err := s.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM runs WHERE status IN ('PENDING', 'RETRYING') AND next_attempt_at <= clock_timestamp()),
		(SELECT count(*) FROM runs WHERE status = 'RUNNING'),
		(SELECT count(*) FROM runs WHERE status = 'RETRYING'),
		(SELECT count(*) FROM runs WHERE status = 'DEAD'),
		(SELECT count(*) FROM runs r JOIN tenants t ON t.id = r.tenant_id WHERE t.paused
			AND r.status IN ('PENDING', 'RETRYING') AND r.next_attempt_at <= clock_timestamp())`).Scan(
		&stats.Due, &stats.Running, &stats.Retrying, &stats.Dead, &stats.Paused)
	return stats, err
}

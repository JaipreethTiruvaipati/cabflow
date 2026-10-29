package store

import (
	"context"
	"errors"
	"time"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/JaipreethTiruvaipati/cabflow/internal/domain"
)

const jobColumns = `id, tenant_id, type, payload, COALESCE(cron, ''), timezone, misfire_policy, priority, max_runtime_ms, shard, created_at`

const runColumns = `id, job_id, tenant_id, scheduled_at, idempotency_key, manual, status, attempt,
	next_attempt_at, started_at, finished_at, COALESCE(last_error, '')`

// CreateJob writes the job and its first run in one go.
func (s *Store) CreateJob(ctx context.Context, job domain.Job, firstRunAt time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO tenants (id) VALUES ($1) ON CONFLICT DO NOTHING`, job.TenantID); err != nil {
		return err
	}
	var cron any
	if job.Cron != "" {
		cron = job.Cron
	}
	if _, err := tx.Exec(ctx, `INSERT INTO jobs (id, tenant_id, type, payload, cron, timezone, misfire_policy, priority, max_runtime_ms, shard)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		job.ID, job.TenantID, job.Type, job.Payload, cron, job.Timezone, job.MisfirePolicy, job.Priority, job.MaxRuntimeMS, job.Shard); err != nil {
		return err
	}
	if err := insertRun(ctx, tx, job, firstRunAt, domain.OccurrenceKey(job.ID, firstRunAt), false); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func insertRun(ctx context.Context, tx pgx.Tx, job domain.Job, at time.Time, key string, manual bool) error {
	_, err := tx.Exec(ctx, `INSERT INTO runs (id, job_id, tenant_id, shard, priority, scheduled_at, idempotency_key, manual, status, next_attempt_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'PENDING', $6)
		ON CONFLICT (idempotency_key) DO NOTHING`,
		uuid.New(), job.ID, job.TenantID, job.Shard, job.Priority, at.UTC(), key, manual)
	return err
}

func (s *Store) GetJob(ctx context.Context, tenantID, jobID uuid.UUID) (domain.Job, error) {
	return scanJob(s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1 AND tenant_id = $2`, jobID, tenantID))
}

func (s *Store) ListJobs(ctx context.Context, tenantID uuid.UUID) ([]domain.Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+jobColumns+` FROM jobs WHERE tenant_id = $1 ORDER BY created_at LIMIT 500`, tenantID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Job, error) { return scanJob(row) })
}

func (s *Store) ListRuns(ctx context.Context, tenantID, jobID uuid.UUID, limit int) ([]domain.Run, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+runColumns+` FROM runs WHERE job_id = $1 AND tenant_id = $2
		ORDER BY scheduled_at DESC LIMIT $3`, jobID, tenantID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.Run, error) { return scanRun(row) })
}

// NextScheduled ignores manual runs; those aren't part of the schedule.
func (s *Store) NextScheduled(ctx context.Context, jobID uuid.UUID) (time.Time, bool, error) {
	var at time.Time
	err := s.pool.QueryRow(ctx, `SELECT next_attempt_at FROM runs WHERE job_id = $1 AND NOT manual
		AND status IN ('PENDING', 'RETRYING') ORDER BY next_attempt_at LIMIT 1`, jobID).Scan(&at)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	return at.UTC(), err == nil, err
}

// Trigger is idempotent per client key, so a double-clicked button or a
// retried request gets the same run back.
func (s *Store) Trigger(ctx context.Context, tenantID, jobID uuid.UUID, clientKey string) (domain.Run, error) {
	job, err := s.GetJob(ctx, tenantID, jobID)
	if err != nil {
		return domain.Run{}, err
	}
	key := domain.ManualKey(job.ID, clientKey)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.Run{}, err
	}
	defer tx.Rollback(ctx)
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return domain.Run{}, err
	}
	if err := insertRun(ctx, tx, job, now, key, true); err != nil {
		return domain.Run{}, err
	}
	run, err := scanRun(tx.QueryRow(ctx, `SELECT `+runColumns+` FROM runs WHERE idempotency_key = $1`, key))
	if err != nil {
		return domain.Run{}, err
	}
	return run, tx.Commit(ctx)
}

func (s *Store) SetPaused(ctx context.Context, tenantID uuid.UUID, paused bool) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO tenants (id, paused) VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET paused = EXCLUDED.paused`, tenantID, paused)
	return err
}

func (s *Store) SetMaxRunning(ctx context.Context, tenantID uuid.UUID, limit int) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO tenants (id, max_running) VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET max_running = EXCLUDED.max_running`, tenantID, limit)
	return err
}

type scanner interface{ Scan(dest ...any) error }

func scanJob(row scanner) (domain.Job, error) {
	var job domain.Job
	err := row.Scan(&job.ID, &job.TenantID, &job.Type, &job.Payload, &job.Cron, &job.Timezone, &job.MisfirePolicy,
		&job.Priority, &job.MaxRuntimeMS, &job.Shard, &job.CreatedAt)
	return job, notFound(err)
}

func scanRun(row scanner) (domain.Run, error) {
	var run domain.Run
	err := row.Scan(&run.ID, &run.JobID, &run.TenantID, &run.ScheduledAt, &run.IdempotencyKey, &run.Manual, &run.Status,
		&run.Attempt, &run.NextAttemptAt, &run.StartedAt, &run.FinishedAt, &run.LastError)
	return run, notFound(err)
}


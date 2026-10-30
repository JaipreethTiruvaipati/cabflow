// Package service sits between the HTTP handlers and the store. Validation,
// defaults and tenant scoping live here so the handlers stay dumb.
package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/JaipreethTiruvaipati/cabflow/internal/domain"
	"github.com/JaipreethTiruvaipati/cabflow/internal/schedule"
	"github.com/JaipreethTiruvaipati/cabflow/internal/store"
)

type CreateJob struct {
	Type          string               `json:"type"`
	Payload       json.RawMessage      `json:"payload"`
	Cron          string               `json:"cron"`
	RunAt         *time.Time           `json:"run_at"`
	Timezone      string               `json:"timezone"`
	MisfirePolicy domain.MisfirePolicy `json:"misfire_policy"`
	Priority      int                  `json:"priority"`
	MaxRuntimeMS  int                  `json:"max_runtime_ms"`
}

// Repository is what we need from storage. *store.Store is the real one.
type Repository interface {
	Now(ctx context.Context) (time.Time, error)
	CreateJob(ctx context.Context, job domain.Job, firstRunAt time.Time) error
	GetJob(ctx context.Context, tenantID, jobID uuid.UUID) (domain.Job, error)
	ListJobs(ctx context.Context, tenantID uuid.UUID) ([]domain.Job, error)
	ListRuns(ctx context.Context, tenantID, jobID uuid.UUID, limit int) ([]domain.Run, error)
	NextScheduled(ctx context.Context, jobID uuid.UUID) (time.Time, bool, error)
	Trigger(ctx context.Context, tenantID, jobID uuid.UUID, clientKey string) (domain.Run, error)
	SetPaused(ctx context.Context, tenantID uuid.UUID, paused bool) error
	SetMaxRunning(ctx context.Context, tenantID uuid.UUID, limit int) error
	DeadLetters(ctx context.Context, tenantID uuid.UUID, limit int) ([]domain.Run, error)
	Replay(ctx context.Context, tenantID, runID uuid.UUID) error
}

var _ Repository = (*store.Store)(nil)

type Jobs struct {
	db   Repository
	calc *schedule.Calculator
	// KnownType lets us reject a job nobody has a handler for.
	KnownType func(string) bool
}

func NewJobs(db Repository, knownType func(string) bool) *Jobs {
	return &Jobs{db: db, calc: schedule.NewCalculator(), KnownType: knownType}
}

func (s *Jobs) Create(ctx context.Context, tenantID uuid.UUID, req CreateJob) (domain.Job, error) {
	job := domain.Job{
		ID: uuid.New(), TenantID: tenantID, Type: req.Type, Payload: req.Payload, Cron: req.Cron,
		Timezone: req.Timezone, MisfirePolicy: req.MisfirePolicy, Priority: req.Priority, MaxRuntimeMS: req.MaxRuntimeMS,
	}
	if job.Timezone == "" {
		job.Timezone = "UTC"
	}
	if job.MisfirePolicy == "" {
		job.MisfirePolicy = domain.Coalesce
	}
	if job.MaxRuntimeMS == 0 {
		job.MaxRuntimeMS = 30_000
	}
	if err := s.validate(job, req.RunAt); err != nil {
		return domain.Job{}, err
	}
	job.Shard = domain.ShardFor(job.ID)

	now, err := s.db.Now(ctx)
	if err != nil {
		return domain.Job{}, err
	}
	first := now
	switch {
	case job.Recurring():
		if first, err = s.calc.Next(now, job.Cron, job.Timezone); err != nil {
			return domain.Job{}, err
		}
	case req.RunAt != nil:
		first = req.RunAt.UTC()
	}
	if err := s.db.CreateJob(ctx, job, first); err != nil {
		return domain.Job{}, err
	}
	return s.db.GetJob(ctx, tenantID, job.ID)
}

func (s *Jobs) validate(job domain.Job, runAt *time.Time) error {
	switch {
	case s.KnownType != nil && !s.KnownType(job.Type):
		return domain.Invalid("unknown job type %q", job.Type)
	case len(job.Payload) == 0 || !json.Valid(job.Payload):
		return domain.Invalid("payload must be JSON")
	case job.Cron != "" && runAt != nil:
		return domain.Invalid("set either cron or run_at, not both")
	case job.MaxRuntimeMS < 100 || job.MaxRuntimeMS > 3_600_000:
		return domain.Invalid("max_runtime_ms must be between 100 and 3600000")
	case job.Priority < -100 || job.Priority > 100:
		return domain.Invalid("priority must be between -100 and 100")
	}
	switch job.MisfirePolicy {
	case domain.FireNow, domain.Coalesce, domain.Skip:
	default:
		return domain.Invalid("misfire_policy must be FIRE_NOW, COALESCE or SKIP")
	}
	return s.calc.Validate(job.Cron, job.Timezone)
}

// NextFireTimes starts from the stored next run, not from now, so a pending
// misfire still shows up first.
func (s *Jobs) NextFireTimes(ctx context.Context, tenantID, jobID uuid.UUID, limit int) ([]time.Time, error) {
	job, err := s.db.GetJob(ctx, tenantID, jobID)
	if err != nil {
		return nil, err
	}
	next, ok, err := s.db.NextScheduled(ctx, jobID)
	if err != nil || !ok {
		return []time.Time{}, err
	}
	return schedule.Upcoming(job, next, pageSize(limit, 10), s.calc)
}

func (s *Jobs) Get(ctx context.Context, tenantID, jobID uuid.UUID) (domain.Job, error) {
	return s.db.GetJob(ctx, tenantID, jobID)
}

func (s *Jobs) List(ctx context.Context, tenantID uuid.UUID) ([]domain.Job, error) {
	return s.db.ListJobs(ctx, tenantID)
}

func (s *Jobs) Runs(ctx context.Context, tenantID, jobID uuid.UUID, limit int) ([]domain.Run, error) {
	return s.db.ListRuns(ctx, tenantID, jobID, pageSize(limit, 20))
}

func (s *Jobs) Trigger(ctx context.Context, tenantID, jobID uuid.UUID, clientKey string) (domain.Run, error) {
	if clientKey == "" {
		clientKey = uuid.NewString()
	}
	return s.db.Trigger(ctx, tenantID, jobID, clientKey)
}

func (s *Jobs) Pause(ctx context.Context, tenantID uuid.UUID, paused bool) error {
	return s.db.SetPaused(ctx, tenantID, paused)
}

func (s *Jobs) SetMaxRunning(ctx context.Context, tenantID uuid.UUID, limit int) error {
	if limit < 1 || limit > 10_000 {
		return domain.Invalid("max_running must be between 1 and 10000")
	}
	return s.db.SetMaxRunning(ctx, tenantID, limit)
}

func (s *Jobs) DeadLetters(ctx context.Context, tenantID uuid.UUID, limit int) ([]domain.Run, error) {
	return s.db.DeadLetters(ctx, tenantID, pageSize(limit, 50))
}

func (s *Jobs) Replay(ctx context.Context, tenantID, runID uuid.UUID) error {
	return s.db.Replay(ctx, tenantID, runID)
}

// pageSize falls back to def for a missing or silly ?limit=.
func pageSize(requested, def int) int {
	if requested < 1 || requested > 100 {
		return def
	}
	return requested
}

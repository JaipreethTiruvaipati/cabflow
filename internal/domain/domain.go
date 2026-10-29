// Package domain has the core types plus the bits of logic that don't need
// a database (shard placement, idempotency keys, validation errors).
package domain

import (
	"encoding/json"
	"errors"
	"time"
	"github.com/google/uuid"
)

// ShardCount is fixed. Nodes own whole shards, so more than 64 nodes would
// just sit idle.
const ShardCount = 64

type MisfirePolicy string

const (
	FireNow  MisfirePolicy = "FIRE_NOW" // replay every missed run
	Coalesce MisfirePolicy = "COALESCE" // one run covers all the missed ones
	Skip     MisfirePolicy = "SKIP"     // drop them, wait for the next one
)

type RunStatus string

const (
	RunPending   RunStatus = "PENDING"
	RunRunning   RunStatus = "RUNNING"
	RunRetrying  RunStatus = "RETRYING"
	RunSucceeded RunStatus = "SUCCEEDED"
	RunDead      RunStatus = "DEAD"
	RunSkipped   RunStatus = "SKIPPED"
)

type Job struct {
	ID            uuid.UUID       `json:"id"`
	TenantID      uuid.UUID       `json:"tenant_id"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
	Cron          string          `json:"cron,omitempty"`
	Timezone      string          `json:"timezone"`
	MisfirePolicy MisfirePolicy   `json:"misfire_policy"`
	Priority      int             `json:"priority"`
	MaxRuntimeMS  int             `json:"max_runtime_ms"`
	Shard         int             `json:"shard"`
	CreatedAt     time.Time       `json:"created_at"`
}

func (j Job) Recurring() bool { return j.Cron != "" }

func (j Job) MaxRuntime() time.Duration { return time.Duration(j.MaxRuntimeMS) * time.Millisecond }

// Run is one occurrence of a job. Retries, and re-runs on another node,
// reuse the same row and therefore the same idempotency key.
type Run struct {
	ID             uuid.UUID  `json:"id"`
	JobID          uuid.UUID  `json:"job_id"`
	TenantID       uuid.UUID  `json:"tenant_id"`
	ScheduledAt    time.Time  `json:"scheduled_at"`
	IdempotencyKey string     `json:"idempotency_key"`
	Manual         bool       `json:"manual"`
	Status         RunStatus  `json:"status"`
	Attempt        int        `json:"attempt"`
	NextAttemptAt  time.Time  `json:"next_attempt_at"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
}

func ShardFor(id uuid.UUID) int {
	var hash uint64
	for _, b := range id {
		hash = hash*1099511628211 + uint64(b)
	}
	return int(hash % ShardCount)
}

var (
	ErrNotFound = errors.New("not found")
	// ErrStaleLease: we lost the shard while the run was in flight, so the
	// fencing check threw our write away. Someone else owns the run now.
	ErrStaleLease = errors.New("stale lease: write fenced off")
)

// ValidationError is the only error whose message goes back to the client.
type ValidationError struct{ Message string }


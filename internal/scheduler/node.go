// Package scheduler is what every node runs: grab a fair share of shards,
// then keep claiming and running whatever is due on them.
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"time"
	"github.com/JaipreethTiruvaipati/cabflow/internal/domain"
	"github.com/JaipreethTiruvaipati/cabflow/internal/metrics"
	"github.com/JaipreethTiruvaipati/cabflow/internal/notify"
	"github.com/JaipreethTiruvaipati/cabflow/internal/safelog"
	"github.com/JaipreethTiruvaipati/cabflow/internal/schedule"
	"github.com/JaipreethTiruvaipati/cabflow/internal/store"
)

type Config struct {
	NodeID       string
	LeaseTTL     time.Duration // how long a silent node keeps its shards
	PollInterval time.Duration // how often due runs are claimed
	Workers      int           // runs executing at once on this node
	PerTenant    int           // max runs one tenant gets per claim
	MaxAttempts  int           // attempts before a run is dead-lettered
	RetryBase    time.Duration
	RetryMax     time.Duration
}

func DefaultConfig(nodeID string) Config {
	return Config{
		NodeID:       nodeID,
		LeaseTTL:     10 * time.Second,
		PollInterval: 100 * time.Millisecond,
		Workers:      64,
		PerTenant:    20,
		MaxAttempts:  5,
		RetryBase:    time.Second,
		RetryMax:     5 * time.Minute,
	}
}

// Handler runs one job. Respect ctx (that's the max runtime) and send
// run.IdempotencyKey with anything that leaves the process.
type Handler func(ctx context.Context, run store.ClaimedRun) error

type Node struct {
	cfg      Config
	db       *store.Store
	calc     *schedule.Calculator
	metrics  *metrics.Metrics
	log      *slog.Logger
	handlers map[string]Handler

	slots    chan struct{}
	inflight sync.WaitGroup
	mu       sync.Mutex
	busy     map[int]int // runs in flight per shard
	leases   store.Leases
}

func New(cfg Config, db *store.Store, sender notify.Sender, m *metrics.Metrics, log *slog.Logger) *Node {
	n := &Node{
		cfg: cfg, db: db, calc: schedule.NewCalculator(), metrics: m, log: log,
		handlers: make(map[string]Handler),
		slots:    make(chan struct{}, cfg.Workers),
		busy:     make(map[int]int),
	}
	// All the built-in types just send a notification keyed by the run. If a
	// run gets retried or redone elsewhere, the receiver drops the repeat.
	send := func(ctx context.Context, run store.ClaimedRun) error {
		return sender.Send(ctx, notify.Notification{Key: run.IdempotencyKey, TenantID: run.TenantID, Type: run.Job.Type, Payload: run.Job.Payload})
	}
	for _, jobType := range []string{"alert", "roster", "no_show", "billing"} {
		n.handlers[jobType] = send
	}
	n.handlers["noop"] = func(context.Context, store.ClaimedRun) error { return nil }
	return n
}

// Handle swaps in a handler for a job type. Call it before Run.
func (n *Node) Handle(jobType string, h Handler) { n.handlers[jobType] = h }

// KnownType lets the API reject job types we have no handler for.
func (n *Node) KnownType(jobType string) bool { _, ok := n.handlers[jobType]; return ok }

func (n *Node) execute(run store.ClaimedRun) {
	defer func() {
		n.mu.Lock()
		n.busy[run.Job.Shard]--
		n.mu.Unlock()
		<-n.slots
		n.inflight.Done()
	}()
	if run.Attempt == 1 && run.StartedAt != nil && !run.Manual {
		n.metrics.ObserveFireLag(run.StartedAt.Sub(run.ScheduledAt).Seconds())
	}
	// Not tied to the node's ctx on purpose: during shutdown we'd rather let the
	// run finish than burn the attempt.
	ctx, cancel := context.WithTimeout(context.Background(), run.Job.MaxRuntime())
	err := n.call(ctx, run)
	cancel()

	outcome := n.outcome(run, err)
	finishCtx, cancelFinish := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelFinish()
	switch err := n.db.Finish(finishCtx, n.cfg.NodeID, run, outcome); {
	case errors.Is(err, domain.ErrStaleLease):
		n.metrics.Stale.Add(1)
		n.log.Warn("stale write rejected: shard moved to another owner", "run_id", run.ID, "fence", run.Fence)
	case err != nil:
		n.fail("finish run", err)
	case outcome.Status == domain.RunSucceeded:
		n.metrics.Succeeded.Add(1)
	case outcome.Status == domain.RunRetrying:
		n.metrics.Retried.Add(1)
		n.log.Info("run will retry", "run_id", run.ID, "attempt", run.Attempt, "error", outcome.Error, "retry_in", outcome.RetryIn)
	default:
		n.metrics.DeadLetters.Add(1)
		n.log.Warn("run dead-lettered", "run_id", run.ID, "attempt", run.Attempt, "error", outcome.Error)
	}
}

func (n *Node) call(ctx context.Context, run store.ClaimedRun) (err error) {
	handler, ok := n.handlers[run.Job.Type]
	if !ok {
		return errUnknownType
	}
	defer func() {
		if p := recover(); p != nil {
			// Stack yes, payload no.
			n.log.Error("handler panic", "run_id", run.ID, "job_type", run.Job.Type, "stack", string(debug.Stack()))
			err = errors.New("handler panic")
		}
	}()
	return handler(ctx, run)
}

var errUnknownType = errors.New("unknown job type")

// outcome decides what happens after an attempt. Failures back off
// exponentially (full jitter) until MaxAttempts, then land in dead letters.
func (n *Node) outcome(run store.ClaimedRun, err error) store.Outcome {
	switch {
	case err == nil:
		return store.Outcome{Status: domain.RunSucceeded}
	case errors.Is(err, errUnknownType):
		return store.Outcome{Status: domain.RunDead, Error: "UNKNOWN_TYPE"}
	case run.Attempt >= n.cfg.MaxAttempts:
		return store.Outcome{Status: domain.RunDead, Error: errorCode(err)}
	}
	ceiling := n.cfg.RetryBase << (run.Attempt - 1)
	if ceiling <= 0 || ceiling > n.cfg.RetryMax {
		ceiling = n.cfg.RetryMax
	}
	return store.Outcome{Status: domain.RunRetrying, RetryIn: rand.N(ceiling) + time.Millisecond, Error: errorCode(err)}
}

// errorCode stores a short code instead of err.Error(), which could contain
// customer data.
func errorCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "TIMEOUT"
	}
	return "HANDLER_ERROR"
}

// fail bumps the per-operation error counter and logs a scrubbed version of
// the error (see safelog).
func (n *Node) fail(op string, err error) {
	n.metrics.Error(op)
	n.log.Error(op+" failed", "error", safelog.Error(err))
}


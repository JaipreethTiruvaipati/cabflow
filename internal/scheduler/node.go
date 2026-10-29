// Package scheduler is what every node runs: grab a fair share of shards,
// then keep claiming and running whatever is due on them.
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"math/rand/v2"
	"runtime/debug"
	"slices"
	"sort"
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

// Run blocks until ctx is done. On the way out it waits for in-flight runs and
// gives its shards back, so nobody has to sit out the lease timeout.
func (n *Node) Run(ctx context.Context) {
	n.maintainLeases(ctx)
	poll := time.NewTicker(n.cfg.PollInterval)
	renew := time.NewTicker(n.cfg.LeaseTTL / 5)
	defer poll.Stop()
	defer renew.Stop()
	for {
		select {
		case <-ctx.Done():
			n.shutdown()
			return
		case <-renew.C:
			n.maintainLeases(ctx)
		case <-poll.C:
			n.claimAndRun(ctx)
		}
	}
}

// maintainLeases is the membership logic. Every node aims for
// ceil(64 / live nodes) shards. A new node picks up free ones while the others
// drop their extras, and if a node dies its leases simply time out and get
// taken.
func (n *Node) maintainLeases(ctx context.Context) {
	if err := n.db.Heartbeat(ctx, n.cfg.NodeID); err != nil {
		n.fail("heartbeat", err)
		return
	}
	owned, err := n.db.RenewLeases(ctx, n.cfg.NodeID, n.cfg.LeaseTTL)
	if err != nil {
		n.fail("renew leases", err)
		return
	}
	live, err := n.db.LiveNodes(ctx, n.cfg.LeaseTTL)
	if err != nil {
		n.fail("count nodes", err)
		return
	}
	live = max(live, 1) // guard against dividing by zero
	share := (domain.ShardCount + live - 1) / live
	switch {
	case len(owned) > share:
		extra := n.surplus(owned, len(owned)-share)
		if err := n.db.ReleaseLeases(ctx, n.cfg.NodeID, extra); err != nil {
			n.fail("release leases", err)
		}
		for _, shard := range extra {
			delete(owned, shard)
		}
	case len(owned) < share:
		gained, err := n.db.AcquireLeases(ctx, n.cfg.NodeID, share-len(owned), n.cfg.LeaseTTL)
		if err != nil {
			n.fail("acquire leases", err)
		}
		for shard, epoch := range gained {
			owned[shard] = epoch
		}
	}
	n.mu.Lock()
	n.leases = owned
	n.mu.Unlock()
	n.metrics.OwnedShards.Store(int64(len(owned)))

	recovered, err := n.db.Recover(ctx, n.cfg.NodeID, n.cfg.MaxAttempts)
	if err != nil {
		n.fail("recover runs", err)
	} else if recovered > 0 {
		n.metrics.Recovered.Add(recovered)
		n.log.Warn("recovered runs from a lost owner", "count", recovered)
	}
}

// surplus picks which shards to give away. Idle ones first, so less work gets
// redone by whoever takes them.
func (n *Node) surplus(owned store.Leases, count int) []int {
	n.mu.Lock()
	defer n.mu.Unlock()
	shards := slices.Collect(maps.Keys(owned))
	sort.Slice(shards, func(i, j int) bool { return n.busy[shards[i]] < n.busy[shards[j]] })
	return shards[:count]
}

func (n *Node) claimAndRun(ctx context.Context) {
	free := cap(n.slots) - len(n.slots)
	if free == 0 {
		return
	}
	runs, skipped, throttled, err := n.db.Claim(ctx, n.cfg.NodeID, free, n.cfg.PerTenant, n.plan)
	if err != nil {
		n.fail("claim runs", err)
		return
	}
	n.metrics.Skipped.Add(int64(skipped))
	n.metrics.Throttled.Add(int64(throttled))
	for _, run := range runs {
		n.metrics.Claimed.Add(1)
		n.slots <- struct{}{}
		n.mu.Lock()
		n.busy[run.Job.Shard]++
		n.mu.Unlock()
		n.inflight.Add(1)
		go n.execute(run)
	}
}

// plan runs inside the claim transaction. ClaimedAt comes from the DB clock,
// which is the one we trust for "is this late".
func (n *Node) plan(run store.ClaimedRun) (bool, time.Time, error) {
	decision, err := schedule.Decide(run.Job, run.ScheduledAt, run.ClaimedAt, n.calc)
	return decision.Run, decision.Next, err
}

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

func (n *Node) shutdown() {
	done := make(chan struct{})
	go func() { n.inflight.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		n.log.Warn("shutdown: some runs still in flight; they will be recovered by another node")
	}
	n.mu.Lock()
	shards := slices.Collect(maps.Keys(n.leases))
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.db.ReleaseLeases(ctx, n.cfg.NodeID, shards); err != nil {
		n.fail("release leases on shutdown", err)
	}
}

// fail bumps the per-operation error counter and logs a scrubbed version of
// the error (see safelog).
func (n *Node) fail(op string, err error) {
	n.metrics.Error(op)
	n.log.Error(op+" failed", "error", safelog.Error(err))
}

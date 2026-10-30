package scheduler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/JaipreethTiruvaipati/cabflow/internal/domain"
	"github.com/JaipreethTiruvaipati/cabflow/internal/metrics"
	"github.com/JaipreethTiruvaipati/cabflow/internal/notify"
	"github.com/JaipreethTiruvaipati/cabflow/internal/scheduler"
	"github.com/JaipreethTiruvaipati/cabflow/internal/service"
	"github.com/JaipreethTiruvaipati/cabflow/internal/store"
	"github.com/JaipreethTiruvaipati/cabflow/internal/testdb"
)

const leaseTTL = time.Second

// receiver is a fake idempotent notification service shared by the nodes.
// It counts every key and remembers the order of first deliveries.
type receiver struct {
	mu    sync.Mutex
	seen  map[string]int
	order []uuid.UUID // tenant of each first delivery
}

func newReceiver() *receiver { return &receiver{seen: map[string]int{}} }

func (r *receiver) Send(_ context.Context, n notify.Notification) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen[n.Key]++
	if r.seen[n.Key] == 1 {
		r.order = append(r.order, n.TenantID)
	}
	return nil
}

func (r *receiver) delivered() (unique, repeats int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, n := range r.seen {
		repeats += n - 1
	}
	return len(r.seen), repeats
}

// gatedSender blocks while its node is frozen, like a stopped process would.
// With freezeAfter set, the node freezes itself partway through a send, so it
// still owns unfinished runs when it goes dark.
type gatedSender struct {
	inner       notify.Sender
	gate        *testdb.Gate
	freezeAfter *atomic.Int64
	frozenAt    *atomic.Int64
}

func (s gatedSender) Send(ctx context.Context, n notify.Notification) error {
	if s.freezeAfter.Add(-1) == 0 {
		s.frozenAt.Store(time.Now().UnixNano())
		s.gate.Freeze()
	}
	for s.gate.Frozen() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	return s.inner.Send(ctx, n)
}

type testNode struct {
	node        *scheduler.Node
	metrics     *metrics.Metrics
	gate        *testdb.Gate
	freezeAfter *atomic.Int64 // arm with Store(n) to freeze on the n-th send
	frozenAt    *atomic.Int64
}

func startNode(t *testing.T, ctx context.Context, dbURL, id string, sender notify.Sender, tweak func(*scheduler.Config), handlers map[string]scheduler.Handler) testNode {
	t.Helper()
	gate := &testdb.Gate{}
	cfg := scheduler.DefaultConfig(id)
	cfg.LeaseTTL, cfg.PollInterval, cfg.RetryBase, cfg.RetryMax = leaseTTL, 50*time.Millisecond, 10*time.Millisecond, 50*time.Millisecond
	if tweak != nil {
		tweak(&cfg)
	}
	m := &metrics.Metrics{}
	freezeAfter, frozenAt := &atomic.Int64{}, &atomic.Int64{}
	freezeAfter.Store(-1)
	node := scheduler.New(cfg, store.New(testdb.Pool(t, dbURL, gate)), gatedSender{sender, gate, freezeAfter, frozenAt}, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for jobType, handler := range handlers {
		node.Handle(jobType, handler)
	}
	go node.Run(ctx)
	t.Cleanup(gate.Resume) // let a "killed" node unwind so its pool can close
	return testNode{node, m, gate, freezeAfter, frozenAt}
}

func seedAlerts(t *testing.T, db *store.Store, tenant uuid.UUID, count int, maxRuntimeMS int) {
	t.Helper()
	at := time.Now().UTC()
	for i := 0; i < count; i++ {
		id := uuid.New()
		payload, _ := json.Marshal(map[string]string{"employee": fmt.Sprintf("emp-%04d", i), "message": "your cab is assigned"})
		job := domain.Job{ID: id, TenantID: tenant, Type: "alert", Payload: payload, Timezone: "UTC",
			MisfirePolicy: domain.Coalesce, MaxRuntimeMS: maxRuntimeMS, Shard: domain.ShardFor(id)}
		if err := db.CreateJob(context.Background(), job, at); err != nil {
			t.Fatal(err)
		}
	}
}

func waitFor(t *testing.T, timeout time.Duration, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// One read-only pool per test DB. Opening a fresh pool on every poll leaked
// connections and made the kill test flaky.
var checkPools sync.Map

func succeededRuns(t *testing.T, dbURL string) int {
	t.Helper()
	pool, ok := checkPools.Load(dbURL)
	if !ok {
		pool, _ = checkPools.LoadOrStore(dbURL, testdb.Pool(t, dbURL, nil))
	}
	var n int
	if err := pool.(*pgxpool.Pool).QueryRow(context.Background(), `SELECT count(*) FROM runs WHERE status = 'SUCCEEDED'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func startCluster(t *testing.T, ctx context.Context, dbURL string, rx notify.Sender, size int) []testNode {
	t.Helper()
	nodes := make([]testNode, size)
	for i := range nodes {
		nodes[i] = startNode(t, ctx, dbURL, fmt.Sprintf("node-%d", i), rx, nil, nil)
	}
	waitFor(t, 10*time.Second, "shards to spread across nodes", func() bool {
		total := 0
		for _, n := range nodes {
			owned := int(n.metrics.OwnedShards.Load())
			if owned < domain.ShardCount/size-1 {
				return false
			}
			total += owned
		}
		return total == domain.ShardCount
	})
	return nodes
}

// Happy path: three nodes, 1,200 alerts, not a single repeat.
func TestThreeNodesDeliverEachAlertOnce(t *testing.T) {
	dbURL := testdb.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rx := newReceiver()
	startCluster(t, ctx, dbURL, rx, 3)
	seedAlerts(t, store.New(testdb.Pool(t, dbURL, nil)), uuid.New(), 1200, 5000)
	waitFor(t, 30*time.Second, "1,200 alerts", func() bool { u, _ := rx.delivered(); return u == 1200 })
	waitFor(t, 5*time.Second, "all runs recorded", func() bool { return succeededRuns(t, dbURL) == 1200 })
	if _, repeats := rx.delivered(); repeats != 0 {
		t.Fatalf("healthy cluster re-sent %d alerts", repeats)
	}
}

// The kill -9 case from the brief: a node dies while 1,200 alerts go out. The
// others should pick up its shards within the lease time, one alert per person.
func TestKilledNodeIsTakenOverAndEveryAlertArrivesOnce(t *testing.T) {
	dbURL := testdb.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rx := newReceiver()
	nodes := startCluster(t, ctx, dbURL, rx, 3)
	victim := nodes[0]
	victim.freezeAfter.Store(100) // killed mid-send and never resumed during the test
	seedAlerts(t, store.New(testdb.Pool(t, dbURL, nil)), uuid.New(), 1200, 5000)

	waitFor(t, 30*time.Second, "1,200 alerts after the kill", func() bool { u, _ := rx.delivered(); return u == 1200 })
	waitFor(t, 10*time.Second, "all runs recorded", func() bool { return succeededRuns(t, dbURL) == 1200 })
	if victim.frozenAt.Load() == 0 {
		t.Fatal("the node was never killed")
	}
	if took := time.Since(time.Unix(0, victim.frozenAt.Load())); took > leaseTTL+14*time.Second { // lease expiry plus delivering the rest, on a loaded laptop
		t.Fatalf("takeover took %s; want within the lease time plus a few polls", took)
	}
}

// The SIGSTOP case: freeze a node past its lease and let the others finish.
// When it wakes up, its late writes should bounce off the fencing check.
func TestFrozenNodeIsFencedWhenItResumes(t *testing.T) {
	dbURL := testdb.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rx := newReceiver()
	nodes := startCluster(t, ctx, dbURL, rx, 3)
	frozen := nodes[0]
	frozen.freezeAfter.Store(100)
	// Long max runtime so the frozen node's attempts are still alive when it
	// wakes up. That's what a really stopped process looks like.
	seedAlerts(t, store.New(testdb.Pool(t, dbURL, nil)), uuid.New(), 1200, 60_000)

	waitFor(t, 30*time.Second, "others to finish the frozen node's work", func() bool { u, _ := rx.delivered(); return u == 1200 })
	waitFor(t, 10*time.Second, "all runs recorded", func() bool { return succeededRuns(t, dbURL) == 1200 })
	frozen.gate.Resume()
	waitFor(t, 10*time.Second, "the resumed node's stale writes to be rejected", func() bool { return frozen.metrics.Stale.Load() > 0 })
	time.Sleep(200 * time.Millisecond)
	if unique, _ := rx.delivered(); unique != 1200 || succeededRuns(t, dbURL) != 1200 {
		t.Fatalf("want 1,200 alerts and 1,200 runs, got %d alerts and %d runs", unique, succeededRuns(t, dbURL))
	}
}

// The 50k vs 20 case, scaled down: a noisy tenant shouldn't hold up a quiet one.
func TestNoisyTenantDoesNotDelayQuietTenant(t *testing.T) {
	dbURL := testdb.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := store.New(testdb.Pool(t, dbURL, nil))
	noisy, quiet := uuid.New(), uuid.New()
	seedAlerts(t, db, noisy, 2000, 5000)
	seedAlerts(t, db, quiet, 20, 5000)
	if err := db.SetMaxRunning(ctx, noisy, 1000); err != nil {
		t.Fatal(err)
	}
	rx := newReceiver()
	startNode(t, ctx, dbURL, "node-0", rx, func(c *scheduler.Config) { c.Workers = 8 }, nil)
	waitFor(t, 60*time.Second, "all 2,020 alerts", func() bool { u, _ := rx.delivered(); return u == 2020 })
	rx.mu.Lock()
	defer rx.mu.Unlock()
	last := 0
	for i, tenant := range rx.order {
		if tenant == quiet {
			last = i
		}
	}
	if last >= 100 {
		t.Fatalf("quiet tenant's last alert was #%d of 2,020; it should finish within the first 100", last+1)
	}
}

func TestFailingRunRetriesThenDeadLettersThenReplays(t *testing.T) {
	dbURL := testdb.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := store.New(testdb.Pool(t, dbURL, nil))
	tenant := uuid.New()
	seedAlerts(t, db, tenant, 1, 5000)
	var attempts atomic.Int64
	var healthy atomic.Bool
	startNode(t, ctx, dbURL, "node-0", newReceiver(), func(c *scheduler.Config) { c.MaxAttempts = 3 }, map[string]scheduler.Handler{
		"alert": func(context.Context, store.ClaimedRun) error {
			attempts.Add(1)
			if !healthy.Load() {
				return errors.New("downstream rejected the alert")
			}
			return nil
		},
	})
	var dead []domain.Run
	waitFor(t, 10*time.Second, "dead letter", func() bool { dead, _ = db.DeadLetters(ctx, tenant, 10); return len(dead) == 1 })
	if attempts.Load() != 3 || dead[0].LastError != "HANDLER_ERROR" {
		t.Fatalf("want 3 attempts then a dead letter, got %d attempts, error %q", attempts.Load(), dead[0].LastError)
	}
	healthy.Store(true)
	if err := db.Replay(ctx, tenant, dead[0].ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "replayed run to succeed", func() bool { return succeededRuns(t, dbURL) == 1 })
}

func TestMaxRuntimeIsEnforced(t *testing.T) {
	dbURL := testdb.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := store.New(testdb.Pool(t, dbURL, nil))
	tenant := uuid.New()
	seedAlerts(t, db, tenant, 1, 200)
	startNode(t, ctx, dbURL, "node-0", newReceiver(), func(c *scheduler.Config) { c.MaxAttempts = 2 }, map[string]scheduler.Handler{
		"alert": func(ctx context.Context, _ store.ClaimedRun) error {
			<-ctx.Done() // a stuck downstream call
			return ctx.Err()
		},
	})
	waitFor(t, 10*time.Second, "timed-out run to be dead-lettered", func() bool {
		dead, _ := db.DeadLetters(ctx, tenant, 10)
		return len(dead) == 1 && dead[0].LastError == "TIMEOUT"
	})
}

// After downtime SKIP drops the missed run and COALESCE runs it once. Either
// way the schedule carries on from now.
func TestMisfirePolicyAfterDowntime(t *testing.T) {
	for _, policy := range []domain.MisfirePolicy{domain.Skip, domain.Coalesce} {
		t.Run(string(policy), func(t *testing.T) {
			dbURL := testdb.New(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pool := testdb.Pool(t, dbURL, nil)
			jobs := service.NewJobs(store.New(pool), nil)
			job, err := jobs.Create(ctx, uuid.New(), service.CreateJob{Type: "alert", Payload: json.RawMessage(`{}`), Cron: "*/5 * * * *", MisfirePolicy: policy})
			if err != nil {
				t.Fatal(err)
			}
			// Pretend we were down: push the stored occurrence 20 minutes back.
			missed := time.Now().Add(-20 * time.Minute).Truncate(5 * time.Minute).UTC()
			if _, err := pool.Exec(ctx, `UPDATE runs SET scheduled_at = $1, next_attempt_at = $1, idempotency_key = $2`,
				missed, domain.OccurrenceKey(job.ID, missed)); err != nil {
				t.Fatal(err)
			}
			rx := newReceiver()
			startNode(t, ctx, dbURL, "node-0", rx, nil, nil)
			var times []time.Time
			waitFor(t, 10*time.Second, "next occurrence after the misfire", func() bool {
				times, _ = jobs.NextFireTimes(ctx, job.TenantID, job.ID, 1)
				return len(times) == 1 && times[0].After(time.Now())
			})
			time.Sleep(200 * time.Millisecond)
			unique, _ := rx.delivered()
			if want := map[domain.MisfirePolicy]int{domain.Skip: 0, domain.Coalesce: 1}[policy]; unique != want {
				t.Fatalf("%s: want %d alerts for the missed occurrence, got %d", policy, want, unique)
			}
		})
	}
}

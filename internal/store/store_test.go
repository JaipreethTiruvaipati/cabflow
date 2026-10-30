package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/JaipreethTiruvaipati/cabflow/internal/domain"
	"github.com/JaipreethTiruvaipati/cabflow/internal/store"
	"github.com/JaipreethTiruvaipati/cabflow/internal/testdb"
)

var ctx = context.Background()

func runNow(store.ClaimedRun) (bool, time.Time, error) { return true, time.Time{}, nil }

func TestPruneRetainsActiveRunsAndDeadLetters(t *testing.T) {
	url := testdb.New(t)
	pool := testdb.Pool(t, url, nil)
	db := store.New(pool)
	tenant := uuid.New()
	for _, status := range []string{"SUCCEEDED", "SKIPPED", "PENDING", "RUNNING", "RETRYING", "DEAD", "SUCCEEDED"} {
		job := addJob(t, db, tenant, time.Now())
		age := "8 days"
		if status == "SUCCEEDED" {
			var count int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM runs WHERE status = 'SUCCEEDED'`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count > 0 {
				age = "1 day"
			}
		}
		if _, err := pool.Exec(ctx, `UPDATE runs SET status = $2, finished_at = statement_timestamp() - $3::interval WHERE job_id = $1`, job.ID, status, age); err != nil {
			t.Fatal(err)
		}
	}
	kept := addJob(t, db, tenant, time.Now())
	if _, err := pool.Exec(ctx, `UPDATE runs SET status = 'SUCCEEDED', finished_at = statement_timestamp() - interval '8 days' WHERE job_id = $1`, kept.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Trigger(ctx, tenant, kept.ID, "pending"); err != nil {
		t.Fatal(err)
	}
	locked, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locked.Rollback(ctx)
	if _, err := locked.Exec(ctx, `SELECT pg_advisory_xact_lock(4242, 1)`); err != nil {
		t.Fatal(err)
	}
	if n, err := db.Prune(ctx, 7); err != nil || n != 0 {
		t.Fatalf("another cleaner owns the lock: deleted %d (%v)", n, err)
	}
	if err := locked.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := db.Prune(ctx, 7); err != nil || n != 3 {
		t.Fatalf("want 3 expired successful/skipped runs removed, got %d (%v)", n, err)
	}
	var runs, jobs int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM runs), (SELECT count(*) FROM jobs)`).Scan(&runs, &jobs); err != nil {
		t.Fatal(err)
	}
	if runs != 6 || jobs != 6 {
		t.Fatalf("active, dead and recent records must stay; got %d runs, %d jobs", runs, jobs)
	}
	if _, err := db.GetJob(ctx, tenant, kept.ID); err != nil {
		t.Fatalf("one-off with pending manual run was removed: %v", err)
	}
	if n, err := db.Prune(ctx, 7); err != nil || n != 0 {
		t.Fatalf("cleanup should now be empty: %d (%v)", n, err)
	}
}

func TestPruneBatchesAndPreservesRecurringJob(t *testing.T) {
	url := testdb.New(t)
	pool := testdb.Pool(t, url, nil)
	db := store.New(pool)
	job := addJob(t, db, uuid.New(), time.Now())
	if _, err := pool.Exec(ctx, `UPDATE jobs SET cron = '* * * * *' WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO runs
		(id, job_id, tenant_id, shard, priority, scheduled_at, idempotency_key, status, next_attempt_at, finished_at)
		SELECT gen_random_uuid(), $1, $2, $3, 0, clock_timestamp(), 'expired-' || i, 'SUCCEEDED',
			clock_timestamp(), statement_timestamp() - interval '8 days' FROM generate_series(1, 5001) i`,
		job.ID, job.TenantID, job.Shard); err != nil {
		t.Fatal(err)
	}
	for _, want := range []int64{5000, 1, 0} {
		if n, err := db.Prune(ctx, 7); err != nil || n != want {
			t.Fatalf("want batch of %d, got %d (%v)", want, n, err)
		}
	}
	if _, err := db.GetJob(ctx, job.TenantID, job.ID); err != nil {
		t.Fatalf("recurring job was removed: %v", err)
	}
}

func addJob(t *testing.T, db *store.Store, tenant uuid.UUID, at time.Time) domain.Job {
	t.Helper()
	id := uuid.New()
	job := domain.Job{ID: id, TenantID: tenant, Type: "alert", Payload: json.RawMessage(`{}`), Timezone: "UTC",
		MisfirePolicy: domain.Coalesce, MaxRuntimeMS: 5000, Shard: domain.ShardFor(id)}
	if err := db.CreateJob(ctx, job, at); err != nil {
		t.Fatal(err)
	}
	return job
}

func ownAllShards(t *testing.T, db *store.Store, node string) store.Leases {
	t.Helper()
	leases, err := db.AcquireLeases(ctx, node, domain.ShardCount, 10*time.Second)
	if err != nil || len(leases) != domain.ShardCount {
		t.Fatalf("acquire: %v, got %d shards", err, len(leases))
	}
	return leases
}

// 8 nodes racing for shards must never end up sharing one.
func TestConcurrentLeaseAcquisitionIsExclusive(t *testing.T) {
	url := testdb.New(t)
	db := store.New(testdb.Pool(t, url, nil))
	var mu sync.Mutex
	owners := make(map[int]string)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		node := uuid.NewString()
		wg.Add(1)
		go func() {
			defer wg.Done()
			leases, err := db.AcquireLeases(ctx, node, domain.ShardCount, 10*time.Second)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			defer mu.Unlock()
			for shard := range leases {
				if previous, taken := owners[shard]; taken {
					t.Errorf("shard %d given to %s and %s", shard, previous, node)
				}
				owners[shard] = node
			}
		}()
	}
	wg.Wait()
	if len(owners) != domain.ShardCount {
		t.Fatalf("want all %d shards owned, got %d", domain.ShardCount, len(owners))
	}
}

// The SIGSTOP case: node-a's lease runs out, node-b takes over, then node-a
// tries to commit.
func TestStaleOwnerCannotCommit(t *testing.T) {
	url := testdb.New(t)
	db := store.New(testdb.Pool(t, url, nil))
	addJob(t, db, uuid.New(), time.Now().Add(-time.Second))
	ownAllShards(t, db, "node-a")
	claimed, _, _, err := db.Claim(ctx, "node-a", 10, 10, runNow)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v, %d runs", err, len(claimed))
	}

	// Pretend node-a froze long enough for its leases to run out.
	if _, err := testdb.Pool(t, url, nil).Exec(ctx, `UPDATE shard_leases SET expires_at = clock_timestamp() - interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	ownAllShards(t, db, "node-b")

	err = db.Finish(ctx, "node-a", claimed[0], store.Outcome{Status: domain.RunSucceeded})
	if !errors.Is(err, domain.ErrStaleLease) {
		t.Fatalf("stale owner's write: want ErrStaleLease, got %v", err)
	}
	if recovered, err := db.Recover(ctx, "node-b", 5); err != nil || recovered != 1 {
		t.Fatalf("new owner should recover the run: %v, %d", err, recovered)
	}
}

func TestClaimIsFairAndRespectsCapAndPause(t *testing.T) {
	url := testdb.New(t)
	db := store.New(testdb.Pool(t, url, nil))
	noisy, quiet, paused := uuid.New(), uuid.New(), uuid.New()
	past := time.Now().Add(-time.Second)
	for i := 0; i < 50; i++ {
		addJob(t, db, noisy, past)
	}
	for i := 0; i < 3; i++ {
		addJob(t, db, quiet, past)
		addJob(t, db, paused, past)
	}
	if err := db.SetMaxRunning(ctx, noisy, 5); err != nil {
		t.Fatal(err)
	}
	if err := db.SetPaused(ctx, paused, true); err != nil {
		t.Fatal(err)
	}
	ownAllShards(t, db, "node-a")
	claimed, _, throttled, err := db.Claim(ctx, "node-a", 100, 20, runNow)
	if err != nil {
		t.Fatal(err)
	}
	count := map[uuid.UUID]int{}
	for _, run := range claimed {
		count[run.TenantID]++
	}
	if count[noisy] != 5 || count[quiet] != 3 || count[paused] != 0 {
		t.Fatalf("want noisy capped at 5, quiet 3, paused 0; got %v", count)
	}
	// 20 candidates for noisy (perTenant), 5 slots, so 15 throttled.
	if throttled != 15 {
		t.Fatalf("want 15 runs held back by noisy's cap, got %d", throttled)
	}
	stats, err := db.Stats(ctx)
	if err != nil || stats.Paused != 3 {
		t.Fatalf("want 3 due runs held by the pause, got %d (%v)", stats.Paused, err)
	}
}

// If the next occurrence weren't written in the claim tx, a crash right after
// claiming would end the schedule for good.
func TestClaimStoresNextOccurrence(t *testing.T) {
	url := testdb.New(t)
	db := store.New(testdb.Pool(t, url, nil))
	job := addJob(t, db, uuid.New(), time.Now().Add(-time.Second))
	ownAllShards(t, db, "node-a")
	next := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	plan := func(store.ClaimedRun) (bool, time.Time, error) { return true, next, nil }
	if _, _, _, err := db.Claim(ctx, "node-a", 10, 10, plan); err != nil {
		t.Fatal(err)
	}
	at, ok, err := db.NextScheduled(ctx, job.ID)
	if err != nil || !ok || !at.Equal(next) {
		t.Fatalf("next occurrence: want %s, got %s (%v, %v)", next, at, ok, err)
	}
}

func TestDeadLetterAndReplay(t *testing.T) {
	url := testdb.New(t)
	db := store.New(testdb.Pool(t, url, nil))
	tenant := uuid.New()
	addJob(t, db, tenant, time.Now().Add(-time.Second))
	ownAllShards(t, db, "node-a")
	claimed, _, _, _ := db.Claim(ctx, "node-a", 10, 10, runNow)
	if err := db.Finish(ctx, "node-a", claimed[0], store.Outcome{Status: domain.RunDead, Error: "HANDLER_ERROR"}); err != nil {
		t.Fatal(err)
	}
	dead, err := db.DeadLetters(ctx, tenant, 10)
	if err != nil || len(dead) != 1 || dead[0].LastError != "HANDLER_ERROR" {
		t.Fatalf("dead letters: %v %+v", err, dead)
	}
	if err := db.Replay(ctx, tenant, dead[0].ID); err != nil {
		t.Fatal(err)
	}
	again, _, _, _ := db.Claim(ctx, "node-a", 10, 10, runNow)
	if len(again) != 1 || again[0].IdempotencyKey != claimed[0].IdempotencyKey || again[0].Attempt != 1 {
		t.Fatalf("replay should requeue the same occurrence with a fresh budget, got %+v", again)
	}
	if err := db.Replay(ctx, uuid.New(), dead[0].ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("another tenant must not replay this run, got %v", err)
	}
}

func TestManualTriggerIsIdempotent(t *testing.T) {
	url := testdb.New(t)
	db := store.New(testdb.Pool(t, url, nil))
	tenant := uuid.New()
	job := addJob(t, db, tenant, time.Now().Add(time.Hour))
	first, err := db.Trigger(ctx, tenant, job.ID, "click-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.Trigger(ctx, tenant, job.ID, "click-1")
	if err != nil || second.ID != first.ID {
		t.Fatalf("same key should return the same run: %v", err)
	}
	if _, err := db.Trigger(ctx, uuid.New(), job.ID, "click-1"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("another tenant must not trigger this job, got %v", err)
	}
}

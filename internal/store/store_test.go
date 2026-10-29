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


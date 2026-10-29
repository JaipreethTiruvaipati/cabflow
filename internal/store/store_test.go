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


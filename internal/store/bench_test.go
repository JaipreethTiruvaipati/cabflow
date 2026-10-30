package store_test

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/JaipreethTiruvaipati/cabflow/internal/domain"
	"github.com/JaipreethTiruvaipati/cabflow/internal/store"
	"github.com/JaipreethTiruvaipati/cabflow/internal/testdb"
)

const (
	benchRuns    = 10_000
	benchTenants = 50
)

// BenchmarkClaim drains 10k due runs from 50 tenants on one node: claim,
// then finish each one. DB work only, no handler.
//
//	TEST_DATABASE_URL=... go test -run '^$' -bench Claim -benchtime 3x ./internal/store/
func BenchmarkClaim(b *testing.B) {
	pool := testdb.Pool(b, testdb.New(b), nil)
	db := store.New(pool)
	if leases, err := db.AcquireLeases(ctx, "bench", domain.ShardCount, time.Hour); err != nil || len(leases) != domain.ShardCount {
		b.Fatalf("acquire: %v, %d shards", err, len(leases))
	}
	tenants := make([]uuid.UUID, benchTenants)
	for i := range tenants {
		tenants[i] = uuid.New()
		if err := db.SetMaxRunning(ctx, tenants[i], 10_000); err != nil {
			b.Fatal(err)
		}
	}

	drained := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		seedDueRuns(b, pool, tenants)
		b.StartTimer()
		for {
			runs, _, _, err := db.Claim(ctx, "bench", 64, 20, runNow)
			if err != nil {
				b.Fatal(err)
			}
			if len(runs) == 0 {
				break
			}
			// Real nodes finish runs in parallel, so do the same here.
			var wg sync.WaitGroup
			for _, run := range runs {
				wg.Add(1)
				go func(run store.ClaimedRun) {
					defer wg.Done()
					if err := db.Finish(ctx, "bench", run, store.Outcome{Status: domain.RunSucceeded}); err != nil {
						b.Error(err)
					}
				}(run)
			}
			wg.Wait()
			drained += len(runs)
		}
	}
	b.ReportMetric(float64(drained)/b.Elapsed().Seconds(), "runs/s")
}

func seedDueRuns(b *testing.B, pool *pgxpool.Pool, tenants []uuid.UUID) {
	b.Helper()
	due := time.Now().Add(-time.Second).UTC()
	jobs := make([][]any, benchRuns)
	runs := make([][]any, benchRuns)
	payload, _ := json.Marshal(map[string]string{"employee": "bench"})
	for i := range benchRuns {
		id, tenant := uuid.New(), tenants[i%len(tenants)]
		shard := domain.ShardFor(id)
		jobs[i] = []any{id, tenant, "alert", payload, "UTC", string(domain.Coalesce), 0, 30_000, shard}
		runs[i] = []any{uuid.New(), id, tenant, shard, 0, due, domain.OccurrenceKey(id, due), "PENDING", due}
	}
	if _, err := pool.CopyFrom(ctx, pgx.Identifier{"jobs"},
		[]string{"id", "tenant_id", "type", "payload", "timezone", "misfire_policy", "priority", "max_runtime_ms", "shard"}, pgx.CopyFromRows(jobs)); err != nil {
		b.Fatal(err)
	}
	if _, err := pool.CopyFrom(ctx, pgx.Identifier{"runs"},
		[]string{"id", "job_id", "tenant_id", "shard", "priority", "scheduled_at", "idempotency_key", "status", "next_attempt_at"}, pgx.CopyFromRows(runs)); err != nil {
		b.Fatal(err)
	}
}

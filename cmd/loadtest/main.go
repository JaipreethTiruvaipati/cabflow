// Command loadtest dumps a pile of one-off alerts into the database, waits for
// the cluster to run them and prints fire lag and exactly-once numbers. Exits
// non-zero if anything is off.
//
//	go run ./cmd/loadtest -jobs 100000 -window 10m
//
// -burst is the brief's 50k vs 20 case: everything fires at once and the quiet
// tenant still has to start on time.
//
//	go run ./cmd/loadtest -burst -jobs 50000 -quiet 20
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/JaipreethTiruvaipati/cabflow/internal/domain"
)

func main() {
	dbURL := flag.String("db", "postgres://tripora:tripora@localhost:5432/tripora?sslmode=disable", "PostgreSQL URL")
	jobs := flag.Int("jobs", 100_000, "number of one-off alert jobs")
	window := flag.Duration("window", 10*time.Minute, "fire times are spread evenly over this window")
	startIn := flag.Duration("start-in", 30*time.Second, "delay before the first fire time")
	tenants := flag.Int("tenants", 50, "tenants; the first one gets half of all jobs")
	maxLag := flag.Duration("max-p99", 2*time.Second, "p99 fire lag target")
	reportOnly := flag.String("report", "", "only print the report for an earlier run's tag")
	burst := flag.Bool("burst", false, "fire every job at the same instant: -jobs for one hot tenant plus -quiet for another")
	quiet := flag.Int("quiet", 20, "jobs for the quiet tenant in -burst mode")
	flag.Parse()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *dbURL)
	check(err)
	defer pool.Close()
	if *reportOnly != "" {
		var hot uuid.UUID
		var total int
		check(pool.QueryRow(ctx, `SELECT tenant_id, total FROM (SELECT j.tenant_id, count(*) OVER () AS total, count(*) AS n
			FROM jobs j WHERE j.payload->>'loadtest' = $1 GROUP BY j.tenant_id) t ORDER BY n DESC LIMIT 1`, *reportOnly).Scan(&hot, &total))
		var jobs int
		var wasBurst bool
		check(pool.QueryRow(ctx, `SELECT count(*), COALESCE(bool_or(payload->>'mode' = 'burst'), false)
			FROM jobs WHERE payload->>'loadtest' = $1`, *reportOnly).Scan(&jobs, &wasBurst))
		os.Exit(report(ctx, pool, *reportOnly, hot, jobs, *maxLag, wasBurst))
	}

	tag := "lt-" + time.Now().UTC().Format("20060102T150405")
	if *burst {
		*tenants = 2 // the hot tenant and the quiet one
	}
	ids := make([]uuid.UUID, *tenants)
	for i := range ids {
		ids[i] = uuid.New()
	}
	hot := ids[0]
	if *burst {
		start := seed(ctx, pool, tag, ids, *jobs, *quiet, 0, *startIn)
		fmt.Printf("seeded %d hot + %d quiet jobs (tag %s), all firing at %s\n", *jobs, *quiet, tag, start.Format(time.TimeOnly))
		*jobs += *quiet
	} else {
		start := seed(ctx, pool, tag, ids, *jobs, 0, *window, *startIn)
		fmt.Printf("seeded %d jobs (tag %s) firing from %s over %s\n", *jobs, tag, start.Format(time.TimeOnly), *window)
	}

	deadline := time.Now().Add(*startIn + *window + 5*time.Minute)
	for {
		var done, failed int
		check(pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE r.status = 'SUCCEEDED'), count(*) FILTER (WHERE r.status = 'DEAD')
			FROM runs r JOIN jobs j ON j.id = r.job_id WHERE j.payload->>'loadtest' = $1`, tag).Scan(&done, &failed))
		fmt.Printf("%s succeeded=%d dead=%d\n", time.Now().Format(time.TimeOnly), done, failed)
		if done+failed == *jobs || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Second)
	}
	os.Exit(report(ctx, pool, tag, hot, *jobs, *maxLag, *burst))
}

// seed bulk-loads jobs and runs with COPY. With quiet > 0 it's a burst and
// everything is due at the same instant; otherwise tenants[0] gets half the
// jobs and fire times are spread over the window.
func seed(ctx context.Context, pool *pgxpool.Pool, tag string, tenants []uuid.UUID, count, quiet int, window, startIn time.Duration) time.Time {
	var now time.Time
	check(pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now))
	start := now.Add(startIn)
	for _, tenant := range tenants {
		_, err := pool.Exec(ctx, `INSERT INTO tenants (id) VALUES ($1) ON CONFLICT DO NOTHING`, tenant)
		check(err)
	}
	burst := quiet > 0
	mode := "spread"
	if burst {
		mode = "burst"
	}
	total := count + quiet
	jobRows := make([][]any, total)
	runRows := make([][]any, total)
	for i := range total {
		tenant := tenants[0]
		switch {
		case burst && i >= count:
			tenant = tenants[1]
		case !burst && i%2 == 1 && len(tenants) > 1:
			tenant = tenants[1+rand.IntN(len(tenants)-1)]
		}
		id := uuid.New()
		at := start.UTC()
		if !burst {
			at = start.Add(time.Duration(float64(window) * float64(i) / float64(count))).UTC()
		}
		payload, _ := json.Marshal(map[string]string{"loadtest": tag, "mode": mode, "employee": fmt.Sprintf("emp-%06d", i), "message": "your cab is assigned"})
		shard := domain.ShardFor(id)
		jobRows[i] = []any{id, tenant, "alert", payload, "UTC", string(domain.Coalesce), 0, 30_000, shard}
		runRows[i] = []any{uuid.New(), id, tenant, shard, 0, at, domain.OccurrenceKey(id, at), "PENDING", at}
	}
	_, err := pool.CopyFrom(ctx, pgx.Identifier{"jobs"},
		[]string{"id", "tenant_id", "type", "payload", "timezone", "misfire_policy", "priority", "max_runtime_ms", "shard"}, pgx.CopyFromRows(jobRows))
	check(err)
	_, err = pool.CopyFrom(ctx, pgx.Identifier{"runs"},
		[]string{"id", "job_id", "tenant_id", "shard", "priority", "scheduled_at", "idempotency_key", "status", "next_attempt_at"}, pgx.CopyFromRows(runRows))
	check(err)
	return start
}

// report prints the numbers and returns the exit code. For a burst only the
// quiet tenant's p99 counts; the hot one is supposed to queue behind its cap.
func report(ctx context.Context, pool *pgxpool.Pool, tag string, hot uuid.UUID, jobs int, maxLag time.Duration, burst bool) int {
	fmt.Println()
	fmt.Printf("machine: %s, %d CPUs, %s/%s\n", cpuModel(), runtime.NumCPU(), runtime.GOOS, runtime.GOARCH)

	var succeeded, dead int
	var first, last time.Time
	check(pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE r.status = 'SUCCEEDED'), count(*) FILTER (WHERE r.status = 'DEAD'),
			min(r.scheduled_at), max(r.finished_at)
		FROM runs r JOIN jobs j ON j.id = r.job_id WHERE j.payload->>'loadtest' = $1`, tag).Scan(&succeeded, &dead, &first, &last))
	fmt.Printf("runs: %d succeeded, %d dead of %d; throughput %.1f runs/s\n", succeeded, dead, jobs, float64(succeeded)/last.Sub(first).Seconds())

	hotName, otherName, gate := "hot tenant (50% of jobs)", "other tenants", "all tenants"
	if burst {
		hotName, otherName, gate = "hot tenant (burst)", "quiet tenant", "quiet tenant"
	}
	var p99 float64
	for _, group := range []struct{ name, filter string }{
		{"all tenants", "$2::uuid IS NOT NULL"},
		{hotName, "r.tenant_id = $2"},
		{otherName, "r.tenant_id <> $2"},
	} {
		var n int
		var p50, p95, p99g, worst float64
		check(pool.QueryRow(ctx, `SELECT count(*), COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY lag), 0),
				COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY lag), 0),
				COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY lag), 0), COALESCE(max(lag), 0)
			FROM (SELECT extract(epoch FROM r.started_at - r.scheduled_at) AS lag
				FROM runs r JOIN jobs j ON j.id = r.job_id WHERE j.payload->>'loadtest' = $1 AND `+group.filter+`) lags`, tag, hot).
			Scan(&n, &p50, &p95, &p99g, &worst))
		if n == 0 {
			continue // nothing to show, e.g. "other tenants" in a one-tenant run
		}
		fmt.Printf("fire lag, %-25s p50=%.3fs p95=%.3fs p99=%.3fs max=%.3fs\n", group.name+":", p50, p95, p99g, worst)
		if group.name == gate {
			p99 = p99g
		}
	}

	var delivered, repeats int
	var d50, d99 float64
	err := pool.QueryRow(ctx, `SELECT count(*), COALESCE(sum(d.attempts - 1), 0),
			COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM d.received_at - r.scheduled_at)), 0),
			COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM d.received_at - r.scheduled_at)), 0)
		FROM runs r JOIN jobs j ON j.id = r.job_id JOIN deliveries d ON d.idempotency_key = r.idempotency_key
		WHERE j.payload->>'loadtest' = $1`, tag).Scan(&delivered, &repeats, &d50, &d99)
	if err != nil {
		fmt.Println("deliveries: receiver table not found; run the compose receiver to check exactly-once")
	} else {
		fmt.Printf("delivery lag: p50=%.3fs p99=%.3fs\n", d50, d99)
		fmt.Printf("alerts: %d delivered once each, %d re-sends ignored by the receiver\n", delivered, repeats)
	}
	if burst && err == nil {
		// Where did the quiet tenant's last alert land?
		var last, total int
		check(pool.QueryRow(ctx, `SELECT COALESCE(max(pos) FILTER (WHERE tenant_id <> $2), 0), count(*)
			FROM (SELECT r.tenant_id, row_number() OVER (ORDER BY d.received_at) AS pos
				FROM runs r JOIN jobs j ON j.id = r.job_id JOIN deliveries d ON d.idempotency_key = r.idempotency_key
				WHERE j.payload->>'loadtest' = $1) ranked`, tag, hot).Scan(&last, &total))
		fmt.Printf("quiet tenant's last alert was delivery #%d of %d\n", last, total)
	}

	failures := []string{}
	if succeeded != jobs {
		failures = append(failures, fmt.Sprintf("%d runs did not succeed", jobs-succeeded))
	}
	if err == nil && delivered != jobs {
		failures = append(failures, fmt.Sprintf("%d alerts delivered, want %d", delivered, jobs))
	}
	if p99 > maxLag.Seconds() {
		failures = append(failures, fmt.Sprintf("%s p99 fire lag %.3fs over %s", gate, p99, maxLag))
	}
	if len(failures) > 0 {
		fmt.Println("RESULT: FAIL -", strings.Join(failures, "; "))
		return 1
	}
	fmt.Printf("RESULT: PASS - every job ran and every alert arrived exactly once, %s p99 fire lag under %s\n", gate, maxLag)
	return 0
}

func cpuModel() string {
	data, _ := os.ReadFile("/proc/cpuinfo")
	for _, line := range strings.Split(string(data), "\n") {
		if name, ok := strings.CutPrefix(line, "model name"); ok {
			return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(name), ":"))
		}
	}
	return "unknown CPU"
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "loadtest:", err)
		os.Exit(2)
	}
}

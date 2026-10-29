// Package metrics serves /metrics in Prometheus text format. Hand-rolled to
// avoid pulling in the client library for a dozen numbers.
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JaipreethTiruvaipati/cabflow/internal/store"
)

var lagBuckets = []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60}

// The run-count gauges come from a DB query, so we cache them. They can be
// up to statsTTL old, which is fine for a dashboard.
const statsTTL = 5 * time.Second

type Metrics struct {
	lagCounts   [10]atomic.Int64 // one per bucket, plus +Inf
	lagSumMS    atomic.Int64
	Claimed     atomic.Int64
	Succeeded   atomic.Int64
	Retried     atomic.Int64
	DeadLetters atomic.Int64
	Skipped     atomic.Int64
	Throttled   atomic.Int64 // due runs held back by their tenant's running cap
	Stale       atomic.Int64 // writes rejected by the fencing check
	Recovered   atomic.Int64 // runs taken back from a lost owner
	HTTP5xx     atomic.Int64
	OwnedShards atomic.Int64

	errorsMu sync.Mutex
	errors   map[string]int64 // keyed by operation, e.g. "claim runs"

	statsMu   sync.Mutex
	stats     store.Stats
	statsAt   time.Time
	statsFrom func(context.Context) (store.Stats, error)
}

// ObserveFireLag records how late a run started, in seconds.
func (m *Metrics) ObserveFireLag(seconds float64) {
	m.lagSumMS.Add(int64(seconds * 1000))
	i := 0
	for i < len(lagBuckets) && seconds > lagBuckets[i] {
		i++
	}
	m.lagCounts[i].Add(1)
}

// Error bumps the error counter for an operation.
func (m *Metrics) Error(operation string) {
	m.errorsMu.Lock()
	defer m.errorsMu.Unlock()
	if m.errors == nil {
		m.errors = make(map[string]int64)
	}
	m.errors[operation]++
}

func (m *Metrics) cachedStats(ctx context.Context) (store.Stats, error) {
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	if !m.statsAt.IsZero() && time.Since(m.statsAt) < statsTTL {
		return m.stats, nil
	}
	stats, err := m.statsFrom(ctx)
	if err != nil {
		return store.Stats{}, err
	}
	m.stats, m.statsAt = stats, time.Now()
	return stats, nil
}

// Handler serves the metrics. Counters are per node; the run counts are
// cluster-wide since they come straight from the database.
func (m *Metrics) Handler(db *store.Store) http.Handler {
	m.statsMu.Lock()
	m.statsFrom = db.Stats
	m.statsMu.Unlock()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		var cumulative, total int64
		for i := range m.lagCounts {
			total += m.lagCounts[i].Load()
		}
		fmt.Fprintln(w, "# TYPE tripora_fire_lag_seconds histogram")
		for i, le := range lagBuckets {
			cumulative += m.lagCounts[i].Load()
			fmt.Fprintf(w, "tripora_fire_lag_seconds_bucket{le=\"%g\"} %d\n", le, cumulative)
		}
		fmt.Fprintf(w, "tripora_fire_lag_seconds_bucket{le=\"+Inf\"} %d\n", total)
		fmt.Fprintf(w, "tripora_fire_lag_seconds_sum %.3f\ntripora_fire_lag_seconds_count %d\n", float64(m.lagSumMS.Load())/1000, total)

		counters := []struct {
			name  string
			value int64
		}{
			{"tripora_runs_claimed_total", m.Claimed.Load()},
			{"tripora_runs_succeeded_total", m.Succeeded.Load()},
			{"tripora_runs_retried_total", m.Retried.Load()},
			{"tripora_runs_dead_lettered_total", m.DeadLetters.Load()},
			{"tripora_runs_skipped_total", m.Skipped.Load()},
			{"tripora_runs_throttled_total", m.Throttled.Load()},
			{"tripora_stale_writes_rejected_total", m.Stale.Load()},
			{"tripora_runs_recovered_total", m.Recovered.Load()},
			{"tripora_http_5xx_total", m.HTTP5xx.Load()},
		}
		for _, c := range counters {
			fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", c.name, c.name, c.value)
		}

		fmt.Fprintln(w, "# TYPE tripora_errors_total counter")
		m.errorsMu.Lock()
		ops := make([]string, 0, len(m.errors))
		for op := range m.errors {
			ops = append(ops, op)
		}
		sort.Strings(ops)
		for _, op := range ops {
			fmt.Fprintf(w, "tripora_errors_total{operation=%q} %d\n", op, m.errors[op])
		}
		m.errorsMu.Unlock()

		fmt.Fprintf(w, "# TYPE tripora_owned_shards gauge\ntripora_owned_shards %d\n", m.OwnedShards.Load())

		stats, err := m.cachedStats(r.Context())
		if err != nil {
			return
		}
		fmt.Fprintf(w, "# TYPE tripora_runs gauge\n")
		fmt.Fprintf(w, "tripora_runs{state=\"due\"} %d\n", stats.Due)
		fmt.Fprintf(w, "tripora_runs{state=\"running\"} %d\n", stats.Running)
		fmt.Fprintf(w, "tripora_runs{state=\"retrying\"} %d\n", stats.Retrying)
		fmt.Fprintf(w, "tripora_runs{state=\"dead_lettered\"} %d\n", stats.Dead)
		fmt.Fprintf(w, "tripora_runs{state=\"paused\"} %d\n", stats.Paused)
	})
}

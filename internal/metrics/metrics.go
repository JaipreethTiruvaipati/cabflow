// Package metrics serves /metrics in Prometheus text format. Hand-rolled to
// avoid pulling in the client library for a dozen numbers.
package metrics

import (
	"context"
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


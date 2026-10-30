# Tripora: distributed trip-job scheduler

Tripora is the MoveInSync case study 3 solution. It runs timed jobs (rosters at 10 PM tenant time, "your cab is assigned" alerts, no-show cancels, billing close) on a cluster of three or more nodes. Any node can be killed or frozen at any time; no job is lost and no alert is sent twice.

It is Go and PostgreSQL, nothing else. See [docs/design.md](docs/design.md) for the architecture, the freeze/fencing sequence, the failure-mode table and the trade-offs.

## Run it

```bash
docker compose up --build
```

This starts PostgreSQL, a notification receiver, and three identical nodes. Each node serves the API and runs the scheduler:

| | |
|---|---|
| node-1 / node-2 / node-3 | http://localhost:8081, :8082, :8083 |
| receiver stats | http://localhost:9000/stats |
| metrics | http://localhost:8081/metrics (per node) |

## Try it

```bash
TOKEN=$(go run ./cmd/token -secret local-development-secret -roles admin)
H="Authorization: Bearer $TOKEN"

# Roster every day at 22:00 India time
curl -H "$H" localhost:8081/v1/jobs -d '{"type":"roster","payload":{"kind":"tomorrow"},"cron":"0 22 * * *","timezone":"Asia/Kolkata"}'

# One pickup alert at a set time
curl -H "$H" localhost:8081/v1/jobs -d '{"type":"alert","payload":{"employee":"e-42","message":"your cab is assigned"},"run_at":"2030-01-01T08:00:00+05:30"}'

curl -H "$H" "localhost:8082/v1/jobs/<id>/next-fire-times?limit=5"
curl -H "$H" -H "Idempotency-Key: retry-safe-1" -X POST localhost:8083/v1/jobs/<id>/trigger
curl -H "$H" localhost:8081/v1/jobs/<id>/runs
curl -H "$H" -X POST localhost:8081/v1/tenants/00000000-0000-0000-0000-000000000001/pause
curl -H "$H" -X POST localhost:8081/v1/tenants/00000000-0000-0000-0000-000000000001/resume
curl -H "$H" -X PUT localhost:8081/v1/tenants/00000000-0000-0000-0000-000000000001/max-running -d '{"max_running":50}'
curl -H "$H" localhost:8081/v1/dead-letters
curl -H "$H" -X POST localhost:8081/v1/dead-letters/<run-id>/replay
```

| Endpoint | Who |
|---|---|
| `GET /v1/jobs`, `/v1/jobs/{id}`, `/v1/jobs/{id}/runs`, `/v1/jobs/{id}/next-fire-times` | employee, admin, operator |
| `POST /v1/jobs`, `POST /v1/jobs/{id}/trigger`, dead letters | admin, operator |
| `/v1/tenants/{id}/pause`, `resume`, `max-running` | admin for their own tenant; operator for any tenant |
| `/healthz`, `/readyz`, `/metrics` | no token |

- **Tokens:** HS256 JWTs carrying `tenant_id`, `roles`, `iss=tripora` and `aud=tripora-api`.
- **Tenant isolation:** every query is scoped to the token's tenant, so one client can never read another's jobs.
- **Errors:** bad input gets a 400 with a plain message. Anything unexpected is logged and returns a bare 500.
- **Logs:** never include payloads, so phone numbers and addresses stay out of them. Database errors are logged as their SQLSTATE code only (`internal/safelog`).
- **Employees:** can see jobs and runs but not payloads, which can name other employees.

**Metrics for on-call** (`/metrics` on each node):

| Metric | What it shows |
|---|---|
| `tripora_fire_lag_seconds` | Histogram of actual start minus scheduled time |
| `tripora_runs{state="due"}` | Queue depth: runs due now |
| `tripora_runs{state="running"}`, `{state="retrying"}`, `{state="dead_lettered"}`, `{state="paused"}` | Run counts by state, cached 5 s |
| `tripora_runs_retried_total`, `tripora_runs_dead_lettered_total` | Retries and dead letters |
| `tripora_runs_throttled_total` | Runs held back by a tenant's cap |
| `tripora_stale_writes_rejected_total` | Writes rejected by the fencing check |
| `tripora_errors_total{operation}` | Errors by operation |
| `tripora_http_5xx_total` | Server errors on the API |

**Job fields:**
- `type`: `alert`, `roster`, `no_show`, `billing` or `noop`.
- `payload`: JSON.
- `cron` (5 fields) or `run_at`.
- `timezone`: an IANA zone.
- `misfire_policy`: `COALESCE` (default), `FIRE_NOW` or `SKIP`.
- `priority`.
- `max_runtime_ms`.

## Tests

```bash
docker compose up -d postgres
export TEST_DATABASE_URL='postgres://tripora:tripora@localhost:5432/tripora?sslmode=disable'
go test -race ./...            # about a minute; each test gets its own database
scripts/chaos.sh               # kill -9 and SIGSTOP against the real docker cluster
```

| Requirement | Test |
|---|---|
| kill -9 while sending 1,200 alerts: takeover within the lease, one alert each | `TestKilledNodeIsTakenOverAndEveryAlertArrivesOnce`, `scripts/chaos.sh kill` |
| Node frozen past its lease: its stale write is rejected by the fencing token | `TestFrozenNodeIsFencedWhenItResumes`, `TestStaleOwnerCannotCommit`, `scripts/chaos.sh freeze` |
| Concurrency: leases are exclusive; 3 nodes deliver 1,200 alerts with zero repeats | `TestConcurrentLeaseAcquisitionIsExclusive`, `TestThreeNodesDeliverEachAlertOnce` |
| Fairness and per-tenant cap | `TestNoisyTenantDoesNotDelayQuietTenant`, `TestClaimIsFairAndRespectsCapAndPause` |
| Retries, dead letters, replay | `TestFailingRunRetriesThenDeadLettersThenReplays`, `TestDeadLetterAndReplay` |
| Max run time | `TestMaxRuntimeIsEnforced` |
| Misfire policies | `TestMisfirePolicies`, `TestMisfirePolicyAfterDowntime` |
| DST gap and overlap | `TestSpringForwardSkipsMissingWallTime`, `TestFallBackRunsOnceAtFirstOccurrence` |
| Recurring schedule survives crashes | `TestClaimStoresNextOccurrence` |
| Tenant isolation, roles, safe errors | `internal/api/api_test.go` |

## Load test: 100,000 jobs in one 10-minute window, three nodes

```bash
docker compose up -d --build
go run ./cmd/loadtest -jobs 100000 -window 10m   # half the jobs belong to one hot tenant
```

Measured on an Intel i5-10300H laptop (8 threads, 23 GB RAM, Fedora 44). PostgreSQL 18 and all three nodes ran in Docker on the same machine:

| | |
|---|---|
| runs | 100,000 succeeded, 0 dead-lettered |
| throughput | 166.7 runs/s (the offered rate; the cluster kept up for the whole window) |
| fire lag, all tenants | p50 0.068 s, p95 0.196 s, **p99 0.573 s**, max 4.748 s |
| fire lag, hot tenant (50% of jobs) | p50 0.068 s, p99 1.188 s |
| fire lag, the other 49 tenants | p50 0.067 s, p99 0.346 s |
| delivery lag (receiver) | p50 0.082 s, p99 1.576 s |
| exactly once | 100,000 alerts delivered once each, 0 re-sends |

Result: **PASS** (p99 fire lag under 2 s).

**Burst fairness** (the PDF's "50,000 alerts at 8:00 AM must not delay another client's 20"):

```bash
go run ./cmd/loadtest -burst -jobs 50000 -quiet 20   # 50,000 + 20 alerts, all due at the same instant
```

| | |
|---|---|
| runs | 50,020 succeeded, 0 dead-lettered, each alert delivered exactly once |
| quiet tenant (20 alerts) | p50 0.034 s, **p99 0.086 s**; its last alert was delivery **#80 of 50,020** |
| hot tenant (50,000 alerts) | drained at 480 runs/s, capped at 100 running at a time; p50 56 s, p99 102 s |

Result: **PASS**. The flood is queued behind its own cap, and the other tenant is served first.

**Chaos runs** against the real docker cluster (`scripts/chaos.sh`, 1,200 alerts each):

| Scenario | Result |
|---|---|
| `kill -9 node-1` while alerts fire | 1,200 of 1,200 delivered once each. Lag during takeover p99 10.4 s (the 10 s lease) |
| `SIGSTOP node-2` for 30 s, then `SIGCONT` | 1,200 of 1,200 delivered once each. node-2's **5 stale writes rejected** by the fencing token; 5 re-sends ignored by the receiver |

**Claim-path benchmark** (one node, database work only, no handler):

```bash
go test ./internal/store -run x -bench Claim -benchtime 3x
```

Result: about 1,110 runs/s per node.

## Trade-offs (short)

- **Consistency over availability.** All claims and commits go through one PostgreSQL primary and a fencing check. If the database is down, nothing runs, but nothing runs twice either.
- **Latency vs load.** Polling every 100 ms gives sub-second lag with simple code, at the cost of steady idle queries.
- **Re-sends instead of per-recipient tracking.** After a failover or rebalance a run is sent again under the same key and the receiver drops the repeat. That is simpler, with extra downstream calls only during failures.
- **Left out:** multi-region, job update/delete, retention of old runs, payload encryption. See [docs/design.md](docs/design.md) for details.

## Assumptions

- Downstream services honour `Idempotency-Key`. The bundled receiver does, and counts the repeats it ignored.
- Handlers respect their context deadline (the built-in ones do).
- One PostgreSQL primary is the source of truth; 64 shards are enough for the node counts we expect.

## Why Go

The PDF prefers Java but allows Go if the choice is defended. This problem is mostly concurrency: leases, worker pools, timeouts, races between nodes. Go's goroutines, `context` deadlines and race detector make that code short and testable, and every test here runs under `-race`. Nodes are single static binaries that start in milliseconds, which matters when nodes come and go.

The layering maps directly:

| Layer | Package |
|---|---|
| controller | `internal/api` |
| service | `internal/service` |
| repository | `internal/store` |
| node | `internal/scheduler` |

The scheduling rules (`internal/schedule`: DST and misfire) are plain functions, tested without a database.

## Layout

```text
cmd/tripora         node: API + scheduler
cmd/receiver        idempotent notification receiver (for compose and chaos tests)
cmd/loadtest        seeds jobs, measures fire lag, checks exactly-once
cmd/token           issues local JWTs
internal/domain     types, keys, shard placement, validation errors
internal/schedule   cron on tenant time, DST, misfire policy
internal/store      PostgreSQL: schema, leases, claim, fenced finish, recovery
internal/scheduler  the node loop
internal/service    use cases behind the API
internal/api        HTTP, JWT auth, error mapping
internal/metrics    Prometheus text metrics
internal/notify     notification sender (HTTP with Idempotency-Key)
internal/testdb     throwaway databases and node freeze/kill for tests
scripts/chaos.sh    kill -9 and SIGSTOP scenarios against docker compose
```

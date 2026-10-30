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


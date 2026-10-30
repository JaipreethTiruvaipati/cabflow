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


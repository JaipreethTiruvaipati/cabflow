# Tripora: distributed trip-job scheduler

Tripora is the MoveInSync case study 3 solution. It runs timed jobs (rosters at 10 PM tenant time, "your cab is assigned" alerts, no-show cancels, billing close) on a cluster of three or more nodes. Any node can be killed or frozen at any time; no job is lost and no alert is sent twice.

It is Go and PostgreSQL, nothing else. See [docs/design.md](docs/design.md) for the architecture, the freeze/fencing sequence, the failure-mode table and the trade-offs.


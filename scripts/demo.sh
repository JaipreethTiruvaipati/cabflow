#!/usr/bin/env bash
# The demo from the video. Every command really runs against the local
# cluster; `run` just echoes it like it's being typed first.
#
#   docker compose up -d --build     # once, before recording
#   asciinema rec --headless --window-size 130x36 -c scripts/demo.sh demo.cast
set -uo pipefail
cd "$(dirname "$0")/.."

TENANT=00000000-0000-0000-0000-000000000001

typed() {
  local text="$1"
  for ((i = 0; i < ${#text}; i++)); do
    printf '%s' "${text:i:1}"
    sleep 0.012
  done
  printf '\n'
}

say() {
  printf '\n\033[1;36m# %s\033[0m\n' "$1"
  sleep 1.5
}

run() {
  printf '\033[1;32m$\033[0m '
  typed "$1"
  eval "$1"
  sleep 2
}

say "Tripora: 3 identical Go nodes + PostgreSQL. One command starts it all."
run "docker compose up -d"
run "docker compose ps --format 'table {{.Name}}\t{{.Status}}'"

say "The 3 nodes split the 64 shards between them."
run "for p in 8081 8082 8083; do curl -s localhost:\$p/metrics | grep '^tripora_owned_shards'; done"

say "No token, no access."
run "curl -s localhost:8081/v1/jobs; echo"
run "export TOKEN=\$(go run ./cmd/token -secret local-development-secret -roles admin); H=\"Authorization: Bearer \$TOKEN\""

say "Recurring job: tomorrow's roster at 22:00 India time."
run "JOB=\$(curl -s -H \"\$H\" localhost:8081/v1/jobs -d '{\"type\":\"roster\",\"payload\":{\"kind\":\"tomorrow\"},\"cron\":\"0 22 * * *\",\"timezone\":\"Asia/Kolkata\"}' | jq -r .id); echo \$JOB"
say "Next fire times, asked from a different node: 22:00 IST is 16:30 UTC."
run "curl -s -H \"\$H\" \"localhost:8082/v1/jobs/\$JOB/next-fire-times?limit=3\" | jq ."

say "Manual trigger, sent twice with the same Idempotency-Key: same run both times."
run "curl -s -H \"\$H\" -H 'Idempotency-Key: demo-1' -X POST localhost:8083/v1/jobs/\$JOB/trigger | jq -c '{id, status}'"
run "curl -s -H \"\$H\" -H 'Idempotency-Key: demo-1' -X POST localhost:8081/v1/jobs/\$JOB/trigger | jq -c '{id, status}'"
run "sleep 1; curl -s -H \"\$H\" localhost:8081/v1/jobs/\$JOB/runs | jq -c '.[] | {status, manual, scheduled_at}'"

say "One-off pickup alert, due in 10 seconds."
run "AT=\$(date -u -d '+10 seconds' +%Y-%m-%dT%H:%M:%SZ); BODY=\$(jq -nc --arg at \"\$AT\" '{type:\"alert\", payload:{employee:\"e-42\"}, run_at:\$at}'); A=\$(curl -s -H \"\$H\" localhost:8081/v1/jobs -d \"\$BODY\" | jq -r .id); echo due at \$AT"
run "sleep 12; curl -s -H \"\$H\" localhost:8081/v1/jobs/\$A/runs | jq -c '.[] | {status, attempt, scheduled_at, started_at}'"

say "Operator controls: pause and resume a tenant, list dead letters."
run "curl -s -H \"\$H\" -X POST localhost:8081/v1/tenants/$TENANT/pause; echo"
run "curl -s -H \"\$H\" -X POST localhost:8081/v1/tenants/$TENANT/resume; echo"
run "curl -s -H \"\$H\" localhost:8081/v1/dead-letters; echo"

say "Roles: an employee can read but not create, and never sees payloads."
run "E=\"Authorization: Bearer \$(go run ./cmd/token -secret local-development-secret -roles employee)\""
run "curl -s -H \"\$E\" localhost:8081/v1/jobs -d '{\"type\":\"alert\",\"payload\":{}}'; echo"
run "curl -s -H \"\$E\" localhost:8081/v1/jobs/\$JOB | jq -c '{id, type, payload}'"

say "Live failure: 1,200 alerts firing, node-2 gets SIGSTOP for 30 s, then SIGCONT."
say "The others take its shards after the 10 s lease; node-2's late writes must be fenced off."
run "scripts/chaos.sh freeze"
run "docker compose logs node-2 | grep 'stale write rejected' | head -3"

say "On-call metrics: fire lag histogram, run counts, retries, fenced writes."
run "curl -s localhost:8081/metrics | grep -E '^tripora_(fire_lag_seconds_bucket|runs\\{|stale|runs_retried|runs_throttled)'"

say "Measured results (100k jobs in 10 min, 50k burst, chaos runs) are in the README."
run "sed -n '/^## Load test/,/^## Trade-offs/p' README.md | head -40"

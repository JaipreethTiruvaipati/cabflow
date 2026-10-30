#!/usr/bin/env bash
# The two failure drills from the case study, run against the compose cluster:
#   kill    kill -9 node-1 while 1,200 pickup alerts are firing
#   freeze  SIGSTOP node-2 for 30 s, then SIGCONT
# loadtest seeds the alerts and checks that each one arrived exactly once; it
# exits non-zero if not, which fails this script.
set -euo pipefail
cd "$(dirname "$0")/.."

DB="postgres://tripora:tripora@localhost:5432/tripora?sslmode=disable"

seed_alerts() { # 1,200 alerts due over $1, first one in 10 s; runs in the background
  go run ./cmd/loadtest -db "$DB" -jobs 1200 -tenants 1 -start-in 10s -window "$1" -max-p99 60s &
  LOADTEST=$!
}

wait_ready() {
  for port in 8081 8082 8083; do
    until curl -sf "localhost:$port/readyz" >/dev/null; do sleep 1; done
  done
}

stale_rejections() {
  curl -s "localhost:$1/metrics" | awk '/^tripora_stale_writes_rejected_total/ {print $2}'
}

kill_scenario() {
  echo "== kill -9 node-1 while alerts fire"
  seed_alerts 10s
  sleep 13
  docker compose kill -s SIGKILL node-1
  wait "$LOADTEST"
  docker compose up -d node-1
}

freeze_scenario() {
  echo "== SIGSTOP node-2 for 30 s while alerts fire"
  # Slow the receiver down (300 ms per alert) so node-2 is definitely mid-send
  # when we stop it. Otherwise there may be nothing for the fence to reject.
  RECEIVER_DELAY_MS=300 docker compose up -d receiver
  seed_alerts 20s
  sleep 13
  docker compose kill -s SIGSTOP node-2
  sleep 30
  docker compose kill -s SIGCONT node-2
  wait "$LOADTEST"
  sleep 2
  echo "node-2 stale writes rejected after resuming: $(stale_rejections 8082)"
  docker compose up -d receiver
}

docker compose up -d --build
wait_ready
case "${1:-all}" in
  kill) kill_scenario ;;
  freeze) freeze_scenario ;;
  all) kill_scenario; wait_ready; freeze_scenario ;;
  *) echo "usage: $0 [kill|freeze|all]" >&2; exit 2 ;;
esac

#!/usr/bin/env bash
# Launches the full 8.1/8.2/8.3 demo stack: docker compose, embedding preload,
# the five HTTP services, the two stage-16 daemons, and the static dashboard
# server. Mirrors the command sequence in CLAUDE.md's "8.3 ... minimal demo
# slice" section exactly -- this script doesn't invent a new way to start
# anything, it just runs those same commands in order and manages them.
#
# Ctrl+C (or any exit) stops everything this script started. Logs for the
# background processes go to demo/.run_logs/ (gitignored).
#
# Usage: demo/run_demo.sh [--skip-compose] [--skip-preload] [--seed-demo-session]
#   --skip-compose       don't run `docker compose up -d` (stack already up)
#   --skip-preload       don't re-run preload_embeddings_to_redis.py
#   --seed-demo-session  feed the "demo-rock-thread" session (the one
#                        demo/uc82.html and demo/uc83.html default their
#                        session-id field to) a few real events via
#                        event_ingestion, so /sessions/demo-rock-thread
#                        resolves immediately on a fresh stack instead of
#                        404ing with "unknown session" until you drive some
#                        events into it yourself (see demo/seed_demo_session.py)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PY="$ROOT/platform/enrichment/.venv/bin/python"
LOG_DIR="$ROOT/demo/.run_logs"
mkdir -p "$LOG_DIR"

SKIP_COMPOSE=0
SKIP_PRELOAD=0
SEED_DEMO_SESSION=0
for arg in "$@"; do
    case "$arg" in
        --skip-compose) SKIP_COMPOSE=1 ;;
        --skip-preload) SKIP_PRELOAD=1 ;;
        --seed-demo-session) SEED_DEMO_SESSION=1 ;;
        *) echo "unknown argument: $arg" >&2; exit 1 ;;
    esac
done

PIDS=()
NAMES=()

cleanup() {
    echo
    echo "Shutting down demo services..."
    for i in "${!PIDS[@]}"; do
        pid="${PIDS[$i]}"
        name="${NAMES[$i]}"
        if kill -0 "$pid" 2>/dev/null; then
            echo "  stopping $name (pid $pid)"
            kill "$pid" 2>/dev/null || true
        fi
    done
    for pid in "${PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    echo "Done."
}
trap cleanup EXIT INT TERM

start() {
    local name="$1"; shift
    local logfile="$LOG_DIR/${name}.log"
    echo "Starting $name (log: demo/.run_logs/${name}.log)"
    "$@" >"$logfile" 2>&1 &
    PIDS+=("$!")
    NAMES+=("$name")
}

wait_for_health() {
    local name="$1" url="$2" tries="${3:-60}"
    for ((i = 0; i < tries; i++)); do
        if curl -sf "$url" >/dev/null 2>&1; then
            echo "  $name is up"
            return 0
        fi
        sleep 1
    done
    echo "  WARNING: $name did not become healthy at $url within ${tries}s -- check demo/.run_logs/${name}.log" >&2
    return 1
}

if [[ "$SKIP_COMPOSE" -eq 0 ]]; then
    echo "Bringing up docker compose stack..."
    docker compose up -d
    docker compose ps
else
    echo "Skipping docker compose (--skip-compose)"
fi

if [[ "$SKIP_PRELOAD" -eq 0 ]]; then
    echo "Preloading track embeddings into Redis..."
    PYTHONPATH=platform "$PY" platform/streaming/preload_embeddings_to_redis.py
else
    echo "Skipping embedding preload (--skip-preload)"
fi

echo "Starting backend services..."
start semantic_api    "$PY" -m uvicorn semantic_api.main:app --app-dir platform --port 8000
start session_api     "$PY" -m uvicorn session_api.main:app --app-dir platform --port 8030
start event_ingestion "$PY" -m uvicorn event_ingestion.main:app --app-dir platform --port 8020
start demo_api        "$PY" -m uvicorn demo_api:app --app-dir usecases/8_1_batch_reactive --port 8010
PYTHONPATH=platform start proactive_service "$PY" -m uvicorn proactive_service:app --app-dir usecases/8_3_streaming_proactive --port 8040

wait_for_health semantic_api    http://127.0.0.1:8000/health
wait_for_health session_api     http://127.0.0.1:8030/health
wait_for_health event_ingestion http://127.0.0.1:8020/health
wait_for_health demo_api        http://127.0.0.1:8010/health
wait_for_health proactive_service http://127.0.0.1:8040/health

echo "Starting stage-16 daemons..."
PYTHONPATH=platform start session_consumer_daemon "$PY" -m streaming.session_consumer_daemon
PYTHONPATH=platform start refresh_daemon "$PY" -m streaming.refresh_daemon --semantic-api-url http://127.0.0.1:8000

# Daemons have no /health endpoint; give them a moment to attach their
# Kafka consumer groups before the dashboard starts driving traffic.
sleep 2

if [[ "$SEED_DEMO_SESSION" -eq 1 ]]; then
    echo "Seeding demo-rock-thread session..."
    "$PY" demo/seed_demo_session.py
    echo "  waiting for the daemons to catch up..."
    sleep 5
    if curl -sf http://127.0.0.1:8030/sessions/demo-rock-thread >/dev/null 2>&1; then
        echo "  demo-rock-thread is live on session_api"
    else
        echo "  WARNING: demo-rock-thread still not resolving on session_api -- check demo/.run_logs/session_consumer_daemon.log and refresh_daemon.log" >&2
    fi
fi

echo "Starting static dashboard server on http://localhost:8080 ..."
start dashboard_server "$PY" -m http.server 8080 --directory demo
sleep 1

cat <<'EOF'

========================================================================
Demo stack is up. Open http://localhost:8080

  Reminder (per CLAUDE.md): these two daemons hold the canonical Kafka
  consumer groups. Running the test suite while they're up will make
  several stage 9-16 tests fail (message-stream contention, not a real
  bug). Stop this script (Ctrl+C) before running pytest.
========================================================================

Press Ctrl+C to stop all services started by this script.
EOF

wait

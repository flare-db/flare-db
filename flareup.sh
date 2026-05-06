#!/usr/bin/env bash
#run /home/ganesh/flare/harness/flare/flareup.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${SCRIPT_DIR}"
LOG_DIR="${REPO_DIR}/logs"
FLARE_LOG="${LOG_DIR}/flare-server.log"

if ! command -v java >/dev/null 2>&1; then
    echo "java is required but was not found on PATH."
    exit 1
fi
if ! command -v cargo >/dev/null 2>&1; then
    echo "cargo is required but was not found on PATH."
    exit 1
fi

mkdir -p "${LOG_DIR}"

cleanup() {
    if [[ -n "${SERVER_PID:-}" ]] && kill -0 "${SERVER_PID}" >/dev/null 2>&1; then
        kill "${SERVER_PID}" >/dev/null 2>&1 || true
        wait "${SERVER_PID}" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

cd "${REPO_DIR}"

echo "Starting Flare server on localhost:8099..."
RUST_LOG=info cargo run -p flare >"${FLARE_LOG}" 2>&1 &
SERVER_PID=$!

port_ready() {
    if command -v nc >/dev/null 2>&1; then
        nc -z localhost 8099 >/dev/null 2>&1
    else
        (echo >/dev/tcp/localhost/8099) >/dev/null 2>&1
    fi
}

echo "Waiting for Flare server to be ready..."
for _ in {1..60}; do
    if port_ready; then
        break
    fi
    sleep 0.5
done

if ! port_ready; then
    echo "Flare server did not become ready on localhost:8099."
    echo "Check logs: ${FLARE_LOG}"
    exit 1
fi

echo "Flare server ready."
echo ""
echo "  Flare log : tail -f \"${FLARE_LOG}\""
echo "  Worker logs will be created per job by flare run() in ${LOG_DIR}"
echo ""
echo "Flare is ready. Submit a job; harness will be spawned after artifact staging."
wait "${SERVER_PID}"

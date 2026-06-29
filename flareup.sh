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

echo "Starting FlareDB server on localhost:8099..."
# ensure default base directory exists and pass it to flaredb as first arg
BASE_DIR="${HOME}/.flaredb"
mkdir -p "${BASE_DIR}"

# generate a new instance id every time the script runs
if command -v uuidgen >/dev/null 2>&1; then
    INSTANCE_ID="$(uuidgen)"
else
    INSTANCE_ID="$(date +%s)-$$"
fi

# create per-instance directory and logs dir
INSTANCE_DIR="${BASE_DIR}/${INSTANCE_ID}"
INSTANCE_LOG_DIR="${INSTANCE_DIR}/logs"
mkdir -p "${INSTANCE_LOG_DIR}"
FLARE_LOG="${INSTANCE_LOG_DIR}/flare-server.log"

# launch flaredb with the base dir arg and set FLAREDB_INSTANCE_ID for the server process
RUST_LOG=info FLAREDB_INSTANCE_ID="${INSTANCE_ID}" cargo run -p flaredb -- "${BASE_DIR}" >"${FLARE_LOG}" 2>&1 &
SERVER_PID=$!

port_ready() {
    if command -v nc >/dev/null 2>&1; then
        nc -z localhost 8099 >/dev/null 2>&1
    else
        (echo >/dev/tcp/localhost/8099) >/dev/null 2>&1
    fi
}

echo "Waiting for Flare server to start..."
for _ in {1..60}; do
    if port_ready; then
        break
    fi
    sleep 0.5
done

if ! port_ready; then
    echo "Flare server failed to start on localhost:8099."
    echo "Check logs: ${FLARE_LOG}"
    exit 1
fi

echo "Flared up! 🔥🔥"
echo ""
echo "  FlareDB server logs : ${FLARE_LOG}"
echo "  Instance Dir        : ${INSTANCE_DIR}"
echo "  Worker logs         : will be availabe during execution at ${INSTANCE_DIR} under unique jobid"
echo ""
echo "FlareDB is ready."
echo "SDK workers will be started automatically when jobs are submitted from the Runner SDK."
wait "${SERVER_PID}"

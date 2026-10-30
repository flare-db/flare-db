#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${SCRIPT_DIR}"
LOG_DIR="${REPO_DIR}/logs"
FLARE_LOG="${LOG_DIR}/flare-server.log"

# Version should match the CLI constant FLAREDB_VERSION in src/main.rs
FLAREDB_VERSION="0.1.8"

# Build type (default: debug)
BUILD_TYPE="debug"
# Empty means: use RUST_LOG if set, otherwise the build-type default.
LOG_LEVEL=""

usage() {
    cat <<'EOF'
Usage: ./flareup-dev.sh [--debug|--release] [--log-level LEVEL]

  --debug          Use target/debug/flaredb (default)
  --release        Use target/release/flaredb
  --log-level L    Log level for the flaredb crate: debug, info, trace, ...
                   Other crates stay at info. A value containing '=' or ','
                   is passed to RUST_LOG as a full filter.
  -h, --help       Show this help.

The RUST_LOG environment variable is honoured when --log-level is not given.

Environment:
  FLAREDB_BASE_DIR  Base directory (default: ~/.flaredb). Use an isolated
                    directory for test runs to avoid Paimon table collisions.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --release)
            BUILD_TYPE="release"
            shift
            ;;
        --debug)
            BUILD_TYPE="debug"
            shift
            ;;
        --log-level)
            if [[ $# -lt 2 || -z "${2:-}" ]]; then
                echo "--log-level requires a value (e.g. debug or info)" >&2
                usage >&2
                exit 2
            fi
            LOG_LEVEL="$2"
            shift 2
            ;;
        --log-level=*)
            LOG_LEVEL="${1#*=}"
            if [[ -z "${LOG_LEVEL}" ]]; then
                echo "--log-level requires a value (e.g. debug or info)" >&2
                usage >&2
                exit 2
            fi
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

# Log level precedence: --log-level, then RUST_LOG, then the build-type default.
if [[ -n "${LOG_LEVEL}" ]]; then
    if [[ "${LOG_LEVEL}" == *=* || "${LOG_LEVEL}" == *,* ]]; then
        SERVER_RUST_LOG="${LOG_LEVEL}"
    elif [[ "${LOG_LEVEL}" == "info" ]]; then
        SERVER_RUST_LOG="info"
    else
        SERVER_RUST_LOG="flaredb=${LOG_LEVEL},info"
    fi
elif [[ -n "${RUST_LOG:-}" ]]; then
    SERVER_RUST_LOG="${RUST_LOG}"
elif [[ "${BUILD_TYPE}" == "debug" ]]; then
    SERVER_RUST_LOG="flaredb=debug,info"
else
    SERVER_RUST_LOG="info"
fi

if ! command -v java >/dev/null 2>&1; then
    echo "java is required but was not found on PATH."
    exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required but was not found on PATH."
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

# ensure default base directory exists and pass it to flaredb as first arg
BASE_DIR="${FLAREDB_BASE_DIR:-${HOME}/.flaredb}"
mkdir -p "${BASE_DIR}"

# Setup bin/instances and worker jar
BIN_DIR="${BASE_DIR}/bin"
INSTANCES_DIR="${BASE_DIR}/instances"
mkdir -p "${BIN_DIR}" "${INSTANCES_DIR}"

WORKER_JAR_NAME="beam-sdks-java-harness-2.76.0-flare-bundled.jar"
WORKER_JAR="${BIN_DIR}/${WORKER_JAR_NAME}"
WORKER_JAR_URL="https://github.com/flare-db/flare-db/releases/download/beam-worker-2.76.0/${WORKER_JAR_NAME}"

# Download worker jar if it doesn't exist (show progress bar)
if [[ ! -f "${WORKER_JAR}" ]]; then
    echo "Downloading worker jar from ${WORKER_JAR_URL}..."
    if ! curl --fail --location --progress-bar --output "${WORKER_JAR}" "${WORKER_JAR_URL}"; then
        echo "Failed to download worker jar. Please check your internet connection and try again."
        exit 1
    fi
    echo "\nWorker jar downloaded successfully to ${WORKER_JAR}"
else
    echo "Worker jar already exists at ${WORKER_JAR}"
fi

# Prefer local build binary for development; fall back to bin/flaredb-<version> if present
LOCAL_FLAREDB_BINARY_DEBUG="${REPO_DIR}/target/debug/flaredb"
LOCAL_FLAREDB_BINARY_RELEASE="${REPO_DIR}/target/release/flaredb"

case "${BUILD_TYPE}" in
    release)
        if [[ -x "${LOCAL_FLAREDB_BINARY_RELEASE}" ]]; then
            FLAREDB_BINARY="${LOCAL_FLAREDB_BINARY_RELEASE}"
        elif [[ -x "${LOCAL_FLAREDB_BINARY_DEBUG}" ]]; then
            FLAREDB_BINARY="${LOCAL_FLAREDB_BINARY_DEBUG}"
        elif [[ -x "${BIN_DIR}/flaredb-${FLAREDB_VERSION}" ]]; then
            FLAREDB_BINARY="${BIN_DIR}/flaredb-${FLAREDB_VERSION}"
        else
            echo "Release binary not found. Build it with:"
            echo "  cargo build --release -p flaredb"
            exit 1
        fi
        ;;
    debug)
        if [[ -x "${LOCAL_FLAREDB_BINARY_DEBUG}" ]]; then
            FLAREDB_BINARY="${LOCAL_FLAREDB_BINARY_DEBUG}"
        elif [[ -x "${LOCAL_FLAREDB_BINARY_RELEASE}" ]]; then
            FLAREDB_BINARY="${LOCAL_FLAREDB_BINARY_RELEASE}"
        elif [[ -x "${BIN_DIR}/flaredb-${FLAREDB_VERSION}" ]]; then
            FLAREDB_BINARY="${BIN_DIR}/flaredb-${FLAREDB_VERSION}"
        else
            echo "Debug binary not found. Build it with:"
            echo "  cargo build -p flaredb"
            exit 1
        fi
        ;;
esac

echo "Using ${BUILD_TYPE} build: ${FLAREDB_BINARY}"
echo "Server log level: ${SERVER_RUST_LOG}"

# generate a new instance id every time the script runs
if command -v uuidgen >/dev/null 2>&1; then
    INSTANCE_ID="$(uuidgen)"
else
    INSTANCE_ID="$(date +%s)-$$"
fi

# create per-instance directory and logs dir
INSTANCE_DIR="${INSTANCES_DIR}/${INSTANCE_ID}"
INSTANCE_LOG_DIR="${INSTANCE_DIR}/logs"
mkdir -p "${INSTANCE_LOG_DIR}"
FLARE_LOG="${INSTANCE_LOG_DIR}/flare-server.log"

# launch flaredb binary with the base dir arg and set FLAREDB_INSTANCE_ID and WORKER_JAR_PATH for the server process
RUST_LOG="${SERVER_RUST_LOG}" FLAREDB_INSTANCE_ID="${INSTANCE_ID}" WORKER_JAR_PATH="${WORKER_JAR}" "${FLAREDB_BINARY}" "${BASE_DIR}" >"${FLARE_LOG}" 2>&1 &
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
echo "  Instance ID         : ${INSTANCE_ID}"
echo "  FlareDB server logs : ${FLARE_LOG}"
echo "  Worker logs path    : ${INSTANCE_LOG_DIR}/jobs/<job-id>/logs/flare-worker.log"
echo "                        Check this location for worker logs after submitting jobs."
echo ""
echo "FlareDB is ready."
echo "SDK workers will be started automatically when jobs are submitted from the Runner SDK."
wait "${SERVER_PID}"

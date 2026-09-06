#!/usr/bin/env bash
# End-to-end CliniClaw demo setup, from scratch, with the local vLLM model.
# Walks the README steps in order, delegating each one to the script that
# already implements it.
#
# Usage: ./bac_setup.sh [options]
#   --skip-data   Do not download/extract data.zip
#   --skip-load   Do not upload the Synthea bundles to FHIR
#   --skip-vllm   Assume a vLLM server is already listening on VLLM_PORT
#
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

NEMOCLAW_TAG="v0.0.97"
DATA_URL="https://huggingface.co/datasets/aalmahrooqi/synthetic-fhir-data/resolve/main/data.zip"
VLLM_PORT="${VLLM_PORT:-8000}"
VLLM_LOG="${SCRIPT_DIR}/logs/vllm.log"
VLLM_TIMEOUT="${VLLM_TIMEOUT:-3600}"   # the first run also downloads the model weights

SKIP_DATA=0
SKIP_LOAD=0
SKIP_VLLM=0

while [ $# -gt 0 ]; do
    case "$1" in
        --skip-data) SKIP_DATA=1 ;;
        --skip-load) SKIP_LOAD=1 ;;
        --skip-vllm) SKIP_VLLM=1 ;;
        -h|--help)   sed -n '2,9p' "$0"; exit 0 ;;
        *)           echo "Unknown option: $1 (use -h for help)" >&2; exit 1 ;;
    esac
    shift
done

# ── Prerequisites ──────────────────────────────────────────────────────────
for cmd in docker python3 curl unzip; do
    command -v "${cmd}" >/dev/null 2>&1 || { echo "ERROR: '${cmd}' not found on PATH." >&2; exit 1; }
done
docker info >/dev/null 2>&1 || { echo "ERROR: cannot reach the Docker daemon." >&2; exit 1; }

if ! command -v nemoclaw >/dev/null 2>&1; then
    echo "--- Installing NemoClaw ${NEMOCLAW_TAG} ---"
    # The installer always finishes by onboarding a sandbox of its own, and on a
    # DGX Spark it defaults to an "express install" that creates `my-assistant`
    # with its own managed vLLM. NEMOCLAW_NO_EXPRESS=1 keeps it off that path;
    # all we want here is the CLI, so its onboarding result is ignored and
    # setup_helper.sh onboards hermes-health from our own Dockerfile instead.
    curl -fsSL https://www.nvidia.com/nemoclaw.sh | \
        NEMOCLAW_INSTALL_REF= \
        NEMOCLAW_INSTALL_TAG="${NEMOCLAW_TAG}" \
        NEMOCLAW_NO_EXPRESS=1 \
        NEMOCLAW_NON_INTERACTIVE=1 \
        NEMOCLAW_ACCEPT_THIRD_PARTY_SOFTWARE=1 \
        bash || echo "    (installer onboarding skipped/failed — only the CLI is needed here)"
    export PATH="${HOME}/.nemoclaw/bin:${HOME}/.local/bin:${PATH}"
    command -v nemoclaw >/dev/null 2>&1 || {
        echo "ERROR: nemoclaw is installed but not on PATH — open a new shell and re-run." >&2
        exit 1
    }
fi

# ── 1. Sample data ─────────────────────────────────────────────────────────
if [ "${SKIP_DATA}" -eq 1 ] || compgen -G "data/*.json" >/dev/null; then
    echo "--- Sample data already in data/, skipping download ---"
else
    echo "--- Downloading Synthea sample data ---"
    [ -f data.zip ] || curl -L -o data.zip "${DATA_URL}"
    unzip -q -o data.zip
fi

# ── 2. FHIR server ─────────────────────────────────────────────────────────
if docker ps -q -f "name=^health-demo-hapi$" -f status=running | grep -q .; then
    echo "--- FHIR server already running, skipping ---"
else
    echo "--- Starting the FHIR server ---"
    docker compose up -d
fi

# ── 3. Load the Synthea bundles ────────────────────────────────────────────
# load_synthea.py waits for the server to accept requests before uploading.
# if [ "${SKIP_LOAD}" -eq 1 ]; then
#     echo "--- Skipping the data load (--skip-load) ---"
# else
#     # sleep for 30 s
#     sleep 30
#     echo "--- Loading the Synthea bundles (takes a few minutes) ---"
#     python3 scripts/load_synthea.py --data-dir data
# fi

# ── 4. Local vLLM ──────────────────────────────────────────────────────────
if [ "${SKIP_VLLM}" -eq 1 ]; then
    echo "--- Skipping vLLM startup (--skip-vllm) ---"
elif curl -fs "http://localhost:${VLLM_PORT}/v1/models" >/dev/null 2>&1; then
    echo "--- vLLM already serving on port ${VLLM_PORT} ---"
else
    echo "--- Starting local vLLM ---"
    mkdir -p "$(dirname "${VLLM_LOG}")"
    nohup ./scripts/run_vllm.sh >"${VLLM_LOG}" 2>&1 </dev/null &
    VLLM_PID=$!
    echo "    logs: ${VLLM_LOG}"
    echo "    the first run pulls the image and the model weights, so give it time"

    deadline=$(( $(date +%s) + VLLM_TIMEOUT ))
    until curl -fs "http://localhost:${VLLM_PORT}/v1/models" >/dev/null 2>&1; do
        if ! kill -0 "${VLLM_PID}" 2>/dev/null; then
            echo "ERROR: run_vllm.sh exited before the server came up. Last lines of ${VLLM_LOG}:" >&2
            tail -n 30 "${VLLM_LOG}" >&2
            exit 1
        fi
        if [ "$(date +%s)" -ge "${deadline}" ]; then
            echo "ERROR: vLLM was not ready within ${VLLM_TIMEOUT}s. See ${VLLM_LOG}." >&2
            exit 1
        fi
        sleep 15
    done
    echo "[OK] vLLM ready on port ${VLLM_PORT}"
fi

# ── 5. Hermes sandbox + dashboard ──────────────────────────────────────────
# startup_w_local_model.sh exports the vLLM provider settings and hands off to
# setup_helper.sh, which brings up the dashboard, builds the image and onboards.
# `nemoclaw onboard` prefers the managed systemd gateway, which on this host
# starts but never answers its gRPC health check, so onboarding dies on the
# gateway health deadline. A hand-spawned gateway does come up, and onboarding
# reuses whichever healthy gateway already holds port 8080 — so start one here
# when the port is free, and leave an existing gateway alone.
if (exec 3<>/dev/tcp/127.0.0.1/8080) 2>/dev/null; then
    echo "--- OpenShell gateway already listening on port 8080 ---"
else
    echo "--- Starting the OpenShell gateway ---"
    # A failed onboarding leaves the managed unit restarting, and it wants the
    # same port. Stop it so the hand-spawned gateway is the only owner of 8080.
    if command -v systemctl >/dev/null 2>&1; then
        systemctl --user stop nemoclaw-openshell-gateway 2>/dev/null || true
        systemctl --user reset-failed nemoclaw-openshell-gateway 2>/dev/null || true
    fi
    ./scripts/fix_gateway_issues.sh
fi

echo "--- Building the Hermes image and onboarding the sandbox ---"
./scripts/startup_w_local_model.sh

echo ""
echo "============================================="
echo "|       Demo is up! 🚀                      |"
echo "|       Dashboard: http://127.0.0.1:8090    |"
echo "============================================="
echo ""
echo "Tear everything down with ./scripts/shutdown.sh"
echo ""

#!/usr/bin/env bash
# Hermes agent setup for OpenShell (health demo).
# SPDX-License-Identifier: Apache-2.0

docker compose -f docker-compose.yml -f docker-compose.dashboard.yml up -d

set -euo pipefail
export NEMOCLAW_TRUSTED_PRIVATE_INFERENCE_HOSTS="inference-api.nvidia.com"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SANDBOX_NAME="hermes-health"
NEMOCLAW_SOURCE="${NEMOCLAW_SOURCE:-${HOME}/.nemoclaw/source}"
DOCKERFILE="${SCRIPT_DIR}/Dockerfile"
EHR_EXPERT="${SCRIPT_DIR}/agents/ehr-expert"
NEMO_RELAY_PLUGINS_TOML="${SCRIPT_DIR}/plugins/nemo_relay/plugins.toml"
CONTEXT_PATHS=(agents/hermes nemoclaw-blueprint tools/mcp-tool-discovery-runtime src/lib/tool-disclosure.ts src/lib/actions/sandbox/openshell-child-visible-credentials.v0.0.85.json src/lib/messaging scripts)

case "${1:-}" in
    -h|--help) sed -n '2,17p' "$0"; exit 0 ;;
    "") ;;
    *) echo "Unknown option: $1 (use -h for help)" >&2; exit 1 ;;
esac

PINNED_MCP_RUNTIME="${SCRIPT_DIR}/vendor/mcp-tool-discovery-runtime"

command -v nemoclaw >/dev/null 2>&1 || { echo "ERROR: 'nemoclaw' CLI not found on PATH." >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: 'python3' not found on PATH." >&2; exit 1; }
[ -f "${PINNED_MCP_RUNTIME}/package.json" ] || { echo "ERROR: ${PINNED_MCP_RUNTIME}/package.json not found." >&2; exit 1; }
[ -f "${PINNED_MCP_RUNTIME}/package-lock.json" ] || { echo "ERROR: ${PINNED_MCP_RUNTIME}/package-lock.json not found." >&2; exit 1; }
[ -f "${DOCKERFILE}" ] || { echo "ERROR: ${DOCKERFILE} not found." >&2; exit 1; }
[ -d "${NEMOCLAW_SOURCE}" ] || { echo "ERROR: NemoClaw source not found at ${NEMOCLAW_SOURCE}." >&2; exit 1; }
[ -f "${EHR_EXPERT}/SOUL.md" ] || { echo "ERROR: ${EHR_EXPERT}/SOUL.md not found." >&2; exit 1; }
[ -d "${EHR_EXPERT}/skills" ] || { echo "ERROR: ${EHR_EXPERT}/skills not found." >&2; exit 1; }
[ -f "${NEMO_RELAY_PLUGINS_TOML}" ] || { echo "ERROR: ${NEMO_RELAY_PLUGINS_TOML} not found." >&2; exit 1; }

# ── Stage the build context ────────────────────────────────────────────────
# Assemble a throwaway context with just the subtrees the template needs, plus
# the Dockerfile, so `--from` sees the right build context.
STAGING="$(mktemp -d "${TMPDIR:-/tmp}/hermes-build-XXXXXX")"
trap 'rm -rf "${STAGING}"' EXIT

echo "--- Staging build context from ${NEMOCLAW_SOURCE} ---"
for p in "${CONTEXT_PATHS[@]}"; do
    [ -e "${NEMOCLAW_SOURCE}/${p}" ] || { echo "ERROR: missing source subtree: ${p}" >&2; exit 1; }
done
( cd "${NEMOCLAW_SOURCE}" && cp -a --parents "${CONTEXT_PATHS[@]}" "${STAGING}/" )

# ── Overlay advisory-clean MCP runtime pins ────────────────────────────────
# The staged runtime installer ends in `npm audit --omit=dev --audit-level=low`,
# which queries the live advisory database. A lockfile that audited clean when
# nemoclaw was vendored therefore starts failing the image build the day a new
# advisory lands, and only on hosts without a cached layer for that step.
# Overlay the pins that carry the fixes, but refuse to run if nemoclaw's own
# dependency set moved, because the vendored lock would then be stale.
STAGED_MCP_RUNTIME="${STAGING}/tools/mcp-tool-discovery-runtime"
python3 - "${STAGED_MCP_RUNTIME}/package.json" "${PINNED_MCP_RUNTIME}/package.json" <<'PY'
import json, sys

with open(sys.argv[1]) as f: upstream = json.load(f)
with open(sys.argv[2]) as f: pinned = json.load(f)

drift = sorted(k for k in upstream.keys() | pinned.keys()
               if k != "overrides" and upstream.get(k) != pinned.get(k))
if drift:
    sys.exit(f"ERROR: vendored MCP runtime pins are stale (differing keys: {', '.join(drift)}).\n"
             "       Regenerate vendor/mcp-tool-discovery-runtime against the current nemoclaw source.")
PY
cp "${PINNED_MCP_RUNTIME}/package.json" "${STAGED_MCP_RUNTIME}/package.json"
cp "${PINNED_MCP_RUNTIME}/package-lock.json" "${STAGED_MCP_RUNTIME}/package-lock.json"
echo "[OK] Overlaid advisory-clean MCP runtime pins"

mkdir -p "${STAGING}/health-demo"
cp "${EHR_EXPERT}/SOUL.md" "${STAGING}/health-demo/SOUL.md"
cp -a "${EHR_EXPERT}/skills" "${STAGING}/health-demo/skills"
cp "${NEMO_RELAY_PLUGINS_TOML}" "${STAGING}/health-demo/plugins.toml"

cp "${DOCKERFILE}" "${STAGING}/Dockerfile"
echo "[OK] Context staged at ${STAGING}"

# ── Onboard (build image + configure sandbox) ──────────────────────────────
echo "--- nemoclaw onboard (${SANDBOX_NAME}) ---"
NEMOCLAW_SANDBOX_NAME="${SANDBOX_NAME}" \
NEMOCLAW_SANDBOX_GPU=0 \
    nemoclaw onboard \
        --agent hermes \
        --fresh \
        --name "${SANDBOX_NAME}" \
        --non-interactive \
        --from "${STAGING}/Dockerfile"

openshell policy set ${SANDBOX_NAME} --policy "${SCRIPT_DIR}/policy/policy.yaml" --wait
echo "=== Done. Connect with: ==="
echo "  nemoclaw ${SANDBOX_NAME} connect"
echo "  sandbox\$ hermes chat"
echo "  Dashboard served at localhost:8090"

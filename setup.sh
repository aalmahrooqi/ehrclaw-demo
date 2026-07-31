#!/usr/bin/env bash
# Hermes agent setup for OpenShell (health demo).
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
NEMOCLAW_TRUSTED_PRIVATE_INFERENCE_HOSTS=inference-api.nvidia.com 
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SANDBOX_NAME="hermes-health"
NEMOCLAW_SOURCE="${NEMOCLAW_SOURCE:-${HOME}/.nemoclaw/source}"
DOCKERFILE="${SCRIPT_DIR}/Dockerfile"
EHR_EXPERT="${SCRIPT_DIR}/agents/ehr-expert"
NEMO_RELAY_PLUGINS_TOML="${SCRIPT_DIR}/plugins/nemo_relay/plugins.toml"
NEMO_GUARDRAILS_CONFIG="${SCRIPT_DIR}/configs/nemo_guardrails/config.yaml"
GUARDRAILS_WHEELS="${SCRIPT_DIR}/vendor/wheels"
CONTEXT_PATHS=(agents/hermes nemoclaw-blueprint tools/mcp-tool-discovery-runtime src/lib/tool-disclosure.ts src/lib/actions/sandbox/openshell-child-visible-credentials.v0.0.85.json src/lib/messaging scripts)

case "${1:-}" in
    -h|--help) sed -n '2,17p' "$0"; exit 0 ;;
    "") ;;
    *) echo "Unknown option: $1 (use -h for help)" >&2; exit 1 ;;
esac

command -v nemoclaw >/dev/null 2>&1 || { echo "ERROR: 'nemoclaw' CLI not found on PATH." >&2; exit 1; }
[ -f "${DOCKERFILE}" ] || { echo "ERROR: ${DOCKERFILE} not found." >&2; exit 1; }
[ -d "${NEMOCLAW_SOURCE}" ] || { echo "ERROR: NemoClaw source not found at ${NEMOCLAW_SOURCE}." >&2; exit 1; }
[ -f "${EHR_EXPERT}/SOUL.md" ] || { echo "ERROR: ${EHR_EXPERT}/SOUL.md not found." >&2; exit 1; }
[ -d "${EHR_EXPERT}/skills" ] || { echo "ERROR: ${EHR_EXPERT}/skills not found." >&2; exit 1; }
[ -f "${NEMO_RELAY_PLUGINS_TOML}" ] || { echo "ERROR: ${NEMO_RELAY_PLUGINS_TOML} not found." >&2; exit 1; }
[ -f "${NEMO_GUARDRAILS_CONFIG}" ] || { echo "ERROR: ${NEMO_GUARDRAILS_CONFIG} not found." >&2; exit 1; }
ls "${GUARDRAILS_WHEELS}"/annoy-*.whl >/dev/null 2>&1 || { echo "ERROR: prebuilt annoy wheel not found in ${GUARDRAILS_WHEELS} (needed for the nemo_guardrails local backend)." >&2; exit 1; }

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

mkdir -p "${STAGING}/health-demo"
cp "${EHR_EXPERT}/SOUL.md" "${STAGING}/health-demo/SOUL.md"
cp -a "${EHR_EXPERT}/skills" "${STAGING}/health-demo/skills"
cp "${NEMO_RELAY_PLUGINS_TOML}" "${STAGING}/health-demo/plugins.toml"

# Health demo: stage the NeMo Guardrails local backend inputs — the rails config
# as a config dir (nemoguardrails wants config.yml) and the prebuilt annoy wheel
# that satisfies nemoguardrails==0.22.0's annoy dep without a compiler in the
# hardened image.
mkdir -p "${STAGING}/health-demo/guardrails" "${STAGING}/health-demo/wheels"
cp "${NEMO_GUARDRAILS_CONFIG}" "${STAGING}/health-demo/guardrails/config.yml"
cp "${GUARDRAILS_WHEELS}"/annoy-*.whl "${STAGING}/health-demo/wheels/"

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
        --from "${STAGING}/Dockerfile"

openshell policy set ${SANDBOX_NAME} --policy "${SCRIPT_DIR}/policy/policy.yaml" --wait
echo "=== Done. Connect with: ==="
echo "  nemoclaw ${SANDBOX_NAME} connect"
echo "  sandbox\$ hermes chat"


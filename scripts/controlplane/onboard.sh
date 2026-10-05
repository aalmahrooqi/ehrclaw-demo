#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Run the demo's onboarding inside the control plane.
#
# Onboarding is interactive (inference provider + credentials) and NemoClaw
# starts its own gateway while it runs, so the supervisor is paused for the
# duration to keep two gateways off port 8080.

set -euo pipefail

log() { printf '[cliniclaw] %s\n' "$*"; }
die() { printf '[cliniclaw] ERROR: %s\n' "$*" >&2; exit 1; }

CLINICLAW_ROOT="${CLINICLAW_ROOT:?CLINICLAW_ROOT is not set}"
REPO="${CLINICLAW_REPO:?CLINICLAW_REPO is not set}"
export HOME="${CLINICLAW_ROOT}/home"
export PATH="${HOME}/.local/bin:${PATH}"

PAUSE_FILE="${HOME}/.local/state/cliniclaw-supervisor.pause"

[ -d "${REPO}" ] || die "repository is not mounted at ${REPO}"
[ -x "${REPO}/setup.sh" ] || die "${REPO}/setup.sh is missing or not executable"

mkdir -p "$(dirname "${PAUSE_FILE}")"
printf 'onboarding started %s by pid %s\n' "$(date -Is)" "$$" > "${PAUSE_FILE}"
trap 'rm -f "${PAUSE_FILE}"' EXIT

log "supervisor paused; NemoClaw owns the gateway for this run"
log "toolchain: $(cat "${CLINICLAW_DIST:-/opt/cliniclaw/dist}/pin.json")"

# setup.sh stages a build context out of the NemoClaw source tree. That tree
# lives in the image here rather than under $HOME, so point it at the pinned
# copy explicitly.
export NEMOCLAW_SOURCE="${NEMOCLAW_SOURCE:-${CLINICLAW_DIST:-/opt/cliniclaw/dist}/nemoclaw-source}"

cd "${REPO}"
if [ "$#" -gt 0 ]; then
    log "running: $*"
    exec "$@"
fi

log "running ${REPO}/setup.sh"
./setup.sh

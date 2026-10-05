#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# PID 1 for the CliniClaw control plane.
#
# Responsibilities, in order:
#   1. Publish the image's pinned toolchain into the shared state tree.
#   2. Refuse to run if that state was built by a different toolchain, because
#      an older openshell-gateway against a newer openshell.db fails with a sqlx
#      migration error and looks like a refused TCP connection.
#   3. Keep openshell-gateway listening, restarting it if it dies and rotating
#      its log before it can grow without bound.
#
# The gateway stays NemoClaw-managed: NemoClaw only recognises systemd as an
# external supervisor, so this script deliberately launches the gateway the same
# way NemoClaw does (same argv0 shape, same PID file, environment derived from
# the config NemoClaw wrote) rather than declaring itself the owner.

set -uo pipefail

log() { printf '[cliniclaw] %s\n' "$*"; }
warn() { printf '[cliniclaw] WARNING: %s\n' "$*" >&2; }
die() { printf '[cliniclaw] ERROR: %s\n' "$*" >&2; exit 1; }

CLINICLAW_ROOT="${CLINICLAW_ROOT:-}"
[ -n "${CLINICLAW_ROOT}" ] || die "CLINICLAW_ROOT is not set"
case "${CLINICLAW_ROOT}" in
    /*) ;;
    *) die "CLINICLAW_ROOT must be an absolute path, got '${CLINICLAW_ROOT}'" ;;
esac

DIST="${CLINICLAW_DIST:-/opt/cliniclaw/dist}"
export HOME="${CLINICLAW_ROOT}/home"
STATE_ROOT="${HOME}/.local/state"
GATEWAY_STATE="${STATE_ROOT}/nemoclaw/openshell-docker-gateway"
GATEWAY_CONFIG="${GATEWAY_STATE}/openshell-gateway.toml"
GATEWAY_LOG="${GATEWAY_STATE}/openshell-gateway.log"
GATEWAY_PID_FILE="${GATEWAY_STATE}/openshell-gateway.pid"
PIN_FILE="${STATE_ROOT}/cliniclaw-pin.json"
PAUSE_FILE="${STATE_ROOT}/cliniclaw-supervisor.pause"
GATEWAY_BIN="${HOME}/.local/bin/openshell-gateway"
MAX_LOG_MB="${CLINICLAW_MAX_LOG_MB:-256}"
POLL_SECONDS="${CLINICLAW_POLL_SECONDS:-5}"
GATEWAY_NAME="${CLINICLAW_GATEWAY_NAME:-nemoclaw}"

gateway_child=""

shutdown() {
    log "shutdown requested"
    if [ -n "${gateway_child}" ] && kill -0 "${gateway_child}" 2>/dev/null; then
        log "stopping openshell-gateway (pid ${gateway_child})"
        kill -TERM "${gateway_child}" 2>/dev/null || true
        for _ in $(seq 1 30); do
            kill -0 "${gateway_child}" 2>/dev/null || break
            sleep 0.5
        done
        kill -0 "${gateway_child}" 2>/dev/null && kill -KILL "${gateway_child}" 2>/dev/null || true
    fi
    exit 0
}
trap shutdown TERM INT

# ── Preflight ──────────────────────────────────────────────────────────────

prepare_state_tree() {
    for dir in \
        "${HOME}/.local/bin" \
        "${STATE_ROOT}" \
        "${HOME}/.config" \
        "${HOME}/.nemoclaw" \
        "${HOME}/.cache"
    do
        mkdir -p "${dir}" 2>/dev/null || die \
            "cannot create ${dir}. The bind mount at ${CLINICLAW_ROOT} must be writable by uid $(id -u); run scripts/cliniclaw.sh init on the host."
    done
    [ -w "${HOME}" ] || die \
        "${HOME} is not writable by uid $(id -u). Fix ownership on the host: chown -R $(id -u):$(id -g) ${CLINICLAW_ROOT}"
}

check_docker() {
    local socket="${DOCKER_SOCKET:-/var/run/docker.sock}"
    [ -S "${socket}" ] || die \
        "no Docker socket at ${socket}. The control plane drives the host daemon; mount it and set DOCKER_SOCKET."
    docker version --format '{{.Server.Version}}' >/dev/null 2>&1 || die \
        "cannot talk to the Docker daemon at ${socket}. Add the host docker group gid to the container (scripts/cliniclaw.sh init records it)."
    log "docker daemon: $(docker version --format '{{.Server.Version}}' 2>/dev/null)"
}

# The pinned OpenShell binaries have to exist on a path the *host* daemon can
# resolve: the gateway hands supervisor_bin straight to Docker when it starts a
# sandbox. That is why they are copied into the shared mount instead of being
# used from the image.
publish_toolchain() {
    local changed=0 binary target
    for binary in openshell openshell-gateway openshell-sandbox; do
        [ -x "${DIST}/bin/${binary}" ] || die "image is missing ${DIST}/bin/${binary}"
        target="${HOME}/.local/bin/${binary}"
        if ! cmp -s "${DIST}/bin/${binary}" "${target}"; then
            install -m 0755 "${DIST}/bin/${binary}" "${target}" \
                || die "could not publish ${binary} to ${target}"
            changed=1
        fi
    done

    # A shim rather than a copy: the CLI needs its 450MB node_modules tree, which
    # stays in the image.
    printf '#!/bin/sh\nexec /usr/bin/node "%s/bin/nemoclaw.js" "$@"\n' \
        "${DIST}/nemoclaw-source" > "${HOME}/.local/bin/nemoclaw"
    chmod 0755 "${HOME}/.local/bin/nemoclaw"

    if [ "${changed}" = "1" ]; then
        log "published pinned OpenShell binaries to ${HOME}/.local/bin"
    fi
}

# Version skew between the toolchain and the on-disk sqlite database is the
# failure this whole image exists to prevent, so it is checked explicitly rather
# than left to surface as a connection refused.
check_pin() {
    local image_pin live_pin
    image_pin="$(cat "${DIST}/pin.json" 2>/dev/null)" \
        || die "image is missing ${DIST}/pin.json"

    if [ ! -f "${PIN_FILE}" ]; then
        printf '%s' "${image_pin}" > "${PIN_FILE}"
        log "recorded toolchain pin: ${image_pin}"
        return
    fi

    live_pin="$(cat "${PIN_FILE}" 2>/dev/null)"
    if [ "${live_pin}" = "${image_pin}" ]; then
        log "toolchain pin matches state: ${image_pin}"
        return
    fi

    if [ ! -f "${GATEWAY_STATE}/openshell.db" ]; then
        printf '%s' "${image_pin}" > "${PIN_FILE}"
        log "toolchain pin updated (no database to migrate): ${image_pin}"
        return
    fi

    if [ "${CLINICLAW_ALLOW_STATE_RESET:-0}" = "1" ]; then
        local stamp archive
        stamp="$(date +%Y%m%d_%H%M%S)"
        archive="${GATEWAY_STATE}.superseded.${stamp}"
        warn "toolchain changed (${live_pin} -> ${image_pin}); archiving gateway state to ${archive}"
        mv "${GATEWAY_STATE}" "${archive}" || die "could not archive ${GATEWAY_STATE}"
        mkdir -p "${GATEWAY_STATE}"
        rm -rf "${HOME}/.config/openshell/gateways/${GATEWAY_NAME}"
        printf '%s' "${image_pin}" > "${PIN_FILE}"
        warn "gateway state reset; re-run: scripts/cliniclaw.sh onboard"
        return
    fi

    die "$(cat <<EOF
toolchain / state mismatch.

  state was created by: ${live_pin}
  this image provides:  ${image_pin}

openshell.db carries schema migrations from the toolchain that created it. A
different openshell-gateway will refuse to start against it ("migration N was
previously applied but is missing in the resolved migrations"), which surfaces
as a refused connection on 127.0.0.1:8080.

Either run the image that matches the state, or discard the state and onboard
again:

  scripts/cliniclaw.sh reset
EOF
)"
}

rotate_log() {
    [ -f "${GATEWAY_LOG}" ] || return 0
    local size_mb
    size_mb=$(( $(stat -c %s "${GATEWAY_LOG}" 2>/dev/null || echo 0) / 1048576 ))
    [ "${size_mb}" -ge "${MAX_LOG_MB}" ] || return 0
    local rotated="${GATEWAY_LOG}.$(date +%Y%m%d_%H%M%S)"
    log "gateway log is ${size_mb}MB; rotating to $(basename "${rotated}")"
    mv "${GATEWAY_LOG}" "${rotated}" 2>/dev/null || return 0
    # Keep one generation; an unbounded log is what wedged the host gateway.
    ls -1t "${GATEWAY_LOG}".* 2>/dev/null | tail -n +2 | xargs -r rm -f
}

# ── Gateway lifecycle ──────────────────────────────────────────────────────

gateway_listening() {
    local port="${1}"
    ss -H -tln 2>/dev/null | grep -qE "[:.]${port}[[:space:]]"
}

# NemoClaw records the gateway PID in runtime.json and probes it to decide
# whether a gateway is already up. Leaving a stale PID there invites a second
# gateway on the same port, so the live PID is written back after each start.
sync_runtime_pid() {
    local pid="${1}" runtime="${GATEWAY_STATE}/runtime.json"
    [ -f "${runtime}" ] || return 0
    python3 - "${runtime}" "${pid}" <<'PY' 2>/dev/null || true
import json, sys
path, pid = sys.argv[1], int(sys.argv[2])
try:
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
except (OSError, ValueError):
    raise SystemExit(0)
if not isinstance(data, dict):
    raise SystemExit(0)
data["pid"] = pid
with open(path, "w", encoding="utf-8") as handle:
    json.dump(data, handle, indent=2)
    handle.write("\n")
PY
}

start_gateway() {
    local env_script
    env_script="$(python3 /usr/local/lib/cliniclaw/gateway-env.py "${GATEWAY_CONFIG}")" || return 1

    # shellcheck disable=SC1090
    set -a
    eval "${env_script}"
    set +a

    [ -x "${GATEWAY_BIN}" ] || { warn "${GATEWAY_BIN} is not executable"; return 1; }

    rotate_log
    log "starting openshell-gateway on 127.0.0.1:${CLINICLAW_GATEWAY_PORT}"

    # The argv0 shape is NemoClaw's: its own process discovery greps for it, so
    # keeping the format lets `nemoclaw`/`openshell` recognise this process as
    # the gateway they manage.
    bash -c 'exec -a "openshell-gateway[nemoclaw='"${GATEWAY_NAME}"';port='"${CLINICLAW_GATEWAY_PORT}"']" "$0"' \
        "${GATEWAY_BIN}" >>"${GATEWAY_LOG}" 2>&1 &
    gateway_child=$!
    printf '%s\n' "${gateway_child}" > "${GATEWAY_PID_FILE}"
    sync_runtime_pid "${gateway_child}"

    local waited=0
    while [ "${waited}" -lt 60 ]; do
        if ! kill -0 "${gateway_child}" 2>/dev/null; then
            warn "openshell-gateway exited during startup; last log lines:"
            tail -n 20 "${GATEWAY_LOG}" >&2 || true
            gateway_child=""
            return 1
        fi
        if gateway_listening "${CLINICLAW_GATEWAY_PORT}"; then
            log "gateway is listening on 127.0.0.1:${CLINICLAW_GATEWAY_PORT}"
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done

    warn "gateway did not begin listening within 60s; leaving it running and continuing to watch"
    return 0
}

# ── Main ───────────────────────────────────────────────────────────────────

log "control plane starting (root=${CLINICLAW_ROOT}, home=${HOME})"
prepare_state_tree
check_docker
publish_toolchain
check_pin
mkdir -p "${GATEWAY_STATE}"
rm -f "${PAUSE_FILE}"

log "toolchain: $(cat "${DIST}/pin.json")"
log "ready. Onboard with: scripts/cliniclaw.sh onboard"

onboard_hint_counter=0
backoff=1

while true; do
    if [ -f "${PAUSE_FILE}" ]; then
        # cliniclaw-onboard holds this while NemoClaw manages the gateway
        # itself, so the two never race for port 8080.
        sleep "${POLL_SECONDS}"
        continue
    fi

    if [ ! -f "${GATEWAY_CONFIG}" ]; then
        if [ "$((onboard_hint_counter % 60))" = "0" ]; then
            log "waiting for onboarding (no ${GATEWAY_CONFIG} yet)"
        fi
        onboard_hint_counter=$((onboard_hint_counter + 1))
        sleep "${POLL_SECONDS}"
        continue
    fi

    port="$(python3 - "${GATEWAY_CONFIG}" <<'PY' 2>/dev/null || echo 8080
import sys, tomllib
from urllib.parse import urlsplit
with open(sys.argv[1], "rb") as handle:
    config = tomllib.load(handle)
endpoint = config["openshell"]["drivers"]["docker"]["grpc_endpoint"]
print(urlsplit(endpoint).port or 8080)
PY
)"

    if gateway_listening "${port}"; then
        # Either our child or a gateway NemoClaw started during onboarding.
        backoff=1
        sleep "${POLL_SECONDS}"
        continue
    fi

    if [ -n "${gateway_child}" ] && kill -0 "${gateway_child}" 2>/dev/null; then
        sleep "${POLL_SECONDS}"
        continue
    fi

    warn "gateway is not listening on ${port}; starting it"
    if start_gateway; then
        backoff=1
    else
        warn "gateway start failed; retrying in ${backoff}s"
        sleep "${backoff}"
        backoff=$(( backoff < 60 ? backoff * 2 : 60 ))
    fi
done

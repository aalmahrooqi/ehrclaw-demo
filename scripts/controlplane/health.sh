#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Container healthcheck. "Healthy" means the control plane is doing what it
# should right now, which before onboarding means idling without a gateway --
# otherwise a fresh install would sit permanently unhealthy.

set -uo pipefail

CLINICLAW_ROOT="${CLINICLAW_ROOT:-}"
[ -n "${CLINICLAW_ROOT}" ] || { echo "CLINICLAW_ROOT unset"; exit 1; }

HOME_DIR="${CLINICLAW_ROOT}/home"
GATEWAY_CONFIG="${HOME_DIR}/.local/state/nemoclaw/openshell-docker-gateway/openshell-gateway.toml"
PAUSE_FILE="${HOME_DIR}/.local/state/cliniclaw-supervisor.pause"

if [ -f "${PAUSE_FILE}" ]; then
    echo "onboarding in progress"
    exit 0
fi

if [ ! -f "${GATEWAY_CONFIG}" ]; then
    echo "awaiting onboarding"
    exit 0
fi

port="$(python3 - "${GATEWAY_CONFIG}" <<'PY' 2>/dev/null || echo 8080
import sys, tomllib
from urllib.parse import urlsplit
with open(sys.argv[1], "rb") as handle:
    config = tomllib.load(handle)
print(urlsplit(config["openshell"]["drivers"]["docker"]["grpc_endpoint"]).port or 8080)
PY
)"

if ss -H -tln 2>/dev/null | grep -qE "[:.]${port}[[:space:]]"; then
    echo "gateway listening on ${port}"
    exit 0
fi

echo "gateway not listening on ${port}"
exit 1

STATE="$HOME/.local/state/nemoclaw/openshell-docker-gateway"
BIN="$HOME/.local/bin/openshell-gateway"

# Optional but recommended: move the old log aside
mv "$STATE/openshell-gateway.log" "$STATE/openshell-gateway.log.$(date +%Y%m%d_%H%M%S)" 2>/dev/null || true

export OPENSHELL_DRIVERS=docker
export OPENSHELL_BIND_ADDRESS=127.0.0.1
export OPENSHELL_SERVER_PORT=8080
export OPENSHELL_LOCAL_TLS_DIR="$STATE/tls"
export OPENSHELL_DB_URL="sqlite:${STATE}/openshell.db"
export OPENSHELL_GRPC_ENDPOINT=https://127.0.0.1:8080
export OPENSHELL_SSH_GATEWAY_HOST=127.0.0.1
export OPENSHELL_SSH_GATEWAY_PORT=8080
export OPENSHELL_DOCKER_NETWORK_NAME=openshell-docker
export OPENSHELL_DOCKER_SUPERVISOR_IMAGE="ghcr.io/nvidia/openshell/supervisor@sha256:f4226253a3525c3832adac5b38b419a0f27d1e915effe565b5885e20f93cd5e9"
export OPENSHELL_DOCKER_SUPERVISOR_BIN="$HOME/.local/bin/openshell-sandbox"
export OPENSHELL_GATEWAY_CONFIG="$STATE/openshell-gateway.toml"

setsid bash -c 'exec -a "openshell-gateway[nemoclaw=nemoclaw;port=8080]" "$0" "$@"' "$BIN" \
  >>"$STATE/openshell-gateway.log" 2>&1 < /dev/null &
echo $! > "$STATE/openshell-gateway.pid"
openshell status
openshell sandbox list
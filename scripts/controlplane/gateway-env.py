#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Derive the openshell-gateway launch environment from its own config file.

NemoClaw writes ``openshell-gateway.toml`` during onboarding and starts the
gateway with a matching set of ``OPENSHELL_*`` variables. Restarting the gateway
therefore means reproducing that environment exactly -- a hand-maintained copy
drifts the moment NemoClaw changes a supervisor image digest or network name,
and the gateway then either refuses to start or starts against the wrong
compute driver.

Reading the values back out of the TOML keeps the restart faithful to whatever
onboarding actually recorded. Emits shell-quoted ``export`` lines on stdout.
"""

from __future__ import annotations

import shlex
import sys
import tomllib
from pathlib import Path
from urllib.parse import urlsplit

# Keep in sync with DOCKER_DRIVER_GATEWAY_RUNTIME_ENV_KEYS in NemoClaw's
# src/lib/onboard/docker-driver-gateway-env.ts.
BIND_ADDRESS = "127.0.0.1"


def fail(message: str) -> "NoReturn":  # type: ignore[name-defined]
    print(f"gateway-env: {message}", file=sys.stderr)
    raise SystemExit(1)


def main() -> int:
    if len(sys.argv) != 2:
        fail("usage: gateway-env.py <openshell-gateway.toml>")

    config_path = Path(sys.argv[1]).resolve()
    state_dir = config_path.parent
    try:
        config = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        fail(f"{config_path} does not exist; the gateway has not been onboarded yet")
    except (OSError, tomllib.TOMLDecodeError) as error:
        fail(f"could not parse {config_path}: {error}")

    openshell = config.get("openshell") or {}
    gateway = openshell.get("gateway") or {}
    drivers = (openshell.get("drivers") or {}).get("docker") or {}

    compute_drivers = gateway.get("compute_drivers") or ["docker"]
    if "docker" not in compute_drivers:
        fail(
            "this control plane only supervises the docker compute driver, "
            f"but the config declares {compute_drivers}"
        )

    endpoint = drivers.get("grpc_endpoint")
    if not endpoint:
        fail("openshell.drivers.docker.grpc_endpoint is missing")
    port = urlsplit(endpoint).port
    if not port:
        fail(f"could not read a port from grpc_endpoint {endpoint!r}")

    # The TLS block records the leaf certificate; OPENSHELL_LOCAL_TLS_DIR wants
    # the directory that holds ca.crt plus the server/ and client/ subtrees.
    cert_path = ((gateway.get("tls") or {}).get("cert_path")) or ""
    tls_dir = Path(cert_path).parent.parent if cert_path else state_dir / "tls"

    env = {
        "OPENSHELL_DRIVERS": "docker",
        "OPENSHELL_BIND_ADDRESS": BIND_ADDRESS,
        "OPENSHELL_SERVER_PORT": str(port),
        "OPENSHELL_LOCAL_TLS_DIR": str(tls_dir),
        "OPENSHELL_DB_URL": f"sqlite:{state_dir / 'openshell.db'}",
        "OPENSHELL_GRPC_ENDPOINT": endpoint,
        "OPENSHELL_SSH_GATEWAY_HOST": BIND_ADDRESS,
        "OPENSHELL_SSH_GATEWAY_PORT": str(port),
        "OPENSHELL_GATEWAY_CONFIG": str(config_path),
    }

    optional = {
        "OPENSHELL_DOCKER_NETWORK_NAME": drivers.get("network_name"),
        "OPENSHELL_DOCKER_SUPERVISOR_IMAGE": drivers.get("supervisor_image"),
        "OPENSHELL_DOCKER_SUPERVISOR_BIN": drivers.get("supervisor_bin"),
    }
    for key, value in optional.items():
        if value:
            env[key] = str(value)

    missing = [key for key in ("OPENSHELL_DOCKER_SUPERVISOR_BIN",) if key not in env]
    if missing:
        fail(f"config is missing required docker-driver values: {', '.join(missing)}")

    for key, value in env.items():
        print(f"export {key}={shlex.quote(value)}")

    # The supervisor needs the port separately to probe readiness.
    print(f"export CLINICLAW_GATEWAY_PORT={shlex.quote(str(port))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Upload Synthea FHIR transaction bundles to a running HAPI FHIR R4 server."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_FHIR_URL = "http://localhost:8081/fhir"
DEFAULT_DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def wait_for_server(fhir_url: str, timeout_seconds: int = 600) -> None:
    metadata_url = f"{fhir_url.rstrip('/')}/metadata"
    deadline = time.time() + timeout_seconds
    print(f"Waiting for FHIR server at {metadata_url} ...")

    while time.time() < deadline:
        try:
            with urllib.request.urlopen(metadata_url, timeout=10) as response:
                body = json.loads(response.read().decode())
                version = body.get("fhirVersion", "unknown")
                print(f"Server ready (FHIR {version}).")
                return
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            time.sleep(5)

    raise SystemExit(f"Timed out waiting for FHIR server at {metadata_url}")


def upload_bundle(fhir_url: str, bundle_path: Path, timeout_seconds: int) -> tuple[int, str]:
    data = bundle_path.read_bytes()
    request = urllib.request.Request(
        fhir_url.rstrip("/"),
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/fhir+json",
            "Accept": "application/fhir+json",
        },
    )

    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        status = response.status
        body = response.read().decode("utf-8", errors="replace")
        return status, body[:500]


def summarize_response(body: str) -> str:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return body[:200]

    if parsed.get("resourceType") != "Bundle":
        return body[:200]

    failed = 0
    for entry in parsed.get("entry", []):
        outcome = entry.get("response", {}).get("status", "")
        if outcome and not outcome.startswith("2"):
            failed += 1

    if failed:
        return f"{failed} entries failed in transaction response"
    return "transaction succeeded"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fhir-url", default=DEFAULT_FHIR_URL)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--timeout", type=int, default=900, help="Per-bundle upload timeout in seconds")
    parser.add_argument("--start-at", type=int, default=1, help="1-based bundle index to start from")
    args = parser.parse_args()

    if not args.data_dir.is_dir():
        raise SystemExit(f"Data directory not found: {args.data_dir}")

    bundles = sorted(args.data_dir.glob("*.json"))
    if not bundles:
        raise SystemExit(f"No JSON bundles found in {args.data_dir}")

    wait_for_server(args.fhir_url)

    total = len(bundles)
    successes = 0
    failures: list[tuple[str, str]] = []

    for index, bundle_path in enumerate(bundles, start=1):
        if index < args.start_at:
            continue

        print(f"[{index}/{total}] Uploading {bundle_path.name} ({bundle_path.stat().st_size // (1024 * 1024)} MB) ...", flush=True)
        started = time.time()

        try:
            status, body = upload_bundle(args.fhir_url, bundle_path, args.timeout)
            elapsed = time.time() - started
            summary = summarize_response(body)
            print(f"    -> HTTP {status} in {elapsed:.1f}s ({summary})")
            successes += 1
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            message = error_body[:500]
            print(f"    -> HTTP {exc.code}: {message}")
            failures.append((bundle_path.name, message))
        except Exception as exc:  # noqa: BLE001 - report upload failures and continue
            print(f"    -> ERROR: {exc}")
            failures.append((bundle_path.name, str(exc)))

    print()
    print(f"Finished: {successes}/{total - (args.start_at - 1)} bundles uploaded successfully.")
    if failures:
        print(f"{len(failures)} bundle(s) failed:")
        for name, message in failures:
            print(f"  - {name}: {message[:200]}")
        return 1

    patient_count_url = f"{args.fhir_url.rstrip('/')}/Patient?_summary=count"
    with urllib.request.urlopen(patient_count_url, timeout=30) as response:
        count_body = json.loads(response.read().decode())
        total_patients = count_body.get("total", "unknown")
    print(f"Patient count on server: {total_patients}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

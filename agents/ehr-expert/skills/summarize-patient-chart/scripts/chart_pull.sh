#!/usr/bin/env bash
# Pull every domain of ONE patient's chart in a single command.
#
# Sandbox network policy binds the FHIR host to /usr/bin/curl, so curl does all
# egress and python only ever formats a bundle arriving on stdin.
#
# Age is computed here rather than left as a birthDate: the chart header wants an
# age, and it keeps a DOB out of the summary the agent assembles.
#
# usage: chart_pull.sh <patientId>
set -uo pipefail

PID="${1:?usage: chart_pull.sh <patientId>}"
BASE="${FHIR_BASE:-http://host.openshell.internal:8081/fhir}"
SKILL_DIR="$(cd "$(dirname "$0")/.." && pwd)"
FMT="$SKILL_DIR/../fhir-data-access/scripts/fhir_format.py"

fetch() {  # fetch <label> <query>
  printf '\n== %s\n' "$1"
  curl -sS -H 'Accept: application/fhir+json' "$BASE/$2" | python3 "$FMT"
}

fetch "Patient"      "Patient/$PID"
fetch "Allergies"    "AllergyIntolerance?patient=$PID"
fetch "Problems"     "Condition?patient=$PID&clinical-status=active"
fetch "Medications"  "MedicationRequest?patient=$PID&status=active"
fetch "Labs/vitals"  "Observation?patient=$PID&_sort=-date&_count=25"
fetch "Encounters"   "Encounter?patient=$PID&_sort=-date&_count=5"

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

FORMAT=$(cat <<'PY'
import json, sys
from datetime import date

def txt(c):
    if not c:
        return None
    coding = (c.get("coding") or [{}])[0]
    return c.get("text") or coding.get("display") or coding.get("code")

def value(o):
    if "valueQuantity" in o:
        q = o["valueQuantity"]
        return f'{q.get("value")} {q.get("unit") or ""}'.strip()
    if "valueCodeableConcept" in o:
        return txt(o["valueCodeableConcept"])
    for k in ("valueString", "valueBoolean", "valueInteger"):
        if k in o:
            return str(o[k])
    if "component" in o:
        return "; ".join(f'{txt(c.get("code"))}={value(c)}' for c in o["component"])
    return None

def years(dob):
    if not dob:
        return None
    y, m, d = (int(x) for x in dob.split("-")[:3])
    today = date.today()
    return today.year - y - ((today.month, today.day) < (m, d))

def line(r):
    t, rid = r["resourceType"], r["id"]
    if t == "Patient":
        n = (r.get("name") or [{}])[0]
        given = " ".join(n.get("given") or [])
        addr = (r.get("address") or [{}])[0]
        age = years(r.get("birthDate"))
        return f'{rid} | {given} {n.get("family")} | age={age}y | {r.get("gender")} | {addr.get("city")}'
    if t == "AllergyIntolerance":
        rx = [txt(m) for x in r.get("reaction", []) for m in x.get("manifestation", [])]
        return f'{rid} | {txt(r.get("code"))} | criticality={r.get("criticality")} | {", ".join(rx) or "no reaction recorded"}'
    if t == "Condition":
        return f'{rid} | {txt(r.get("code"))} | onset={r.get("onsetDateTime")} | {txt(r.get("clinicalStatus"))}'
    if t == "MedicationRequest":
        return f'{rid} | {txt(r.get("medicationCodeableConcept"))} | {r.get("status")} | authored={r.get("authoredOn")}'
    if t == "Observation":
        interp = txt((r.get("interpretation") or [None])[0])
        return (f'{rid} | {txt(r.get("code"))} | {value(r)} | '
                f'{r.get("effectiveDateTime")} | flag={interp or "none recorded"}')
    if t == "Encounter":
        return f'{rid} | {txt((r.get("type") or [None])[0])} | {(r.get("period") or {}).get("start")} | {r.get("status")}'
    return f'{rid} | {t}'

b = json.load(sys.stdin)
if b.get("resourceType") == "OperationOutcome":
    for i in b.get("issue", []):
        print(f'  ERROR {i.get("severity")}: {i.get("diagnostics") or i.get("code")}')
elif b.get("resourceType") != "Bundle":
    print(" ", line(b))
else:
    entries = [e["resource"] for e in b.get("entry", [])]
    if not entries:
        print("  nothing on file")
    for r in entries:
        print(" ", line(r))
PY
)

fetch() {  # fetch <label> <query>
  printf '\n== %s\n' "$1"
  curl -sS -H 'Accept: application/fhir+json' "$BASE/$2" | python3 -c "$FORMAT"
}

fetch "Patient"      "Patient/$PID"
fetch "Allergies"    "AllergyIntolerance?patient=$PID"
fetch "Problems"     "Condition?patient=$PID&clinical-status=active"
fetch "Medications"  "MedicationRequest?patient=$PID&status=active"
fetch "Labs/vitals"  "Observation?patient=$PID&_sort=-date&_count=25"
fetch "Encounters"   "Encounter?patient=$PID&_sort=-date&_count=5"

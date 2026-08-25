#!/usr/bin/env python3
"""
Generic FHIR bundle formatter.

Reads a FHIR R4 Bundle (or single resource) from stdin, prints one human-readable
line per entry. Handles Patient, AllergyIntolerance, Condition, MedicationRequest,
Observation, and Encounter. Unknown resource types fall back to "id | ResourceType".

Usage:
    curl -s '...fhir/Patient?family=Doyle' | python3 fhir_format.py
    curl -s '...fhir/Patient/66453'        | python3 fhir_format.py
"""
import json
import sys
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


def fmt(r):
    t, rid = r["resourceType"], r["id"]
    if t == "Patient":
        n = (r.get("name") or [{}])[0]
        given = " ".join(n.get("given") or [])
        addr = (r.get("address") or [{}])[0]
        age = years(r.get("birthDate"))
        deceased = r.get("deceasedDateTime") or r.get("deceasedBoolean")
        parts = [rid, f"{given} {n.get('family')}", f"age={age}y", r.get("gender"), addr.get("city")]
        if deceased:
            parts.append(f"deceased={deceased}")
        return " | ".join(str(p) for p in parts if p is not None)
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
    return f"{rid} | {t}"


def main():
    b = json.load(sys.stdin)
    if b.get("resourceType") == "OperationOutcome":
        for i in b.get("issue", []):
            print(f'ERROR {i.get("severity")}: {i.get("diagnostics") or i.get("code")}')
        sys.exit(1)
    if b.get("resourceType") == "Bundle":
        total = b.get("total")
        if total is not None:
            print(f"total: {total}")
        entries = [e["resource"] for e in b.get("entry", [])]
        if not entries:
            print("nothing on file")
        for r in entries:
            print(fmt(r))
    else:
        # Single resource
        print(fmt(b))


if __name__ == "__main__":
    main()

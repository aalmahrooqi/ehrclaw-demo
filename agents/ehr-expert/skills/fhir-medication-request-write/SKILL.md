---
name: fhir-medication-request-write
description: Create, update, or cancel prescription orders (FHIR R4 MedicationRequest) on the local HAPI FHIR server via its REST API. Use when explicitly instructed to write, add, create, change, refill, stop, or cancel a medication order/prescription for a specific patient. This skill RECORDS an order that has already been specified; it does not decide, recommend, or choose what to prescribe.
---

# FHIR MedicationRequest Write

Write a prescription order to the project's FHIR server by creating or updating a `MedicationRequest` resource. This is the write counterpart to `fhir-data-access`.

## Server

- **Base URL**: `http://host.openshell.internal:8081/fhir`
- **Version**: FHIR R4 (4.0.1), HAPI FHIR
- **Writes**: permitted — policy grants `read-write` to this host over `curl`. Use `POST` to create, `PUT` to update/replace by id.

## Safety boundary (read first)

- This skill **records** a medication order that has been explicitly provided (drug, dose, route, frequency, patient, prescriber). It does **not** choose the drug or dose, diagnose, or recommend therapy.
- **Never invent** a medication, dose, route, patient, or prescriber. If any required field is missing or ambiguous, **STOP and ask** — do not guess.
- **Confirm before writing**: show the fully-constructed JSON to the user and get an explicit go-ahead before sending the `POST`/`PUT`.
- **Verify the patient first** with `fhir-data-access` (`GET Patient/<id>`). Never write against an unknown or guessed patient id.
- Prefer coding the drug with an **RxNorm RxCUI** (via `rxnorm-resolve-rxcui`); when clinically relevant, review the label/interactions (via `drug-label-interactions`) before writing.
- **Read the resource back** after writing to confirm it was stored.
- End any clinical output with: *AI generated content, always consult with a physician or pharmacist.*

## Prerequisite chain

1. Resolve the medication text to an RxCUI → skill `rxnorm-resolve-rxcui`.
2. When relevant, check interactions/warnings → skill `drug-label-interactions`.
3. Confirm the patient id exists → skill `fhir-data-access` (`GET Patient/<id>`).
4. Build the `MedicationRequest`, show it, confirm, then create.

## Required and recommended fields (R4)

**Required**

| Field | Notes |
|---|---|
| `resourceType` | Must be `"MedicationRequest"` |
| `status` | Usually `"active"` (also `draft`, `on-hold`, `stopped`, `cancelled`, `completed`) |
| `intent` | Usually `"order"` (also `proposal`, `plan`) |
| `medicationCodeableConcept` **or** `medicationReference` | Prefer RxNorm coding (`system: http://www.nlm.nih.gov/research/umls/rxnorm`) |
| `subject` | `{ "reference": "Patient/<id>" }` |

**Recommended**

`authoredOn`, `requester` (`Practitioner/<id>`), `dosageInstruction` (dose, timing), `dispenseRequest` (quantity, refills), `reasonCode`/`reasonReference`.

**Route:** express the route as plain language inside `dosageInstruction.text` (e.g. "1 tablet **by mouth** every 6 hours"). Do **not** add a coded `dosageInstruction.route` — SNOMED CT has no free lookup API and no terminology endpoint is allowlisted by policy, so a hand-typed code would be unverifiable. Free text is a valid, sufficient MedicationRequest.

## Create (POST)

Send the resource with no id; the server assigns one and returns `201 Created` with the new id/version in the `Location` header.

```bash
curl -s -i -X POST 'http://host.openshell.internal:8081/fhir/MedicationRequest' \
  -H 'Content-Type: application/fhir+json' \
  -H 'Accept: application/fhir+json' \
  -d '{
  "resourceType": "MedicationRequest",
  "status": "active",
  "intent": "order",
  "medicationCodeableConcept": {
    "coding": [{
      "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
      "code": "313782",
      "display": "Acetaminophen 325 MG Oral Tablet"
    }],
    "text": "Acetaminophen 325 MG Oral Tablet"
  },
  "subject": { "reference": "Patient/<patientId>" },
  "authoredOn": "2026-07-07",
  "dosageInstruction": [{
    "text": "1 tablet by mouth every 6 hours as needed for pain",
    "timing": { "repeat": { "frequency": 1, "period": 6, "periodUnit": "h" } },
    "doseAndRate": [{
      "doseQuantity": { "value": 325, "unit": "mg", "system": "http://unitsofmeasure.org", "code": "mg" }
    }]
  }],
  "dispenseRequest": {
    "quantity": { "value": 30, "unit": "tablet" },
    "numberOfRepeatsAllowed": 0
  }
}'
```

The `code` value must come from `rxnorm-resolve-rxcui`, not be hand-typed. `<patientId>` and `<practitionerId>` must be confirmed real ids.

### Building the JSON safely (recommended)

To avoid shell-quoting mistakes, build the resource in Python, confirm it, then POST a file:

```bash
python3 - <<'PY' > /tmp/mr.json
import json
mr = {
    "resourceType": "MedicationRequest",
    "status": "active",
    "intent": "order",
    "medicationCodeableConcept": {
        "coding": [{
            "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
            "code": "313782",                       # from rxnorm-resolve-rxcui
            "display": "Acetaminophen 325 MG Oral Tablet",
        }],
        "text": "Acetaminophen 325 MG Oral Tablet",
    },
    "subject": {"reference": "Patient/<patientId>"},  # confirmed via fhir-data-access
    "authoredOn": "2026-07-07",
    "dosageInstruction": [{"text": "1 tablet by mouth every 6 hours as needed for pain"}],
}
print(json.dumps(mr, indent=2))
PY

# Show /tmp/mr.json to the user, get confirmation, THEN:
curl -s -i -X POST 'http://host.openshell.internal:8081/fhir/MedicationRequest' \
  -H 'Content-Type: application/fhir+json' \
  -H 'Accept: application/fhir+json' \
  --data-binary @/tmp/mr.json
```

## Verify (read back)

Grab the new id from the `Location` header (or the returned body's `id`), then confirm:

```bash
curl -s -H 'Accept: application/fhir+json' \
  'http://host.openshell.internal:8081/fhir/MedicationRequest/<newId>'
```

## Update, stop, or cancel (PUT)

FHIR update replaces the whole resource at a known id. Fetch the current version, change only what's needed, and `PUT` it back. To cancel/stop an order, set `status` to `"cancelled"` or `"stopped"`.

```bash
# 1) fetch current
curl -s -H 'Accept: application/fhir+json' \
  'http://host.openshell.internal:8081/fhir/MedicationRequest/<id>' -o /tmp/mr.json

# 2) change status (example: stop the order)
python3 - <<'PY'
import json
p = "/tmp/mr.json"
mr = json.load(open(p))
mr["status"] = "stopped"
json.dump(mr, open(p, "w"), indent=2)
PY

# 3) show the diff to the user, confirm, then PUT it back
curl -s -i -X PUT 'http://host.openshell.internal:8081/fhir/MedicationRequest/<id>' \
  -H 'Content-Type: application/fhir+json' \
  -H 'Accept: application/fhir+json' \
  --data-binary @/tmp/mr.json
```

## Response and error handling

- `201 Created` → new resource; read the id from the `Location` header (`.../MedicationRequest/<id>/_history/<version>`).
- `200 OK` → update succeeded.
- Body may be an `OperationOutcome` on failure — always inspect `issue[].severity` and `issue[].diagnostics`.
- `400` / `422` → validation error (bad/missing field, unresolved reference). Fix the resource; do not retry blindly.
- `404` on `PUT` → that id doesn't exist; use `POST` to create instead.
- Reference fields must point to resources that exist (`Patient/<id>`, `Practitioner/<id>`), or the server rejects the write.

## Output format

```text
MedicationRequest write: Acetaminophen 325 MG Oral Tablet

Order (as recorded):
- Patient: Patient/<id> (confirmed)
- Medication: Acetaminophen 325 MG Oral Tablet (RxCUI 313782)
- Dose/route/frequency: 325 mg, oral, every 6h PRN pain
- Status/intent: active / order

Result:
- Created MedicationRequest/<newId> (version <v>)
- Read-back confirmed: yes

AI generated content, always consult with a physician or pharmacist.
```

---
name: fhir-data-access
description: Query the local HAPI FHIR R4 server for patient and clinical data using its REST API. Use when reading, searching, or counting FHIR resources (Patient, Observation, MedicationRequest, Encounter, Condition, etc.), or when the user asks about patient records, medications, labs, or other data held in the FHIR server.
---

# FHIR Data Access

Query the project's FHIR server over its REST API.

## Server

- **Base URL**: `http://host.openshell.internal:8081/fhir`
- **Version**: FHIR R4 (4.0.1), HAPI FHIR
- **Data**:  Patients Records


## Core request pattern

All reads are HTTP GET. Always request JSON.

```bash
curl -s -H 'Accept: application/fhir+json' \
  'http://host.openshell.internal:8081/fhir/<ResourceType>?<params>'
```

Responses are a `Bundle` of type `searchset`; results are under `entry[].resource`.

## Common queries

```bash
# Read one resource by id
curl -s 'http://host.openshell.internal:8081/fhir/Patient/<id>'

# Count without fetching bodies
curl -s 'http://host.openshell.internal:8081/fhir/Patient?_summary=count'

# Search patients by name
curl -s 'http://host.openshell.internal:8081/fhir/Patient?name=Ashely'

# All resources for a patient (use the patient compartment)
curl -s 'http://host.openshell.internal:8081/fhir/MedicationRequest?patient=<patientId>'
curl -s 'http://host.openshell.internal:8081/fhir/Observation?patient=<patientId>'
curl -s 'http://host.openshell.internal:8081/fhir/Condition?patient=<patientId>'
curl -s 'http://host.openshell.internal:8081/fhir/Encounter?patient=<patientId>'
```

## Useful search parameters

| Parameter | Purpose | Example |
|---|---|---|
| `patient` / `subject` | Filter by patient reference | `patient=<id>` |
| `_count` | Page size | `_count=50` |
| `_summary=count` | Return only the total | `_summary=count` |
| `_sort` | Sort results (prefix `-` for desc) | `_sort=-date` |
| `_include` | Include referenced resources | `_include=MedicationRequest:patient` |
| `category` / `code` | Filter clinical resources | `category=laboratory` |
| `status` | Filter by status | `status=completed` |

## Resource type guide

| Need | Resource type |
|---|---|
| Demographics | `Patient` |
| Medications (prescribed) | `MedicationRequest` |
| Medications (administered) | `MedicationAdministration` |
| Labs, vitals, measurements | `Observation` |
| Diagnoses / problems | `Condition` |
| Visits | `Encounter` |
| Procedures | `Procedure` |
| Allergies | `AllergyIntolerance` |
| Immunizations | `Immunization` |
| Care providers | `Practitioner`, `Organization` |


## Extracting fields

Pipe responses through `python3` rather than parsing by eye:

```bash
curl -s 'http://host.openshell.internal:8081/fhir/Patient?_count=' | python3 -c "
import sys, json
b = json.load(sys.stdin)
for e in b.get('entry', []):
    p = e['resource']
    name = p.get('name', [{}])[0]
    print(p['id'], name.get('family'), name.get('given'))
"
```

## Tips

- A search with no matches still returns HTTP 200 with `total: 0` and no `entry`.
- Reference fields look like `Patient/<id>` — strip the prefix to get the raw id.
- For large result sets, page with `_count` and follow the `next` link in `Bundle.link`.
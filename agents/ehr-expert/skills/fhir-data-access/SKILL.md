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

Fetch with `curl`, always. Sandbox network policy grants FHIR egress to
`/usr/bin/curl` and nothing else, so a python script that opens its own
connection (`urllib`, `requests`) is denied at the proxy. Python's job is to read
a bundle on stdin, never to fetch one.

## Common queries

```bash
# Read one resource by id
curl -s 'http://host.openshell.internal:8081/fhir/Patient/<id>'

# Search patient by birthdate
curl -s 'http://host.openshell.internal:8081/fhir/Patient?birthdate=1992-10-14'

# Search by patient name and birthdate
curl -s "http://host.openshell.internal:8081/fhir/Patient?birthdate=1992-10-14&given=Pete"

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

## Resolving a patient

> **Never query on a value you can't read.** DOBs may be masked as `**********`
> both in telemetry views AND in what you receive as input (the platform redacts
> them before they reach you). If the DOB in your context is `**********`, drop it
> entirely and search by name alone. `birthdate=**********` returns `total: 0` and
> that zero tells you nothing about whether the patient is on file.

Given a full identifier, `birthdate=` and `given=`/`family=` together are the
cheapest unique match — one curl, one result. Given only a name, resolve it
before reading anything else — one name is rarely one patient.

```bash
FMT=/sandbox/.hermes/skills/fhir-data-access/scripts/fhir_format.py

# If DOB is readable: name + DOB in one call (cheapest unique match)
curl -s -H 'Accept: application/fhir+json' \
  'http://host.openshell.internal:8081/fhir/Patient?family=Doyle&birthdate=1985-04-12&_count=50' | python3 "$FMT"

# If DOB is masked (**********) or absent: name only, then pick from candidates
curl -s -H 'Accept: application/fhir+json' \
  'http://host.openshell.internal:8081/fhir/Patient?given=Paul&_count=50' | python3 "$FMT"
```

- Derive age in the shell, as above. The chart header wants an age, and the shell
  does date math more reliably than you do.
- Two or more matches: list the candidates with their ids and ask which one.
  Don't take the first.
- If you matched on name alone, say so — don't imply you confirmed a DOB you were
  never given.

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
| `birthdate` | Filter patients by date of birth (use `ge`/`le` prefixes for a range) | `birthdate=ge1960-01-01&birthdate=le2007-01-01` |
| `gender` | Filter patients by administrative sex | `gender=male` |
| `clinical-status` | Filter conditions by clinical state | `clinical-status=active` |

## Demographic and coded filtering

FHIR has no `age` search parameter — translate an age range into a `birthdate`
window (older bound = earlier date). For example, "18 to 65 as of today" becomes
`birthdate=ge<today-65y>&birthdate=le<today-18y>`.

```bash
# Patients 18–65, female (coarse cohort filter)
curl -s 'http://host.openshell.internal:8081/fhir/Patient?gender=female&birthdate=ge1960-01-01&birthdate=le2007-01-01&_count=100'

# Patients with a coded condition (token = system|code, not free text)
curl -s 'http://host.openshell.internal:8081/fhir/Condition?code=http://snomed.info/sct|44054006'

# Combine with _summary=count to size a cohort before fetching bodies
curl -s 'http://host.openshell.internal:8081/fhir/Condition?code=http://snomed.info/sct|44054006&_summary=count'
```

`Condition?code=` matches a coded diagnosis, so you need the code (e.g. a SNOMED
CT code), not a free-text name. Resolve the term to a code first, or search
broadly and filter the returned `code.coding` client-side.

## Resource type guide

| Need | Resource type |
|---|---|
| Demographics | `Patient` |
| Medications (prescribed) | `MedicationRequest` |
| Medications (administered) | `MedicationAdministration` |
| Medications (patient-reported / statement) | `MedicationStatement` |
| Care plans | `CarePlan` |
| Labs, vitals, measurements | `Observation` |
| Diagnoses / problems | `Condition` |
| Visits | `Encounter` |
| Procedures | `Procedure` |
| Allergies | `AllergyIntolerance` |
| Immunizations | `Immunization` |
| Care providers | `Practitioner`, `Organization` |


## Extracting fields

A shared formatter is available for any FHIR bundle or single resource. Use it
instead of writing your own parser:

```bash
FMT=/sandbox/.hermes/skills/fhir-data-access/scripts/fhir_format.py

curl -s 'http://host.openshell.internal:8081/fhir/Patient?family=Doyle' | python3 "$FMT"
curl -s 'http://host.openshell.internal:8081/fhir/Patient/66453'        | python3 "$FMT"
curl -s 'http://host.openshell.internal:8081/fhir/Observation?patient=66453&_sort=-date&_count=10' | python3 "$FMT"
```

The formatter handles Patient, AllergyIntolerance, Condition, MedicationRequest,
Observation, and Encounter — printing one line per entry with the resource id,
key fields, and computed age for patients. Unknown types fall back to `id | ResourceType`.

Only drop to an inline `python3 -c` one-liner if you need a field the formatter
doesn't expose. Never write a temp script file.

## Tips

- A search with no matches still returns HTTP 200 with `total: 0` and no `entry`.
- A search that carries `_sort` or `_count` often omits `total` altogether. Don't
  report a count from a missing field — re-run the same filter with
  `_summary=count` when the number itself is the answer.
- A bad id or malformed parameter returns an `OperationOutcome`, not a `Bundle`.
  Read `issue[].diagnostics` and report the error rather than treating it as an
  empty result.
- Reference fields look like `Patient/<id>` — strip the prefix to get the raw id.
- For large result sets, page with `_count` and follow the `next` link in `Bundle.link`.
---
name: summarize-patient-chart
description: Produce a holistic, multi-domain overview of ONE patient from the connected FHIR dataset — active problems, current medications, recent labs and vitals, allergies, and recent encounters — with critical and abnormal findings surfaced first. Use when the user asks to summarize, review, or give an overview/snapshot of a single patient's chart or record. For targeted reads, searches, or counts of specific resources, use the fhir-data-access skill instead.
---

# Summarize Patient Chart

## When to Use

Use this skill when the user wants a cross-domain snapshot of a single patient —
"summarize the chart for patient X", "what's on file for this patient", "give me
an overview".

Not this skill:
- A single targeted lookup ("what's her last A1c", "is she on metformin") → use
  `fhir-data-access` directly.
- Screening many patients against trial criteria → use
  `clinical-trial-cohort-prescreener`.

## Reads are delegated

This skill does not define its own queries. Use the **`fhir-data-access`** skill
for every read — base URL, search params, and field extraction all live there.
This skill owns *which* resources to pull, *how* to order them, and the *output
layout*.

Get a patient id first. If given a name, resolve it with `fhir-data-access`
(`Patient?name=`); if more than one patient matches, stop and list the
candidates instead of guessing.

## What to pull (the recipe)

Fetch these for the patient, roughly in this priority. Pull only what's needed
to fill the layout below — don't enumerate every resource.

| Domain | Resource | Notes |
|---|---|---|
| Allergies | `AllergyIntolerance` | Criticality and reaction first. |
| Active problems | `Condition` | Prefer `clinical-status=active`. |
| Current meds | `MedicationRequest` | Prefer `status=active`. |
| Recent labs & vitals | `Observation` | `_sort=-date`; capture value, unit, date, and any interpretation flag (H/L/HH/LL/A). |
| Recent visits | `Encounter` | `_sort=-date`, most recent handful. |
| Immunizations | `Immunization` | Only if relevant or asked. |

Compute age from `Patient.birthDate` as of today.

## Ordering and flags

Lead with what matters (per the agent's rules):

1. **Critical/abnormal first** — high-criticality allergies, boxed-warning or
   otherwise high-risk meds, and out-of-range labs (interpretation flag) go at
   the top, not buried in a list.
2. Then the routine active problems and medication list.
3. Attach the clinical **date** to every lab, vital, and med. Call out when the
   most recent value in a domain is old (e.g. `last A1c: 14 mo ago`) — never
   present a stale value as if it were current.

## Report, don't diagnose

Surface, prioritize, and flag what the record contains. Do not infer new
diagnoses, reason through the patient's clinical course, or recommend
starting, stopping, or combining anything. Empty results are gaps, not
clearance — say "nothing on file", never "no problems".

## Output Format

Lead with a one-line identity plus headline flags, then sections. Omit a section
that has no data, but list that domain under **Data gaps** so the absence is
explicit.

```text
**{Given} {Family}, {age}yo {sex} · Patient/{id}** — {top flag(s), or "no critical flags on file"}

### ⚠ Flags
- {high-criticality allergy / boxed-warning med / abnormal lab — each with date + id}

### Active problems
| Condition | Onset | Status | id |
|---|---|---|---|

### Medications
| Medication | Dose | Status | id |
|---|---|---|---|

### Recent labs & vitals
| Test | Value | Date | Flag | id |
|---|---|---|---|---|

### Allergies
| Substance | Reaction | Criticality | id |
|---|---|---|---|

### Recent encounters
| Date | Type | Reason | id |
|---|---|---|---|

### Data gaps
- {domains with nothing on file, plus the date of the last value where relevant}
```

If the user asked for a narrower snapshot (e.g. "meds and allergies only"),
include just those sections — don't pad.

## Limitations

- This is a summary of structured FHIR data, not the full chart.
- Missing data means "not recorded" — not "absent" and not "normal".
- Not medical advice; the clinician confirms against the source record.

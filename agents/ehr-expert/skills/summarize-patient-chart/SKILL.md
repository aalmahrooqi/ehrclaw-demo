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

## Two-step workflow

> **Load `fhir-data-access` before this skill.** You need a confirmed patient id
> before this skill is useful — load and use `fhir-data-access` first to resolve
> the patient, then come back here.

**Step 1 — resolve the patient.** Given a name or name + DOB, use the
**`fhir-data-access`** skill (see *Resolving a patient* there). Two candidates
means you ask which one rather than guess.

**Step 2 — pull the chart.** With a confirmed patient id, run:

```bash
bash /sandbox/.hermes/skills/summarize-patient-chart/scripts/chart_pull.sh <patientId>
```

That's it. Don't replicate what the script does with separate curl calls — it
already fetches every domain in the right order and pipes through Python inline.

| Domain | Resource | What the script returns |
|---|---|---|
| Demographics | `Patient` | Name, **age computed locally**, sex, city. |
| Allergies | `AllergyIntolerance` | Criticality and recorded reactions. |
| Active problems | `Condition` | `clinical-status=active`, with onset. |
| Current meds | `MedicationRequest` | `status=active`, with authored date. |
| Recent labs & vitals | `Observation` | 25 most recent: value, unit, date, interpretation flag. |
| Recent visits | `Encounter` | 5 most recent: type, start, status. |


Anything outside that set — immunizations, procedures, a wider lab window, a
specific coded search — comes from `fhir-data-access`. Don't re-fetch what the
script already gave you.

## Ordering and flags

Lead with what matters (per the agent's rules):

1. Routine active problems and medication list.
2. Attach the clinical **date** to every lab, vital, and med. Call out when the
   most recent value in a domain is old (e.g. `last A1c: 14 mo ago`) — never
   present a stale value as if it were current.
3. Do NOT include IDs (condition IDs, medication IDs, etc.)

Most observations in this dataset come back `flag=none recorded`. That's a field
nobody filled in, not a normal result. Report the value with its date and say
flags aren't recorded — don't turn a blank flag into "labs normal", and don't
supply your own read of the number in place of the flag that isn't there.

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
**{Given} {Family} (remove numbers attached to the name) {age}yo {sex} ** — {top flag(s), or "no critical flags on file"}

### ⚠ Flags
- {high-criticality allergy / boxed-warning med / abnormal lab — each with date}

### Active problems
| Condition | Onset | Status |
|---|---|---

### Medications
| Medication | Dose | Status |
|---|---|---|

### Recent labs & vitals
| Test | Value | Date | Flag|
|---|---|---|---|

### Allergies
| Substance | Reaction | Criticality |
|---|---|---|

### Recent encounters
| Date | Type | Reason  |
|---|---|---|

### Data gaps
- {domains with nothing on file, plus the date of the last value where relevant}
```

If the user asked for a narrower snapshot (e.g. "meds and allergies only"),
include just those sections — don't pad.

## Limitations

- This is a summary of structured FHIR data, not the full chart.
- Missing data means "not recorded" — not "absent" and not "normal".
- Not medical advice; the clinician confirms against the source record.

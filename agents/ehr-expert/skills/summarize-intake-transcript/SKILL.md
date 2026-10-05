---
name: summarize-intake-transcript
description: Turn a nurse/patient intake transcript into a compact, source-labeled pre-visit summary. Use when the user provides a call, interview, portal, or intake transcript and asks for a handoff, intake summary, or preparation for clinician review. This skill summarizes reported information; it does not diagnose, recommend treatment, or autonomously triage.
---

# Summarize Intake Transcript

Create a bite-sized clinician handoff from one intake transcript. Preserve the
difference between what the patient or caregiver reported and what is present in
the FHIR chart.

## Safety boundary

- Do not diagnose, assess severity, recommend treatment or referral, or decide
  clinical urgency.
- Do not calculate clinical thresholds or introduce claims not explicitly
  present in the transcript or record.
- Do not convert a reported symptom into a charted condition.
- Do not state that a medication is being taken merely because it is active in
  the chart.
- Preserve explicit negatives only when the speaker actually stated them.
- If the transcript describes a possible emergency, reproduce the relevant
  wording and say clinician review is required; do not invent instructions.
- A workflow-supplied queue position is a draft operational review order
  requiring clinician confirmation, not autonomous clinical triage.

## Workflow

1. Identify the reason for contact and the patient's stated goal.
2. Extract only the reported facts that affect this handoff.
3. If reconciliation is requested, use **fhir-data-access** or
   **summarize-patient-chart**, but include only chart facts directly related to
   the intake.
4. Put discrepancies, ambiguity, and missing information under **Needs review**.
5. Keep transcript claims and chart facts visibly source-labeled.

## Compact output contract

The handoff must be **120–160 words**, excluding headings and source badges.
Return only the four sections below:

    ## Intake summary
    [One or two sentences; maximum 35 words.]

    ## Reported at intake
    - [Direct patient/caregiver statement.] [intake]
    - [Maximum three bullets, each no more than 16 words.] [intake]

    ## Chart matched
    - [Only directly relevant chart evidence.] [FHIR: ResourceType/id]
    - [Maximum three bullets, each no more than 16 words.] [FHIR: ResourceType/id]

    ## Needs review
    - [Concrete question, discrepancy, or missing fact.] [review]
    - [Maximum three bullets, each no more than 16 words.] [review]

Do not add tables, a preamble, staff-action history, unrelated diagnoses,
unrelated laboratory results, a comprehensive medication list, or a queue
placement section. Omit empty bullets rather than adding filler.

If no chart lookup was requested or performed, use one **Chart matched** bullet:

    - Chart not checked in this summary. [review]

When a workflow explicitly requests a machine-readable queue marker, place it
after the handoff on its own line. Do not discuss the placement inside the four
summary sections.

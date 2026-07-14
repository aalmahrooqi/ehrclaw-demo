---
name: clinical-trial-cohort-prescreener
description: Prescreen patients in the connected FHIR dataset against one clinical trial's inclusion and exclusion criteria. Use when the user supplies clinical trial text or eligibility criteria and asks which patients are potential matches, or requests clinical-trial eligibility prescreening or cohort screening. Produces potential-match results with evidence, never a definitive eligibility decision.
---

# Clinical Trial Cohort Prescreener

## When to Use
This is a clinical-trial pre-screening workflow.

Use this skill when the user provides the text of one clinical trial and asks which patients in the connected FHIR dataset may match it.

Never describe a patient as definitively eligible.
Use the terms:

- potential match
- possible match requiring more data
- not a match based on available evidence

Final eligibility must be confirmed by the trial research team.

## Expected Input
Required:
- The trial's inclusion and exclusion criteria.

Optional:
- The clinical trial text.
- A cohort description.
- A maximum number of patients to screen.
- An as-of date.

When no as-of date is supplied, use all data currently available through the connected FHIR server.


When no cohort limit is supplied:
1. Derive a coarse candidate cohort using the trial's target condition, age range, and administrative sex where possible.
2. Screen at most 100 candidate patients.
3. Clearly state that the results cover a limited candidate cohort, not the entire FHIR population.

## Additional skills required

This skill does not query the FHIR server itself. Use the **`fhir-data-access`**
skill for all reads, which covers:

1. Searching or enumerating patients (including coarse filtering by
   `birthdate`, `gender`, and condition `code`).
2. Searching FHIR resources by patient.
3. Reading individual FHIR resources when additional evidence is needed.

Relevant resource types may include:
- Patient
- Condition
- Observation
- MedicationRequest
- MedicationStatement
- Procedure
- Encounter
- DiagnosticReport
- CarePlan

## Criteria

Attempt direct evaluation of criteria involving:

- Age.
- Administrative sex.
- Presence or absence of coded conditions.
- Numeric laboratory values.
- Laboratory date windows.
- Presence of medications.
- Presence of procedures.
- Encounter history.
- Simple date relationships.
- Explicit minimum or maximum thresholds.

Only compare numeric values when the patient result and trial criterion
use the same unit, or when an available tool performs a reliable unit
conversion.

Mark the criterion as UNKNOWN when units cannot be compared safely.

## Procedure

### 1. Parse the trial

Extract:

- Trial title or identifier, when present.
- Target disease or condition.
- Minimum and maximum age.
- Administrative sex restrictions.
- Inclusion criteria.
- Exclusion criteria.

Split the criteria into atomic statements.

Assign each criterion a stable identifier:

- I1, I2, I3 for inclusion criteria.
- E1, E2, E3 for exclusion criteria.

Preserve the original criterion wording.

### 2. Classify each criterion

For every criterion, assign:

- Type: inclusion or exclusion.
- Domain: demographic, condition, observation, medication,
  procedure, encounter, temporal, or clinical judgment.
- Evaluation method:
  - DIRECT
  - UNKNOWN_WITH_CURRENT_DATA
  - REQUIRES_REVIEW

### 3. Build a coarse candidate cohort

Before retrieving full patient charts, apply the cheapest reliable
filters available.

Prefer:

1. Target condition.
2. Age range.
3. Administrative sex.
4. Other simple coded conditions.

Do not exclude a patient through approximate semantic matching alone.

If a FHIR code or display is ambiguous, retain the patient and mark
the relevant criterion for review.

Report:

- Number of patients considered.
- Number remaining after coarse filtering.
- Any limit applied to the candidate cohort.

### 4. Evaluate each candidate

Retrieve only the resources relevant to the criteria.

For every criterion, produce one of these results:

#### PASS

Direct patient evidence supports eligibility for this criterion.

Examples:

- The inclusion criterion requires age 18 or older and the patient is 54.
- The required diagnosis is present.
- A laboratory result is within the specified range and time window.

#### FAIL

Direct patient evidence rules the patient out.

Examples:

- The patient is younger than the minimum age.
- A required inclusion diagnosis is not present when the available data directly establishes a different diagnosis.
- The patient has a documented exclusion condition.
- A laboratory value falls outside an explicit permitted range.

#### NO_EVIDENCE_OF_EXCLUSION

Use only for exclusion criteria when no matching evidence was found.

This does not prove that the exclusion is absent.

Always use wording such as:

"No evidence of this exclusion was found in the available FHIR record."

Never rewrite this as:

"The patient does not have this condition."

#### UNKNOWN

Use when:

- Required information is missing.
- Record coverage is insufficient.
- The relevant resource was not found.
- Units are incompatible.
- The criterion cannot be resolved from available structured data.
- The medication or condition timeline is incomplete.

#### REVIEW

Use when evidence exists but interpreting it requires clinical or
protocol judgment.

### 5. Attach evidence
Every PASS or FAIL must reference supporting FHIR evidence.

Include, when available:

- Resource type.
- Resource ID.
- Clinical date.
- Code and display.
- Numeric value and unit.
- Medication or procedure name.
- A short explanation of why the evidence is relevant.

Do not cite an entire chart when one or two resources support the result.

UNKNOWN and NO_EVIDENCE_OF_EXCLUSION results should describe what was
searched and what information was unavailable.

### 6. Determine the overall prescreen result

Assign one overall result.

#### POTENTIAL_MATCH

Use when:

- No criterion is a definite FAIL.
- All essential inclusion criteria are PASS.
- Any unresolved exclusion criteria are explicitly reported as
  NO_EVIDENCE_OF_EXCLUSION, UNKNOWN, or REVIEW.

#### POSSIBLE_MATCH_NEEDS_DATA

Use when:

- No criterion is a definite FAIL.
- One or more essential inclusion criteria are UNKNOWN or REVIEW.

Explain exactly what additional information would resolve the case.

#### NOT_A_MATCH

Use when:

- At least one inclusion criterion is a definite FAIL.
- Or at least one exclusion criterion is directly present and therefore
  produces a FAIL.

List the blocking criteria.

Never use the overall result ELIGIBLE.

### 7. Rank the results

Rank patients in this order:

1. POTENTIAL_MATCH.
2. POSSIBLE_MATCH_NEEDS_DATA.
3. NOT_A_MATCH.

Within POTENTIAL_MATCH, rank by:

1. Fewest UNKNOWN criteria.
2. Fewest REVIEW criteria.
3. Greatest number of directly supported PASS results.
4. Most recent supporting evidence.

Do not generate an arbitrary eligibility probability.

### 8. Verify before responding

Before presenting the results, check:

- Every trial criterion appears in the criterion matrix.
- Every PASS and FAIL has direct FHIR evidence.
- No FHIR resource was invented.
- No unsupported unit conversion was performed.
- No missing record was treated as proof of absence.
- Inclusion and exclusion logic was not reversed.
- The patient counts reconcile.
- The overall result follows the rules above.
- The wording says potential match, not eligible.

## Output Format

Use this structure:

# Clinical Trial Cohort Prescreen

## Trial

- Title:
- Identifier:
- Target condition:
- Trial text supplied by user:
- Criteria parsed:
- Criteria requiring manual interpretation:

## Cohort

- Patients initially considered:
- Patients remaining after coarse filtering:
- Patients fully screened:
- Cohort limit:
- FHIR data cutoff:

## Summary
- Potential matches:
- Possible matches needing data:
- Not a match:
- Screening errors:

## Potential Matches

### Patient: Patient/{id}

**Overall result:** POTENTIAL_MATCH

**Summary:** One or two sentences explaining the result.

| Criterion | Type | Result | Evidence | Explanation |
|---|---|---|---|---|
| I1 | Inclusion | PASS | Patient/{id} | Patient is within the required age range. |
| I2 | Inclusion | PASS | Condition/{id} | Required condition is documented. |
| E1 | Exclusion | NO_EVIDENCE_OF_EXCLUSION | Search performed | No matching condition was found in the available record. |
| E2 | Exclusion | REVIEW | Observation/{id} | The trial uses an undefined clinical term. |

**Unknown or review items:**

- List unresolved criteria.
- State what information would resolve each item.

**Key supporting resources:**

- ResourceType/id — date — relevance.

## Possible Matches Needing Data

Use the same structure, emphasizing missing information.

## Excluded Summary
For patients classified as NOT_A_MATCH, provide a compact table:

| Patient | Blocking criterion | Evidence | Reason |
|---|---|---|---|

Avoid producing a full criterion matrix for every excluded patient
unless the user requests it.

## Limitations

Always state:

- This is automated pre-screening against structured FHIR data.
- Missing information may change the result.
- No patient has been confirmed eligible.
- Trial staff must review the official protocol and source records.
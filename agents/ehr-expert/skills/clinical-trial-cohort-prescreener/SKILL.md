---
name: clinical-trial-cohort-prescreener
description: Prescreen patients in the connected FHIR dataset against one clinical trial's inclusion and exclusion criteria. Use when the user supplies clinical trial text or eligibility criteria and asks which patients are potential matches, or requests clinical-trial eligibility prescreening or cohort screening. Produces potential-match results with evidence, never a definitive eligibility decision.
---

# Clinical Trial Cohort Prescreener

## When to Use
Use when the user provides one clinical trial, protocol, or set of eligibility criteria and asks to identify or rank possible matches in the FHIR population.
Typical requests include:

- "Find patients who could qualify for this trial."
- "Screen our patient cohort against these inclusion and exclusion criteria."
- "Compare these patients with this protocol and show the supporting evidence."
- "Rank potential trial matches and identify what information is missing."

Do not use for general patient summaries, finding a suitable trial for one patient, or making a definitive eligibility decision.

## Scope and safety

Use for one trial when the user supplies eligibility criteria and asks which
FHIR patients may match. Use **`fhir-data-access`** for every FHIR read.

- This is pre-screening, not an eligibility determination.
- Never call a patient "eligible" or generate an eligibility probability.
- Use only: `POTENTIAL_MATCH`, `POSSIBLE_MATCH_NEEDS_DATA`, or `NOT_A_MATCH`.
- Missing records never prove that a condition or exclusion is absent.
- Final eligibility requires review by the trial research team.

If the user gives no cohort limit, derive a coarse cohort and fully screen at
most 100 candidates. State that this is a limited candidate cohort, not
necessarily the full FHIR population.

## Workflow

### 1. Parse the trial

Extract the target condition, age range, administrative-sex restrictions, and
all inclusion and exclusion criteria. Split criteria into atomic statements,
preserve their original wording, and assign stable IDs: `I1`, `I2`, ... and
`E1`, `E2`, ...

Classify each criterion by domain (demographic, condition, observation,
medication, procedure, encounter, temporal, or clinical judgment) and whether
it is directly evaluable, unknown with current data, or requires review.

### 2. Build the candidate cohort

Apply the cheapest reliable filters before retrieving detailed records:

1. Target condition.
2. Age range.
3. Administrative sex.
4. Other unambiguous coded conditions.

Do not exclude through approximate semantic matching. Retain patients when a
code or display is ambiguous and mark the criterion `REVIEW`.

Track the number initially considered, remaining after filtering, fully
screened, and any limit applied.

### 3. Evaluate candidates

Retrieve only resources relevant to the criteria. Directly evaluate age,
administrative sex, coded conditions, medications, procedures, encounters,
numeric labs, date windows, simple temporal relationships, and explicit
thresholds when supported by structured data.

Assign one result per criterion:

- **PASS** — direct evidence supports the criterion.
- **FAIL** — direct evidence rules the patient out, such as age outside range,
  a documented exclusion condition, or a value outside an allowed threshold.
- **NO_EVIDENCE_OF_EXCLUSION** — exclusion criterion only; the search found no
  matching evidence. Say: "No evidence of this exclusion was found in the
  available FHIR record." Do not claim the exclusion is absent.
- **UNKNOWN** — required data is missing, coverage or timelines are
  insufficient, no relevant resource was found, units are incompatible, or
  structured data cannot resolve the criterion.
- **REVIEW** — evidence exists, but interpretation requires clinical or
  protocol judgment.

Only compare numeric values when units match or a reliable conversion is
available; otherwise use `UNKNOWN`.

Every `PASS` and `FAIL` must cite direct evidence, preferably
`ResourceType/id`, clinical date, code/display, value/unit, and a brief
explanation. For `UNKNOWN` and `NO_EVIDENCE_OF_EXCLUSION`, state what was
searched and what was unavailable. Cite the smallest sufficient evidence set,
not the entire chart.

### 4. Assign the overall result

- **POTENTIAL_MATCH** — no `FAIL`; all essential inclusion criteria are
  `PASS`; unresolved exclusions are explicitly labeled
  `NO_EVIDENCE_OF_EXCLUSION`, `UNKNOWN`, or `REVIEW`.
- **POSSIBLE_MATCH_NEEDS_DATA** — no `FAIL`, but an essential inclusion
  criterion is `UNKNOWN` or `REVIEW`. State what additional information would
  resolve it.
- **NOT_A_MATCH** — an inclusion criterion has a direct `FAIL`, or a documented
  exclusion criterion produces a `FAIL`. List the blocking criteria.

Rank in that order. Within `POTENTIAL_MATCH`, prefer fewer `UNKNOWN` items,
then fewer `REVIEW` items, more directly supported `PASS` results, and more
recent supporting evidence.

### 5. Verify

Before responding, confirm:

- Every criterion appears in the matrix.
- Every `PASS` and `FAIL` has direct FHIR evidence.
- Missing data was not treated as absence.
- No resource or unit conversion was invented.
- Inclusion/exclusion logic and overall results follow the rules above.
- Cohort counts reconcile.
- Wording says "potential match," never "eligible."

## Output

Use this structure:

```markdown
# Clinical Trial Cohort Prescreen

## Trial
- Title / identifier:
- Target condition:
- Criteria parsed:
- Criteria requiring manual interpretation:

## Cohort
- Initially considered:
- Remaining after coarse filtering:
- Fully screened:
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
**Summary:** [one or two sentences]

| Criterion | Type | Result | Evidence | Explanation |
|---|---|---|---|---|
| I1 | Inclusion | PASS | ResourceType/id | ... |
| E1 | Exclusion | NO_EVIDENCE_OF_EXCLUSION | Search performed | ... |

**Unknown or review items:** [item and data needed]
**Key supporting resources:** ResourceType/id — date — relevance

## Possible Matches Needing Data
[Use the same patient structure, emphasizing unresolved information.]

## Excluded Summary
| Patient | Blocking criterion | Evidence | Reason |
|---|---|---|---|

## Limitations
- Automated pre-screening against structured FHIR data.
- Missing information may change the result.
- No patient has been confirmed eligible.
- Trial staff must review the official protocol and source records.
```

Use full criterion matrices for potential/possible matches. Keep excluded
patients compact unless the user requests full matrices.
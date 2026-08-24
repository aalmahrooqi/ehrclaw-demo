---
name: drug-label-interactions
description: Fetch one drug's openFDA label and summarize interaction-related text (drug_interactions, boxed warnings, contraindications, warnings). Use when the user asks about FDA/openFDA labeling, interactions, contraindications, boxed warnings, or whether a named drug is mentioned against a patient's current meds. A free-text drug name is enough — resolve it to an RxCUI with rxnorm-resolve-rxcui first if one is not already known. This is a single-label lookup, not a pairwise interaction checker, and it does not decide whether a combination is safe.
---

# Drug Label Interactions

Use this skill to fetch the FDA label for one drug by RxCUI and extract the interaction-related information from that drug's label.

This skill does not compare two drugs directly. The openFDA Drug Label API returns labeling records for one drug/product; interaction information appears inside fields such as `drug_interactions`, `warnings`, `boxed_warning`, and `contraindications`.

## Safety Boundary

- Do not give medical advice or tell the user whether a patient should take, stop, or combine medications.
- Do not say a drug has "no interactions" just because the label section is empty or unavailable.
- Say "no interaction label text found" or "no matching label section found" when appropriate.
- Encourage confirmation with a pharmacist, physician, or qualified clinician for patient-specific decisions.

The `name` is optional display context. Do not resolve free-text names in this skill. If `rxcui` is missing, return `Insufficient data`.

## Core Workflow

1. Confirm a single RxCUI is present.
2. Query openFDA Drug Label by `openfda.rxcui`.
3. Extract interaction-related label sections.
4. Summarize the interaction information from the returned label text.
5. Report label identifiers and limitations.

## Fetch Label By RxCUI


```bash
curl -G "https://api.fda.gov/drug/label.json" \
  --data-urlencode 'search=openfda.rxcui:"<RxCUI HERE>"' \
  --data-urlencode "limit=5" \
  -H "Accept: application/json"
```

## Fields To Extract

Prefer these sections, in order:

- `drug_interactions`
- `boxed_warning`
- `contraindications`
- `warnings`
- `precautions`
- `warnings_and_cautions`

Also capture these metadata fields when available:

- `id`
- `set_id`
- `openfda.brand_name`
- `openfda.generic_name`
- `openfda.manufacturer_name`
- `openfda.rxcui`
- `effective_time`

## Interpretation Rules

Use one of these statuses:

- `Interaction label text found`: The returned label includes `drug_interactions` or other interaction-related text.
- `Specific mention found`: The returned label mentions the user's requested drug, class, or interaction term.
- `No specific mention found`: The label was retrieved, but the requested term was not found in the relevant sections.
- `No interaction label text found`: The label was retrieved, but no interaction-related fields were present.
- `Insufficient data`: The RxCUI was missing, openFDA returned errors, or no label records were found.

Never convert `No specific mention found` or `No interaction label text found` into "safe to combine".

## Output Format

Use this structure:

```text
Drug label interaction lookup: warfarin

Input:
- RxCUI: 11289

Status: Interaction label text found

Label:
- Brand/generic: [from openfda fields]
- Effective time: [effective_time]

Interaction-related sections:
- drug_interactions: [brief summary]
- contraindications: [brief summary if relevant]
- warnings: [brief summary if relevant]

Limitations:
- This summarizes openFDA label text for one drug.
- It is not a complete drug interaction checker.
- Missing label text does not prove the drug has no interactions.
```

If the user asked about a specific term:

```text
Specific term checked: aspirin
Result: Specific mention found
Where found: drug_interactions
Summary: [brief summary of the relevant label text]
```

## API Error Handling

- Treat openFDA `404` with "No matches found" as no label records found for that RxCUI.
- Retry transient openFDA errors once with `limit=1`.
- If multiple label records are returned, summarize the most relevant record first and mention that multiple records were found.


## Presenting the results
Prioritize critical interactions first, if multiple interactions, summarize them in a table. Keep your responses short and to the point. Always end with a disclaimer: AI generated content, always consult with a physician or pharmacist.

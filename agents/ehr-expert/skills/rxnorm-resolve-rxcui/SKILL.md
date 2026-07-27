---
name: rxnorm-resolve-rxcui
description: Resolve free-text medication names, brand names, generic names, ingredients, or drug product descriptions to RxNorm RxCUIs using RxNav. Use when asked to normalize drug names, find a drug's CUI/RxCUI, map medication text to RxNorm concepts, or prepare drug identifiers for downstream label, interaction, or medication data lookups.
---

# RxNorm Resolve RxCUI

Use this skill to turn free-text medication input into a normalized RxNorm concept. The main output is an RxCUI, plus the resolved concept name and any ambiguity notes.

## Core Workflow

1. Accept a medication name, brand name, generic name, ingredient, or product description.
2. Query RxNav `findRxcuiByString`.
3. If an RxCUI is returned, resolve it back to the RxNorm concept name.
4. If multiple candidates or no candidate appears, retry with simpler terms or likely generic/brand alternatives.
5. Return the best match, confidence, and candidate list when ambiguous.

## Name to RxCUI

Use RxNav `findRxcuiByString`:

```bash
curl -G "https://rxnav.nlm.nih.gov/REST/rxcui.json" \
  --data-urlencode "name=Ambien" \
  -H "Accept: application/json"
```

Expected response shape:

```json
{
  "idGroup": {
    "name": "Ambien",
    "rxnormId": ["131725"]
  }
}
```

For names with spaces, always use `--data-urlencode`:

```bash
curl -G "https://rxnav.nlm.nih.gov/REST/rxcui.json" \
  --data-urlencode "name=aspirin 325 MG Oral Tablet" \
  -H "Accept: application/json"
```

## RxCUI to Concept Name

Use RxNav `getRxNormName` to verify the concept name:

```bash
curl "https://rxnav.nlm.nih.gov/REST/rxcui/131725.json" \
  -H "Accept: application/json"
```

Expected response shape:

```json
{
  "idGroup": {
    "name": "Ambien"
  }
}
```

## Ambiguity Handling

If the result is ambiguous:

- Prefer exact name matches over partial matches.
- Prefer the concept type closest to the user input: ingredient, brand, clinical drug, or branded drug.
- If the user input includes dose and form, prefer a product-level RxCUI over an ingredient-level RxCUI.
- If the user input is only a simple generic name, an ingredient-level RxCUI is usually acceptable.
- If two or more candidates remain plausible, ask for clarification instead of guessing.

## Retry Strategy

If no RxCUI is returned:

1. Remove punctuation and extra words.
2. Try the active ingredient if the input appears to be a brand.
3. Try the brand if the input appears to be a generic product description.
4. Remove dose, strength, and form to find the ingredient-level concept.
5. Report `not resolved` if no reasonable match is found.

## Output Format

Use this structure:

```text
Input: Ambien

Resolved concept:
- RxCUI: 131725
- RxNorm name: Ambien
- Confidence: high

Notes:
- Input matched a brand-name concept.
```

For ambiguous results:

```text
Input: aspirin

Status: ambiguous

Candidates:
- RxCUI: 1191, name: aspirin
- RxCUI: ..., name: aspirin product variant

Clarification needed:
- Do you want the ingredient concept or a specific dose/form product?
```

## Downstream Use

When another skill needs drug identifiers, return a compact object:

```json
{
  "input": "Ambien",
  "rxcui": "131725",
  "rxnorm_name": "Ambien",
  "confidence": "high"
}
```

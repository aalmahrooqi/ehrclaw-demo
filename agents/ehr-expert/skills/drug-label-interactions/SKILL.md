---
name: drug-label-interactions
description: Retrieve FDA drug-label evidence for medication interaction, contraindication, or warning questions using the packaged lookup_labels.py command. Accepts drug names or known RxCUIs and returns verified label identities, excerpts, and direct source links. Use for named-drug or medication-pair questions, including medication-safety questions that need factual label information. Does not determine whether a patient should take a medicine; not needed for a chart-only conditions or medication-list question.
---

# Drug Label Interactions

Use the packaged command below for label retrieval. It owns name resolution,
RxCUI lookup, verified ingredient fallback, JSON parsing, and bounded retries.
There is no need to load `rxnorm-resolve-rxcui` separately for this workflow.

## Run the lookup

Pass the medicines relevant to the clinician's question, at most three per call:

```bash
python3 /sandbox/.hermes/skills/drug-label-interactions/scripts/lookup_labels.py --drug ibuprofen --drug naproxen --drug clopidogrel --term bleeding --term NSAID --term antiplatelet
```

Use known chart RxCUIs when available instead of resolving those names again:

```bash
python3 /sandbox/.hermes/skills/drug-label-interactions/scripts/lookup_labels.py --rxcui 849574 --rxcui 309362 --term bleeding --term NSAID
```

`--term` is optional literal excerpt filtering, not a clinical classification.
Choose terms from the question or retrieved evidence. For a simple question
about one medicine, pass only that medicine. Patient context already supplied
in the conversation does not need to be fetched again to run this label lookup.

Execute [scripts/lookup_labels.py](scripts/lookup_labels.py) directly. Do not
replace it with hand-written curl commands, inline Python, `execute_code`, or
temporary parsers. Do not patch the script during a clinical lookup. If it is
missing or fails, report that failure. If a tool requires approval, stop and
surface the requirement; do not switch execution methods to avoid it.

The command returns JSON with one result per medicine. It exits `2` for an
unresolved/failed lookup; read any successful results separately. Do not restart
the same failed lookup: the command already retries transient errors once and
stops at its request/time budget. Clarify ambiguous drug names with the clinician.

## Read the evidence

- `resolved_name`, `rxcui`, and `concept_type` identify the RxNorm concept.
- `match_basis` distinguishes a direct RxCUI match from an ingredient fallback.
  A fallback does **not** establish the patient's exact dose, formulation,
  manufacturer, or product. Never silently present it as an exact product match.
- `label.sections` contains source excerpts, literal term-match counts, and
  explicit truncation flags. Arrays and strings are already handled. Do not
  interpret missing terms or omitted text as no risk, no interaction, or safety.
- `Insufficient data` means the lookup did not verify label evidence. Explain
  the returned reason without adding interaction claims from model knowledge.
- The command examines at most five label candidates and returns one label per
  medicine. It is not an exhaustive or validated pairwise interaction checker.

## Answer the question

Keep the answer brief: a small table with medicine/label, relevant warning,
and a **direct `label_url` or `source_url` citation** for each claim. Use the
returned label dates and identities; a generic FDA homepage is not a citation.
Mention a material ingredient/product mismatch or truncated evidence.

Separate label statements from patient-reported use and charted prescriptions.
An active MedicationRequest does not confirm the patient is taking the drug.
Do not approve a combination, choose treatment, infer a diagnosis, or convert
label statements into a patient-specific safety verdict. For "is it safe?",
state that suitability needs clinician review and report only the retrieved
label information relevant to that question. End with a short clinician-review
reminder. Never fill retrieval gaps with model knowledge.

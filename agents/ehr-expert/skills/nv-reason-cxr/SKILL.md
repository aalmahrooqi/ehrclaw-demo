---
name: nv-reason-cxr
description: Run NVIDIA NV-Reason-CXR-3B over a chest X-ray image file and report the model's findings, support-device observations, or a structured report. Use when the user points at a chest X-ray / CXR / chest radiograph image in the sandbox and asks to analyze it, find abnormalities, answer a question about a finding, or write a radiology-style report. Needs an image file path — this skill reads pixels from disk, it cannot pull images out of FHIR, and it never decides a diagnosis or a treatment.
---

# NV-Reason-CXR

Interpret a chest X-ray image with [NV-Reason-CXR-3B](https://github.com/NVIDIA-Medtech/NV-Reason-CXR)
through a thin wrapper script. The wrapper does the HTTP call, image handling,
and JSON shaping; this skill decides which prompt to send and how to report
what comes back.

## Safety Boundary

- Model output is **not** a diagnosis, triage decision, or treatment recommendation.
- Report what the model said. Do not add your own read of the image, and do not
  upgrade a hedged finding into a conclusion.
- Do not say a chest X-ray is "normal" or "clear" on the model's behalf; say the
  model reported no abnormalities and that a radiologist must confirm.
- Always end with the disclaimer: AI-generated output, must be reviewed by a
  qualified clinician.

## Environment

- Wrapper: `/sandbox/.hermes/skills/nv-reason-cxr/scripts/run_nv_reason_cxr.py`
- Images: `/sandbox/cxr/`, PNG and JPEG only. Attaching an image in the
  dashboard chat writes it here and puts the path in the user's message, so the
  path you are handed usually already exists. From the host the equivalent is
  `openshell sandbox upload hermes-health <file> /sandbox/cxr/<file>` — the
  destination has to name the file, a bare directory fails.
- Backend: always `--backend hf-space-api`. It calls the public NVIDIA Space
  over the sandbox egress proxy and needs no model weights, GPU, or token.
  `--backend local` is **not available here**: this sandbox has no GPU
  passthrough and no torch/transformers, so it will fail. Do not try it.

Never redirect the wrapper's stdout with `>` — the JSON on stdout is the result.

## Core Workflow

1. Confirm you have an image path. If the user named a file without a
   directory, look in `/sandbox/cxr/` first with `ls /sandbox/cxr/`.
2. Pick a prompt with the routing rules below.
3. Run the wrapper.
4. Read `output.response_text` out of the JSON and report it in full.

## Running It

```bash
python3 /sandbox/.hermes/skills/nv-reason-cxr/scripts/run_nv_reason_cxr.py \
  /sandbox/cxr/<image>.png \
  --prompt "Find abnormalities and support devices." \
  --backend hf-space-api
```

If no image is available at all, run a wiring check instead of guessing — it
calls nothing and returns a canned response:

```bash
python3 /sandbox/.hermes/skills/nv-reason-cxr/scripts/run_nv_reason_cxr.py \
  /sandbox/cxr/<image>.png --mock
```

Say explicitly when a result came from `--mock`; it is not model output.

## Prompt Routing

Decide the prompt before running. Check these in order:

1. **Exact prompt requested.** The user says "send this prompt exactly" — pass
   only their text as `--prompt` and return the raw response.
2. **Report requested.** The user asks for a report, radiology report, or
   structured write-up — use the two-call pattern below.
3. **General analysis.** "Analyze this X-ray", "what's wrong with this chest
   film", "find abnormalities" — use
   `--prompt "Find abnormalities and support devices."`
4. **Specific question.** "Is there a pneumothorax?", "how many nodules?",
   "which side is the effusion on?" — still run
   `--prompt "Find abnormalities and support devices."`, then answer the
   question from that output in plain text prefixed exactly with `Answer:`.

Orchestration details are yours, not the model's. Never put a file path, a
backend name, or wording like "use the API" into `--prompt`.

### Report Generation (two calls)

First gather context with `--prompt "Examine the chest X-ray."` and do not show
that call. Then run again with the transcript as the prompt, and treat the
second call as the result:

```bash
IFS= read -r -d '' prompt <<'PROMPT'
User: Find abnormalities and support devices.

Assistant:
<raw model context>

User: Write a structured report.
PROMPT

python3 /sandbox/.hermes/skills/nv-reason-cxr/scripts/run_nv_reason_cxr.py \
  /sandbox/cxr/<image>.png --prompt "$prompt" --backend hf-space-api
```

Use the here-doc form, not command substitution, so tags and quotes in the
prior output survive. If you already have sufficient raw context for the same
image from an earlier call, skip straight to the second call.

## Follow-ups

Reuse the raw model context already in the conversation when it answers the
follow-up. Call the wrapper again only when the prior context is insufficient
and the same image is still on disk. If the image is gone, ask the operator to
re-upload it rather than analyzing a different file.

## Reporting Results

Return `output.response_text` complete, including any reasoning or answer
sections the model emits. Do not compress it to a label list unless the user
asked for a summary.

```text
Chest X-ray analysis: /sandbox/cxr/<image>.png

Model: nvidia/NV-Reason-CXR-3B (hf-space-api)
Prompt: Find abnormalities and support devices.

Model output:
[output.response_text verbatim]

AI-generated output from an imaging model. Not a diagnosis — a qualified
clinician must review the image and confirm any finding.
```

## Relationship To The Record

This skill reads a file; it has no link to the FHIR server. Do not claim an
image belongs to a patient unless the operator told you so, and do not write
findings back to the record — there is no imaging write path in this demo. A
patient's `ImagingStudy` entries in FHIR are DICOM metadata only and contain no
pixel data, so they cannot be fed to this model.

## Troubleshooting

| Symptom | Cause | What to do |
|---|---|---|
| `No such file or directory` | Wrong path or image never uploaded | `ls /sandbox/cxr/` and ask the operator to upload if empty |
| Connection or proxy error | Sandbox egress does not allow the Space | Report it; the host operator must allow `nvidia-nv-reason-cxr.hf.space` in `policy/policy.yaml` |
| HTTP or schema error from the backend | Public Space down, rate limited, or changed | Retry once, then report the failure — do not fall back to `--backend local` |
| `Please provide a frontal chest x-ray image` | The file is not a frontal CXR | Say the model rejected the input and ask for a frontal chest radiograph |
| `runtime.truncated_by_max_new_tokens: true` | Response hit the cap | Re-run with a larger `--max-new-tokens` |

## Provenance

`scripts/run_nv_reason_cxr.py` is vendored unmodified from
[NVIDIA-Medtech/medical-AI-skills](https://github.com/NVIDIA-Medtech/medical-AI-skills/tree/dev/skills/nv-reason-cxr)
at commit `504575adb12a62bfd956fd395e144b99a177dbd6` (Apache-2.0). The model
weights are covered by the NVIDIA OneWay Noncommercial License Agreement.

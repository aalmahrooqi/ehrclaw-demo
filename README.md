# CliniClaw

## Running this for BaC?

`./bac_setup.sh` does every step in this README for you, end to end, using the local vLLM model:

```bash
./bac_setup.sh
```

> If you get an Openshell timeout error, running the script again usually should work.


It installs the NemoClaw CLI if it's missing, downloads and extracts the synthetic patient data, starts the FHIR server and loads the bundles into it, brings up local vLLM, then builds the Hermes image and onboards the `hermes-health` sandbox.

The first run takes a while — it pulls the vLLM image and downloads the model weights into `~/hf-cache`. Those logs stream to `logs/vllm.log`, so `tail -f logs/vllm.log` is the place to look if it seems stuck.

When it finishes, open the dashboard at **http://127.0.0.1:8090**!

Tear it all down with `./scripts/shutdown.sh`.

The rest of this README covers the same steps manually, plus how the pieces fit together.

## About

A patient's records can be dense and sprawling, spanning many visits and years of history that take real time to read through. CliniClaw helps clinicians cut through that: get a quick snapshot or ask a specific question and get an answer in seconds — so physicians can spend more time giving care and listening to patients, and less time navigating charts.

CliniClaw is a safe, sandboxed agent for exploring patient health records. Ask it questions in plain language and it looks up the answer from a patient's chart: labs, medications, conditions, visits, and more, while staying strictly within safe boundaries: it retrieves and reports information, but never gives medical advice. It can also check a patient's medications against a drug's labeling for interactions, warnings, and contraindications. Physicians may also ask the agent to prescribe drugs to the patient in natural language, and CliniClaw resolves it to the right drug concept (RxNorm CUI), builds the underlying FHIR request, and writes it to the record for the patient. And given a clinical trial's criteria, it can prescreen the patient population to surface potential matches, each backed by evidence from the record, to help shortlist candidates worth a closer look.

![CliniClaw architecture](assets/architecture.png)

## Components

| Component | Purpose |
|---|---|
| NemoClaw / OpenShell | Secure agent execution, tool isolation, network policy, and credential injection at the egress boundary |
| Hermes | Agent runtime and skill orchestration |
| NeMo Relay | Traces the agent's LLM/tool workflow (ATOF/ATIF exports) and manages adaptive execution, with NemoGuardrails plugin |
| NeMo Guardrails | Blocks medical advice / drug recommendation requests |
| HAPI FHIR | Stores and serves the patient / clinical records (FHIR R4) |

## FHIR Server

This project runs a local **FHIR R4** server using [HAPI FHIR](https://hapifhir.io/) with PostgreSQL. Sample patient records live in `data/` and come from [Synthea](https://synthea.mitre.org/downloads), hosted as [`aalmahrooqi/synthetic-fhir-data`](https://huggingface.co/datasets/aalmahrooqi/synthetic-fhir-data).

## Requirements

- **Python 3** — standard library only, used by `scripts/load_synthea.py` and the dashboard
- **Docker**
- [**NemoClaw**](https://github.com/NVIDIA/NemoClaw) v0.0.97:
```
curl -fsSL https://www.nvidia.com/nemoclaw.sh | \
  NEMOCLAW_INSTALL_REF= \
  NEMOCLAW_INSTALL_TAG=v0.0.97 \
  bash
  ```

### 1. Download Sample Data

Download the Synthea FHIR bundles from [Hugging Face](https://huggingface.co/datasets/aalmahrooqi/synthetic-fhir-data) and extract them into `data/`:

```bash
curl -L -o data.zip "https://huggingface.co/datasets/aalmahrooqi/synthetic-fhir-data/resolve/main/data.zip" && unzip -q data.zip
```


### 2. Start the server

```bash
docker compose up -d
```

The server listens on **http://localhost:8081**

Sanity check:
```bash
curl -s 'http://localhost:8081/fhir/Patient?_summary=count'
```

### 3. Load Synthea data

With the server running, upload the bundles from `data/`:

```bash
python3 scripts/load_synthea.py --data-dir data
```

This posts each Synthea transaction bundle to the FHIR API. The first full load takes a few minutes.

## Agent Setup

The Hermes agent runs in an OpenShell sandbox managed by the **NemoClaw** CLI.

### Quick install

From the repo root, run the startup script that matches your inference setup:

| Script | Inference provider |
|---|---|
| `./scripts/startup_w_inference_api.sh` | NVIDIA-hosted API — set `COMPATIBLE_API_KEY` in the script first |
| `./scripts/startup_w_local_model.sh` | Local vLLM on port 8000 — start it first with `scripts/run_vllm.sh` |

Each one exports its provider settings and then calls `scripts/setup_helper.sh`, which builds the vendored Hermes image (with [`nemo-relay`](https://github.com/NVIDIA/NeMo-Relay) baked in) and onboards a fresh `hermes-health` sandbox non-interactively.

Once it finishes, connect and start chatting:

```bash
nemoclaw hermes-health connect
sandbox$ hermes chat
```

To stop the FHIR server, the dashboard, and the local vLLM container:

```bash
./scripts/shutdown.sh
```

## Demo Dashboard

A lightweight web dashboard for following a synthetic patient from intake to
clinician review and watching the agent work:

- **review queue** — six seeded, FHIR-linked cases, all initially unranked. Ask CliniClaw to review all open patients together from their intake transcripts, automatically update the queue, then generate individual source-labeled handoffs when needed. Completed, validated reviews update the applied patient list automatically; the immediate-attention section has no preset members.
- **activity** — the agent's read-only SOUL.md identity document, memory-learning feed, and skill-evolution feed. SOUL.md is read from the live sandbox every 30 seconds (override its path with `HERMES_SOUL_FILE` or `--soul-file`); failed refreshes are marked stale, never silently replaced with a repository copy.
- **patient drawer** — intake, AI summary, and one patient-scoped chat interface with streamed replies, collapsible agent steps, and an expand-to-full-screen button. Attach a PNG/JPEG with 📎, drag-and-drop, or paste, and it lands in `/sandbox/cxr/` for the chest X-ray skill.
- **hermes** — the native Hermes dashboard, embedded.
- **audit** — live [ATOF](https://docs.nvidia.com/nemo/relay/) trace events, with a per-session browser, pretty JSON rendering, and raw `.jsonl` download.

Patients have distinct, locally rendered pixel-art portraits shared across the
queue, patient header, and intake transcript. Nurse and parent speakers have
their own portraits plus small role labels. Chat uses a clinician portrait for
your messages and the medical lobster CliniClaw for agent replies, including saved
history, streaming turns, and full-screen mode. These are decorative demo
illustrations, not patient photographs or clinical data; no external avatar
service is contacted.

The six in-person intakes cover different reasons for a visit:

| Patient | Presenting concern | Useful detail for the summary |
| --- | --- | --- |
| Paul | Sneezing and itchy eyes affecting school | Child and parent accounts; corrected timing; sleep and activity impact |
| Dara | New fatigue and recurrent lightheadedness | Different symptom timelines; heavier last period; missed blood-test visit |
| Florence | New ankle injury | Worsening pain/swelling; standing versus resting pain; corrected walking history |
| Jacques | Back-pain flare | New episode versus chronic history; work and sleep impact; stated symptom negatives |
| Norbert | Historical laboratory results | Date correction; uncertain subsequent testing and follow-up |
| Dorsey | Ongoing chest pressure with associated symptoms | Current symptoms and onset versus historical chart diagnoses |

The first four conversations are 31–33 turns (roughly 600–675 words). They keep
medicines as supporting context rather than a shared reason for every visit.
Handoffs still target 120–160 words with the same four sections. Current intake
details are fictional; especially Florence's injury is not a new FHIR encounter,
diagnosis, or imaging result. No server-side patient records are modified.

The revised scripts have new local state keys, so earlier medication-review
summaries and chats do not carry over. The previous queue review is archived in
local demo state when its review-basis version changes; unchanged patients retain
their saved history and applied placements. Run **Triage Queue** again to assess
the new stories. New shared `worsening_function` and `symptom_assessment` rules
support these visits, after checking for current warning signs. No patient has a
preset priority, and a work or school form does not turn an unresolved symptom
visit into an administrative-only case. These remain demonstration workflow
rules, not a validated clinical triage scale or evidence that waiting is safe.

The injury and symptom scripts were checked against general
[NHS injury](https://www.nhs.uk/conditions/sprains-and-strains/),
[back-pain](https://www.nhs.uk/conditions/back-pain/), and
[dizziness](https://www.nhs.uk/symptoms/dizziness/) information for plausible
symptom descriptions, not to derive or validate the demo's queue categories.
Run `node scripts/test_dashboard_intakes.mjs` for offline fixture, prompt,
validation, and saved-state isolation checks; these do not evaluate a live model.

The dashboard opens to the **waiting room**. Click a patient in the room or list
(or press Enter/Space on a focused patient) to open their drawer. **Intake** shows
the full transcript and expandable queue evidence; **AI summary** generates or
regenerates the chart-grounded handoff; **Chat** continues the patient's conversation.
Close the patient view to choose another patient from the waiting room or sidebar.
Patient switching is disabled during an active agent turn.

The drawer can expand to full screen or minimize to a return button over the room.
In expanded view, the intake transcript and AI summary appear side by side,
without separate tabs. **Generate summary** automatically opens the existing chat
so you can follow the live work and continue the conversation. Expanded chat sits
alongside the records on desktop; **Hide chat** gives the records more room. Smaller
screens stack the records and chat to keep them readable.
Close it, click the backdrop, or press Escape to return to the room; from full screen,
the first Escape returns to the side panel. On phones, the drawer uses the full width.
The same chat iframe stays mounted across tabs, minimizing, and expansion, preserving
sessions, drafts, attachments, and live responses. Each patient's reading position is
kept for the intake and summary tabs while the page is open. **Generate summary**
runs on demand; opening a patient or tab makes no agent request.

Press Enter to send a chat message and Shift+Enter for a new line. Follow-ups and
regenerated handoffs reuse the patient's session; only **New conversation** starts
a fresh one. Follow-ups leave saved handoffs and queue placements unchanged. Old
`/chat` and `/#chat` links open the same chat in full-screen mode.

Start with **Triage Queue · 6 patients** above the patient list. One read-only
agent run includes every open case, even those hidden by a filter or search;
closed cases and other patients in the FHIR database are excluded. The input
contains only full intake transcripts plus case identifiers. Queue reviews skip
chart lookups, saved handoffs, and chat history to keep the demo fast. Shared
rules determine proposed priorities; all supporting factors must cite intake.
Missing facts and later chart checks are listed under Needs review. Chart access
remains available in individual patient handoffs and chat.

The waiting room includes every open patient, even when the sidebar is filtered.
A compact toolbar leaves space for the complete scene, which scales to the available
width and height without scrolling or cropping. The patient list scrolls independently.
Patients wait on connected, perforated metal benches while CliniClaw, a pixel-art
medical lobster in a white coat, sits at the intake desk. The room and chat
share the same local SVG character.
When CliniClaw finishes a valid review, seated patients appear in the updated order
instantly. The patient list updates automatically at the same time,
with #1 closest to the **Doctor** door. Colored badges show priority: red for
Immediate attention, amber for Review first, blue for Review soon, green for
Routine. Pending badges stay hidden until review; an inconclusive review shows
a gray Needs info badge. A question mark flags a need for more information;
previously assigned priorities are retained for unsupported results.

Hover or keyboard-focus a patient for their queue evidence; click or tap to
open their focused view. Queue evidence is also available inside the drawer. The expanded room includes wider bench spacing, an aquarium, a water
cooler, a snack machine, and a magazine table in front of each bench group. There is no Apply step or
confirmation panel; valid reviews update the whole queue automatically.
Stale, failed, and interrupted reviews display the existing applied order.
Invalid or empty responses trigger one automatic full-batch retry. The same intake
snapshot and validation rules apply; no partial or repaired response changes placement.
The dashboard uses static visuals, without idle gestures, queue movement animations,
or animated visual effects.

CliniClaw's speech bubble mirrors the current chat activity; click it to open the
full queue conversation. Patient small talk comes from
`assets/waiting-room-dialogue.json`: edit `lines`, `intervalMs`, `visibleMs`, or
`firstDelayMs` to configure the jokes and timing. One patient speaks
at a time, with no immediate repeat of the speaker or joke. Small talk runs
automatically while the waiting room is visible. These local, decorative lines
make no LLM requests and never enter
intakes, clinical chat history, or queue evidence. Rebuild the dashboard after
editing the file.

The full response must include each patient exactly once with matching case and patient
IDs, valid rules, and patient-tagged evidence. Partial or malformed responses,
failed runs, and stopped reviews leave all applied placements intact. Changes to
open cases, intakes, the review method, or applied placements invalidate an
unapplied draft and require a new review.

**Triage Queue** or **Reassess queue** starts a fresh, dedicated queue session. Patient handoffs and
follow-ups retain their own sessions, histories, and drafts. The queue activity
panel is read-only. Patient chats do not feed into this intake-only review.
Reassess after intake changes; an unapplied draft from the previous chart-based
workflow must also be reassessed. Results needing more information preserve any existing
placement, including an immediate-attention placement. Previously
unranked patients remain Pending; this does not imply low urgency.

**Dorsey Aurelio Macejkovic (Patient/65157)** has a focused, fictional in-person
chest-pain intake. The nurse asks about onset, location, pain severity (eight out
of ten), spread into the left arm, changes with position or breathing, and
associated breathlessness, sweating, nausea, and lightheadedness. The longer
conversation contains symptom questions and patient answers, with no scripted
escalation or AI/queue instructions. Handoffs remain available on demand.
Dorsey starts **Pending**, just like everyone else. Generating
a summary or chatting does not rank him. If the agent proposes **Immediate
attention**, the completed review automatically moves him to the red section. The same rule and rendering work for any patient.

The scenario references [NIH heart-attack warning-sign guidance](https://www.nhlbi.nih.gov/health/heart-attack/symptoms).
This is a demonstration, not validated clinical triage: real clinical care must
never wait for AI or this simulated workflow. The app sends no actual
emergency notification. Historical Condition/65992, dated July 14, 2023, does not
establish today's diagnosis. The revised scenario has a fresh local state key;
older medication-review, preset-escalation, and brief-intake conversations remain stored
separately and cannot assign priority or contaminate the new run.

Applied immediate-attention placements remain visible in the patient list.
The patient-detail and chat views do not repeat the alert banner or require
acknowledgment. Filtering other reviews does not hide the applied placement.

**Norbert Dwain Muller (Patient/114506)** remains the contrasting historical-result
verification case with a long intake. His February 11, 2026 labs do not trigger a
new urgency rule; without another supported workflow rule, the agent should
return **More information needed**. All scripts distinguish invented intake
reports from separately retrieved chart evidence.

A complete, valid queue review updates all supported placements together and
records an automatic update in the queue session, without claiming clinician confirmation.
Within an applied priority, longest-waiting cases appear first. Partial responses,
failed runs, and ordinary follow-up questions do not reorder patients.
Applied placements update instantly in both the waiting room and sidebar,
including transfers into or out of the immediate-attention section.
A screen-reader status announces the placement changes.

After the queue updates, use **Generate handoff** when ready for a detailed
review. It runs on demand in the same patient conversation; there is no automatic
background run. Handoff generation and regeneration preserve draft proposals
and applied placement. Another completed whole-queue review updates placement. Existing saved conversations remain available.

With the `hermes-health` sandbox running, start it with Docker Compose:

```bash
docker compose -f docker-compose.dashboard.yaml up --build -d
```

Then open **http://127.0.0.1:8090**. To stop it: `docker compose -f docker-compose.dashboard.yaml down`.

To run the isolated chat UI regressions (Node 22+, Python 3, and Chrome required):

```bash
node scripts/test_dashboard_chat.mjs
```
The test launches a temporary dashboard and browser and mocks agent responses.

## NeMo Guardrails

[NeMo Guardrails](https://github.com/NVIDIA/NeMo-Guardrails) keeps the assistant on-task — it blocks the agent from giving medical advice or drug recommendations, so it stays a data-retrieval tool rather than a clinical decision-maker.

It's integrated through **NeMo Relay** ([Nemo Guardrails Plugin](https://docs.nvidia.com/nemo/relay/configure-plugins/nemo-guardrails/about)), and runs in **local** mode, so NeMo Relay spawns an in-sandbox Python worker that applies self-check input/output rails (defined in `configs/nemo_guardrails/config.yaml`) to every managed LLM turn. The self-check calls the same OpenShell-managed inference route as the agent, so no API key ever lives in the sandbox.

The local backend requires `nemoguardrails==0.22.0`, which depends on `annoy` — a package that ships source-only on PyPI (no prebuilt wheel for Python 3.13). Since the hardened sandbox image has no C/C++ compiler, we vendor a prebuilt `annoy` wheel under `vendor/wheels/` and install from it at build time so nothing needs to compile inside the image.

## Skills

Agent skills live under `agents/ehr-expert/skills/`. Each skill documents how the agent queries an external data source.

### Intake Transcript Summarization

Turns a synthetic nurse/patient intake transcript into a 120–160 word,
source-labeled handoff with four sections: Intake summary, Reported at intake,
Chart matched, and Needs review. The review queue renders these as compact
cards and keeps handoff generation separate from automatic whole-queue ordering.

### FHIR Data Access

Reads patient and clinical data from the local HAPI FHIR R4 server (patients, observations, medications, encounters, conditions, etc.) via its REST API. See [HAPI FHIR](https://hapifhir.io/).

### RxNorm Resolve CUI

Normalizes free-text medication names to [RxNorm](https://www.nlm.nih.gov/research/umls/rxnorm/index.html) concepts using the NLM [RxNav](https://rxnav.nlm.nih.gov/) API. Returns an RxCUI plus ambiguity notes when a name does not map cleanly.

Example:

```text
Input: Ambien

Resolved concept:
- RxCUI: 131725
- RxNorm name: Ambien
- Confidence: high
```

### Drug Interaction Label Check

Fetches FDA drug labeling for a known RxCUI from [openFDA](https://open.fda.gov/) and summarizes interaction-related label text (e.g. drug interactions, boxed warnings, contraindications). This is a single-drug label lookup, not a pairwise interaction checker.

### Clinical Trial Cohort Prescreener

Prescreens patients in the FHIR dataset against a single clinical trial's inclusion/exclusion criteria (supplied as text) and reports **potential matches** with supporting evidence. Builds on the FHIR Data Access skill: it derives a coarse candidate cohort (by condition, age, and sex), evaluates each criterion against the record, and returns a per-patient result matrix. It never declares a patient definitively *eligible* — final eligibility must be confirmed by the trial research team.

### Chest X-ray Reasoning (NV-Reason-CXR)

Interprets a chest X-ray image with [NV-Reason-CXR-3B](https://github.com/NVIDIA-Medtech/NV-Reason-CXR): findings, support devices, answers to a specific question about the film, or a structured radiology-style report. The wrapper script under `scripts/` is vendored unmodified from [NVIDIA-Medtech/medical-AI-skills](https://github.com/NVIDIA-Medtech/medical-AI-skills/tree/dev/skills/nv-reason-cxr) (Apache-2.0).

Inference runs against the public [NV-Reason-CXR Hugging Face Space](https://huggingface.co/spaces/nvidia/nv-reason-cxr), which the wrapper reaches with Python's standard library alone — no model weights, GPU passthrough, or Hugging Face token inside the sandbox. The Space is allowlisted for the sandbox's python binaries in `policy/policy.yaml`; the upstream `--backend local` path needs torch, CUDA, and the 3B weights and is not wired up here. Model weights are covered by the NVIDIA OneWay Noncommercial License Agreement.

This skill reads pixels from a file and has no link to the FHIR server — the Synthea `ImagingStudy` resources are DICOM metadata with no image data. Images live in `/sandbox/cxr/` inside the sandbox.

The easiest way in is the dashboard **side chat**: click 📎 (or drag an image onto the panel, or paste one from the clipboard). The dashboard writes it to `/sandbox/cxr/` and appends the sandbox path to your message, so you can just say "what do you see?" with the file attached. From a host terminal the equivalent is:

```bash
openshell sandbox upload hermes-health ./my-cxr.png /sandbox/cxr/my-cxr.png
```

Either way, the agent works from the path:

```text
> analyze the chest x-ray at /sandbox/cxr/my-cxr.png
> write a structured report for /sandbox/cxr/my-cxr.png
```

Output is AI-generated engineering evidence, not a diagnosis or a radiology read.

## Disclaimers
*This demo is not intended to be used in a clinical setting or for actual patient care. It is a technical demonstration only and does not provide medical advice, diagnosis, or treatment. All patient records are synthetic and do not represent real individuals. This product uses publicly available data from the U.S. National Library of Medicine (NLM), National Institutes of Health, Department of Health and Human Services; NLM is not responsible for the product and does not endorse or recommend this or any other product.*

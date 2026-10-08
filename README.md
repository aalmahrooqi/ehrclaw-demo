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
| Florence | New ankle injury and OTC painkiller question | Worsening pain/swelling; reports Aleve this morning and asks about adding ibuprofen; dose and timing need verification |
| Jacques | Back-pain flare | New episode versus chronic history; work and sleep impact; stated symptom negatives |
| Carmen | Knee pain and a question about ibuprofen | Proposed medicine versus actual use; old active prescriptions to reconcile |
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
When CliniClaw finishes a valid review, the patient list updates immediately and
patients walk to their new seats over about four seconds,
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
Documents fly past CliniClaw during review, and patients walk after a validated
change of order. Queue chat can stay open beside the room on desktop; opening or
closing that sidebar does not cancel the walk. Reduced-motion preferences disable
these effects. Idle claws, fish, bubbles, and the desk monitor animate locally.

**Close case** dismisses the patient view and shows the patient walking from the
Doctor door down the side aisle and out of the room over about five seconds.
The six chairs stay in place, and that patient's seat stays empty through page
reloads and further queue reviews. Saved summaries and conversations remain in
the completed case. Reduced motion skips the exit walk; **Reset Demo** restores
all patients and seats. Existing review and immediate-attention completion rules
still apply. Verify with `node scripts/test_dashboard_departure.mjs`.

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

Click CliniClaw’s speech bubble to ask questions before or after triage. The
conversation has the open patients’ intake context and current applied priorities;
ordinary replies never apply a queue proposal. Waiting-room drafts and history
are saved separately from patient conversations.

**Triage Queue** or **Reassess queue** starts a fresh, dedicated queue session. Patient handoffs and
follow-ups retain their own sessions, histories, and drafts. An open queue chat
sidebar stays open during review and accepts follow-up questions when it finishes.
Prior chat messages do not feed into the new intake-only review.
Reassess after intake changes; an unapplied draft from the previous chart-based
workflow must also be reassessed. Results needing more information preserve any existing
placement, including an immediate-attention placement. Previously
unranked patients remain Pending; this does not imply low urgency.

**Dorsey Aurelio Macejkovic (Patient/63738)** has a focused, fictional in-person
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
emergency notification. Historical Condition/64573, dated July 14, 2023, does not
establish today's diagnosis. The revised scenario has a fresh local state key;
older medication-review, preset-escalation, and brief-intake conversations remain stored
separately and cannot assign priority or contaminate the new run.

Applied immediate-attention placements remain visible in the patient list.
The patient-detail and chat views do not repeat the alert banner or require
acknowledgment. Filtering other reviews does not hide the applied placement.

**Carmen Evan Wiegand (Patient/39153)** replaces Norbert in the waiting room.
His fictional intake describes a week of knee pain and a question about trying
ibuprofen/Advil, which he has not started. The chart supplies a separate story:
knee osteoarthritis, heart and kidney history, and active prescription records
for naproxen and clopidogrel. An active order does not establish actual use;
the naproxen order's 1994 authored date makes this distinction easy to explain.
No priority is preset, and historical conditions or medicines do not rank him.

After **Triage Queue**, select Carmen and use **Generate summary**. Short follow-ups:

| Prompt | Demo focus |
| --- | --- |
| List his active prescriptions and dates. | Retrieve chart facts; distinguish orders from actual use. |
| Look up ibuprofen warnings about other painkillers and kidney disease. | Retrieve label warnings for clinician review. |
| What does clopidogrel's label say about NSAIDs? | Explain a label-based interaction concern in everyday language. |
| Which active prescriptions are over ten years old? | Find old orders that need medication reconciliation. |

These are information-retrieval prompts, not requests for the agent to approve
ibuprofen or choose treatment. The after-visit summary extracts explicit
clinician decisions from the chat and confirmed demo actions. Any missing plan
remains marked for review; the intake does not invent a completed visit.
Carmen has a fresh local state key, so Norbert's saved chat, handoff, after-visit
summary, completion status, and vacated seat cannot carry over. The prior queue
review is archived and can be rerun for the new lineup. Other patients retain
their saved state; underlying FHIR records are unchanged.

A complete, valid queue review updates all supported placements together and
records an automatic update in the queue session, without claiming clinician confirmation.
Within an applied priority, longest-waiting cases appear first. Partial responses,
failed runs, and ordinary follow-up questions do not reorder patients.
Applied placements are saved immediately, including transfers into or out of the
immediate-attention section; the room animates each changed seat afterward.
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

Agent skills live under `agents/ehr-expert/skills/`. Each skill documents a clinical data or summarization workflow.

### Intake Transcript Summarization

Turns a synthetic nurse/patient intake transcript into a 120–160 word,
source-labeled handoff with four sections: Intake summary, Reported at intake,
Chart matched, and Needs review. The review queue renders these as compact
cards and keeps handoff generation separate from automatic whole-queue ordering.

### Visit actions inside the patient chat

Actions now appear directly below CliniClaw's replies. There is no separate
**Visit actions** tab. Ask in the chat or use its **Follow-up**, **Prescription**,
**X-ray**, and **Labs** shortcuts; review, edit, confirm, or cancel the resulting
cards without leaving the conversation.

The visit moves from **summary → chart/medication questions → clinician plan →
actions → after-visit summary**. Summary generation stays focused on the handoff.
During Q&A, a focused blood-result or monitoring question that reveals a relevant
testing gap can prompt “Would you like me to prepare a blood-test request for
review?” The answer keeps its dated FHIR evidence, and the offer cites the
clinician's question. It does not mean a test is overdue or an order is placed.
Routine evidence review without a relevant gap needs no offer. A clinician plan,
follow-up discussion, or wrap-up can also prompt up to two relevant next steps,
with a **Why this option?** explanation grounded in the clinician's messages.
Replying to a draft offer prepares it; it does not confirm a clinical order. Dismissed or
already-offered options are not repeated. The agent does not choose a medicine,
dose, test, or treatment on the clinician's behalf.

- When follow-up comes up, CliniClaw immediately offers the next three available
  fictional weekday times in clinic local time. Reply **“Book the second one”**
  or **“Option 2”** to record the demo booking; there is no need to click a button.
  Card controls still work for explicitly requested drafts. Occupied slots are
  excluded, conflicts refresh the choices, and cancellation releases the time.
- Prescription drafts require medication, dose, route, frequency, and duration.
  Fill missing details directly on the card, then **Submit prescription**.
  The confirmed card shows **Prescription · Submitted**, and the after-visit
  summary also calls it a prescription. Submission records the local action;
  it does not check safety, send or dispense medication.
- X-ray requests require the body site/side and reason. A confirmed request is
  not an examination or an imaging result.
- Blood-test requests require the clinician's test list and reason. Confirming
  a lab request does not record specimen collection or produce results. Asking
  for existing blood results is a separate read-only FHIR lookup with sources.

Confirming any action briefly highlights its card and draws a checkmark. The
confirmation badge remains visible afterward; reopening saved cards does not
replay the effect. Reduced-motion preferences keep the static confirmation.

All action records are simulated, saved per patient in this browser, and survive
reloads and new conversations. Existing records from the former tab are restored
as chat cards. **Reset Demo** clears actions and suggestions. No booking service,
pharmacy, laboratory, imaging service, or FHIR write endpoint is contacted.

Short prompts for a fictional workflow demo:

| Prompt | What appears in chat |
| --- | --- |
| We should arrange the next appointment. | Three available times offered immediately. |
| Book the second one. | Confirmed appointment and reference, in chat. |
| Draft an X-ray request for the right knee, reason: right-knee pain. | An editable request for review. |
| Prepare a blood-test request. | A draft with fields for the clinician's tests and reason. |
| Generate an after-visit summary. | The handout, built from chat and confirmed actions. |

These are demo workflow examples, not patient-specific treatment recommendations.

The `demo-visit-actions` skill supplies validated action and suggestion metadata.
The `generate-after-visit-summary` skill rephrases documented clinician plans
into clear patient instructions. It returns an exact source quote for evidence
checks alongside the patient wording; only the paraphrase appears in the handout.
The app inserts exact confirmed action details, including lab requests. The popup,
copy, and print views use patient-copy wording without draft-review boilerplate. Questions, suggestions, and missing information
do not become completed care or invented results. Extra clinician notes remain
optional. Generate the handout in chat, then select **Open summary** in the reply
to view a centered popup over a blurred background. Edit wording, copy, or print
there; changing the source plan requires regeneration before export. Close the
popup with Escape, its close button, or the backdrop to return to chat. Nothing
is delivered to a patient automatically.

New sandbox builds include both skills. For an existing sandbox:

```bash
openshell sandbox upload hermes-health ./agents/ehr-expert/skills/demo-visit-actions/SKILL.md /sandbox/.hermes/skills/demo-visit-actions
openshell sandbox upload hermes-health ./agents/ehr-expert/skills/generate-after-visit-summary/SKILL.md /sandbox/.hermes/skills/generate-after-visit-summary
```

Validation (mocked browser checks; no external writes):

```bash
node scripts/test_dashboard_visit_actions.mjs
node scripts/test_dashboard_avs.mjs
```

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

Fetches [openFDA](https://open.fda.gov/) labeling with the packaged
`drug-label-interactions/scripts/lookup_labels.py` command. It accepts up to
three drug names or known RxCUIs, verifies concepts with RxNav, and returns one
label per medicine with focused excerpts, dates, and direct label URLs.

```bash
python3 /sandbox/.hermes/skills/drug-label-interactions/scripts/lookup_labels.py --drug ibuprofen --drug naproxen --drug clopidogrel --term bleeding --term NSAID
```

The helper performs read-only requests through curl and parses responses in
Python. It tries an exact RxCUI query, then a verified single-ingredient fallback
only when the source reports no matches. Ingredient matches are explicitly
separate from the patient's exact product; ambiguous names and mismatched
combination labels are rejected. Requests have a 60-second batch budget and at
most one retry per transient failure. Permission errors stop the lookup.

The agent runs this packaged command instead of inventing curl/Python pipelines
or temporary parsers. Missing evidence is reported as unverified, and source
excerpts may be incomplete. This is individual label retrieval, not a complete
pairwise interaction checker or a patient-specific treatment decision.
Run offline regressions with `python3 scripts/test_drug_label_lookup.py`.

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

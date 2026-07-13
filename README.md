# EHRClaw


## Components

| Component | Purpose |
|---|---|
| NemoClaw / OpenShell | Secure agent execution, tool isolation, network policy, and credential injection at the egress boundary |
| Hermes | Agent runtime and skill orchestration |
| NeMo Relay | Traces the agent's LLM/tool workflow (ATOF/ATIF exports) and manages adaptive execution, with NemoGuardrails plugin |
| NeMo Guardrails | Blocks medical advice / drug recommendation requests |
| HAPI FHIR | Stores and serves the patient / clinical records (FHIR R4) |

## FHIR Server

This project runs a local **FHIR R4** server using [HAPI FHIR](https://hapifhir.io/) with PostgreSQL. Sample patient records live in `data/` and come from [Synthea](https://synthea.mitre.org/downloads).

## Requirements

- **Python 3.13+**
- **[uv](https://docs.astral.sh/uv/)** for dependency management
- **Docker** (for the FHIR server)

Install the Python dependencies and activate the virtual environment:

```bash
uv sync
source .venv/bin/activate
```

`uv sync` creates a `.venv/` and installs the pinned dependencies from `pyproject.toml` / `uv.lock`.

### 1. Download Sample Data
```bash
mkdir -p data && curl -L -o data/synthea_sample_data_fhir_latest.zip "https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_fhir_latest.zip" && unzip -q data/synthea_sample_data_fhir_latest.zip -d data
```

### 2. Start the server

```bash
docker compose up -d
```

The server listens on **http://localhost:8081**

| URL | Description |
|---|---|
| http://localhost:8081/ | HAPI FHIR web UI (search and browse resources) |
| http://localhost:8081/fhir | FHIR R4 REST API base URL |
| http://localhost:8081/fhir/metadata | CapabilityStatement |

Wait until the server is ready:

```bash
curl http://localhost:8081/fhir/metadata
```

### 3. Load Synthea data

With the server running, upload the bundles from `data/`:

```bash
python3 scripts/load_synthea.py --data-dir data
```

This posts each Synthea transaction bundle to the FHIR API. The first full load takes a few minutes.

## Agent Setup

The Hermes agent runs in an OpenShell sandbox managed by the **NemoClaw** CLI.

### Prerequisite: install NemoClaw

- Install the `nemoclaw` version 0.0.70 CLI first and make sure it's on your `PATH`:
```bash
nemoclaw --version # 
```

### Quick install

From the repo root:

```bash
./setup.sh
```

This builds the vendored Hermes image (with [`nemo-relay`](https://github.com/NVIDIA/NeMo-Relay) baked in) and onboards a fresh `hermes-health` sandbox. Onboarding is interactive and will prompt for your inference provider/credentials.

Once it finishes, connect and start chatting:

```bash
nemoclaw hermes-health connect
sandbox$ hermes chat
```

## NeMo Guardrails

[NeMo Guardrails](https://github.com/NVIDIA/NeMo-Guardrails) keeps the assistant on-task — it blocks the agent from giving medical advice or drug recommendations, so it stays a data-retrieval tool rather than a clinical decision-maker.

It's integrated through **NeMo Relay** ([Nemo Guardrails Plugin](https://docs.nvidia.com/nemo/relay/configure-plugins/nemo-guardrails/about)), and runs in **local** mode, so NeMo Relay spawns an in-sandbox Python worker that applies self-check input/output rails (defined in `configs/nemo_guardrails/config.yaml`) to every managed LLM turn. The self-check calls the same OpenShell-managed inference route as the agent, so no API key ever lives in the sandbox.

The local backend requires `nemoguardrails==0.22.0`, which depends on `annoy` — a package that ships source-only on PyPI (no prebuilt wheel for Python 3.13). Since the hardened sandbox image has no C/C++ compiler, we vendor a prebuilt `annoy` wheel under `vendor/wheels/` and install from it at build time so nothing needs to compile inside the image.

## Skills

Agent skills live under `agents/ehr-expert/skills/`. Each skill documents how the agent queries an external data source.

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

## Disclaimers
- *This demo is not intended to be used in a clinical setting or for actual patient care. It is a technical demonstration only and does not provide medical advice, diagnosis, or treatment. All patient records are synthetic and do not represent real individuals. This product uses publicly available data from the U.S. National Library of Medicine (NLM), National Institutes of Health, Department of Health and Human Services; NLM is not responsible for the product and does not endorse or recommend this or any other product.*

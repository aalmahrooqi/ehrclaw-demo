# EHRClaw

## FHIR Server

This project runs a local **FHIR R4** server using [HAPI FHIR](https://hapifhir.io/) with PostgreSQL. Sample patient records live in `data/` and come from [Synthea](https://synthea.mitre.org/downloads).

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

Runs a [NeMo Guardrails](https://github.com/NVIDIA/NeMo-Guardrails) server on the host to moderate the agent (e.g. block medical advice). The rails config lives in `configs/nemo_guardrails/`.

1. Set your API key (copy `.env.example` to `.env` and fill it in):

```bash
cp .env.example .env   # then edit NVIDIA_API_KEY
```

2. Start the server:

```bash
./scripts/setup_nemoguardrails.sh
```

This loads `.env` and launches `nemoguardrails server --config ./configs/nemo_guardrails` (listens on **http://localhost:8000**).

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

## NLM Attribution Notice
*This product uses publicly available data from the U.S. National Library of Medicine (NLM), National Institutes of Health, Department of Health and Human Services; NLM is not responsible for the product and does not endorse or recommend this or any other product.*

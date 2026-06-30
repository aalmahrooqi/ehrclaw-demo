# Health Demo

## FHIR Server

This project runs a local **FHIR R4** server using [HAPI FHIR](https://hapifhir.io/) with PostgreSQL. Sample patient records live in `data/` and come from [Synthea](https://synthea.mitre.org/downloads).

### Download Sample Data
```bash
mkdir -p data && curl -L -o data/synthea_sample_data_fhir_latest.zip "https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_fhir_latest.zip" && unzip -q data/synthea_sample_data_fhir_latest.zip -d data
```

### Start the server

```bash
docker compose up -d
```

The server listens on **http://localhost:8080**

| URL | Description |
|---|---|
| http://localhost:8080/ | HAPI FHIR web UI (search and browse resources) |
| http://localhost:8080/fhir | FHIR R4 REST API base URL |
| http://localhost:8080/fhir/metadata | CapabilityStatement |

Wait until the server is ready:

```bash
curl http://localhost:8080/fhir/metadata
```

### Load Synthea data

With the server running, upload the bundles from `data/`:

```bash
python3 scripts/load_synthea.py --data-dir data
```

This posts each Synthea transaction bundle to the FHIR API. The first full load takes a few minutes.
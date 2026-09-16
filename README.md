# Site to GCS & Gemini Enterprise Datastore Ingestor

A unified pipeline and control plane that crawls public websites, transforms them into AI-optimized Markdown (with metadata frontmatter and table support), uploads all assets to Google Cloud Storage (GCS), creates or updates a Vertex AI Search / Discovery Engine Data Store, enforces public URL citations (instead of `gs://` paths), and triggers periodic auto-sync via Cloud Scheduler.

Supports both **single-site** and **multi-site targets** with custom depths, page budgets, and path inclusion/exclusion filtering.

---

## Guaranteed Public Web Citations (Not Cloud Storage `gs://`)

When importing unstructured data into Vertex AI Search / Gemini Enterprise, citations and search results default to the Cloud Storage URI (`gs://...`) unless the schema explicitly re-maps the canonical link property.

This system enforces the **Linked Unstructured Documents** specification:
1. **`content.uri`**: Points to the clean Markdown document in GCS (`gs://<bucket>/<prefix>/documents/<id>.md`) for embedding, chunking, and LLM grounding.
2. **`structData.url`**: Stores the canonical live web URL (`https://...`).
3. **`keyPropertyMapping: {"uri": "url"}`**: Configured automatically on the Discovery Engine schema. This tells Vertex AI Search and Gemini Enterprise that the public web URL is the primary document URI, guaranteeing that assistant citations and search cards navigate to the live website.
4. **Markdown Frontmatter & Links**: Every document contains a YAML frontmatter block and a top-level `**Source URL:** <url>` link for synthesis.

---

## Project Structure

- `app.py` — Interactive Web GUI & Control Plane with live multi-site crawler manager, log streaming, Markdown document inspector, and exportable CI/CD / deployment manifests.
- `main.py` — Production-ready Cloud Run Function (2nd Gen) handler (`@functions_framework.http`) with ephemeral `/tmp` staging, GCS upload, and automated Discovery Engine Data Store creation, schema configuration, and document import.
- `web_to_gcs_ai_store.py` — Standalone terminal CLI script supporting single-site and multi-target pipeline execution.
- `cloudbuild.yaml` — Declarative CI/CD pipeline deploying the Cloud Run Function and configuring Cloud Scheduler automatically.
- `targets.json` — Declarative multi-site manifest with per-target depth, page limits, and exclusion rules.
- `requirements.txt` — Dependencies (`functions-framework`, `google-cloud-discoveryengine`, `google-cloud-storage`, `beautifulsoup4`, `requests`, `lxml`).

---

## Managing Scanned Websites & Depth (`targets.json`)

To manage multiple documentation portals, subdomains, and API references without touching deployment code, use a `targets.json` manifest:

```json
{
  "version": "1.0",
  "default_delay": 0.2,
  "targets": [
    {
      "name": "developer-guide",
      "url": "https://docs.example.com/guide/",
      "max_depth": 3,
      "max_pages": 150,
      "include_patterns": ["*/guide/*"],
      "exclude_patterns": ["*/archive/*", "*/deprecated/*"]
    },
    {
      "name": "api-reference",
      "url": "https://api.example.com/v2/",
      "max_depth": 2,
      "max_pages": 50,
      "exclude_patterns": ["*/changelog/*"]
    }
  ]
}
```

### Decoupled GCS Config Pattern
Upload `targets.json` to Cloud Storage:
```bash
gcloud storage cp targets.json gs://my-ai-knowledge-bucket/website_datastore/targets.json
```
Set the Cloud Run Function environment variable `TARGETS_CONFIG_URI="gs://my-ai-knowledge-bucket/website_datastore/targets.json"`. The scheduled sync automatically reads the latest targets and depths on each run—**no code re-deployments needed**.

---

## 0. Local Setup: Virtual Environment & Authentication

To avoid conflicts with system Python packages (PEP 668 `externally-managed-environment`), always create and activate a local virtual environment (`.venv`) before installing dependencies or running the ingestor locally:

```bash
# 1. Create a virtual environment in the project root
python3 -m venv .venv

# 2. Activate the virtual environment
source .venv/bin/activate

# 3. Install required dependencies inside the venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

# 4. Authenticate Google Cloud Application Default Credentials (ADC)
#    (Required for Local + GCP Sync and Cloud Run Function execution modes)
gcloud auth application-default login
gcloud config set project YOUR_GCP_PROJECT_ID
```

> **Tip:** Once `.venv` is created, you can run any command explicitly using `.venv/bin/python app.py` or while the virtual environment is activated (`source .venv/bin/activate`).

---

## 1. Web Management UI (Local Control Plane)

Launch the interactive control plane from your virtual environment:

```bash
.venv/bin/python app.py
```

Open [http://localhost:8085](http://localhost:8085) in your browser to:
- Choose between **3 Execution Modes**:
  1. **Local + GCP Sync**: Crawl locally on your machine, upload Markdown + `metadata.jsonl` to GCS, and update Vertex AI Search Data Store.
  2. **Local Dry-Run**: Crawl and preview extracted Markdown + `metadata.jsonl` locally on disk without touching GCP.
  3. **Cloud Run Function**: Trigger your deployed serverless Cloud Run Function remotely via OIDC and stream results live.
- Toggle between **Single URL** and **Multi-Site Manager**.
- Add target websites with individual depth sliders, max pages, and exclude pattern filters.
- Export or load `targets.json`.
- Inspect extracted Markdown documents and verify citation metadata.
- View real-time logs and export `cloudbuild.yaml` CI/CD configurations.

---

## 2. Automated CI/CD Pipeline (Cloud Build)

Deploy the Cloud Run Function and configure Cloud Scheduler in a single automated build:

```bash
gcloud builds submit --config=cloudbuild.yaml \
  --substitutions=_GCP_PROJECT="my-gcp-project",_GCS_BUCKET="my-ai-knowledge-bucket",_DATA_STORE_ID="web-docs-store"
```

---

## 3. Manual Deployment (gcloud CLI)

Deploy `main.py` directly to Cloud Run Functions:

```bash
gcloud functions deploy site-datastore-ingestor \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=. \
  --entry-point=index_website_handler \
  --memory=2Gi \
  --timeout=1800s \
  --set-env-vars=GCP_PROJECT="my-gcp-project",LOCATION="global",GCS_BUCKET="my-ai-knowledge-bucket",GCS_PREFIX="website_datastore",DATA_STORE_ID="web-docs-store",ENGINE_ID="gemini-assistant-app",TARGETS_CONFIG_URI="gs://my-ai-knowledge-bucket/website_datastore/targets.json"
```

---

## 4. Configuring Periodic Auto-Updates (Cloud Scheduler)

Set up a Cloud Scheduler cron job (e.g., daily at 2:00 AM UTC) to trigger the Cloud Run Function. The function executes with `ReconciliationMode.FULL`, ensuring deleted web pages are pruned from the Data Store and modified pages are updated:

```bash
gcloud scheduler jobs create http site-ingestor-nightly-sync \
  --schedule="0 2 * * *" \
  --uri="https://us-central1-my-gcp-project.cloudfunctions.net/site-datastore-ingestor" \
  --http-method=POST \
  --headers="Content-Type=application/json" \
  --message-body='{}' \
  --oidc-service-account-email="cloud-scheduler-sa@my-gcp-project.iam.gserviceaccount.com"
```

---

## 5. Standalone Terminal CLI

Run the full pipeline directly from the command line using your virtual environment:

```bash
# Multi-target run
.venv/bin/python web_to_gcs_ai_store.py \
  --targets-file targets.json \
  --gcs-bucket "my-ai-knowledge-bucket" \
  --project-id "my-gcp-project" \
  --data-store-id "web-docs-store"

# Single-target run
.venv/bin/python web_to_gcs_ai_store.py \
  --url "https://docs.example.com" \
  --gcs-bucket "my-ai-knowledge-bucket" \
  --project-id "my-gcp-project" \
  --data-store-id "web-docs-store" \
  --max-pages 50 --max-depth 3
```

Add `--dry-run` to crawl and generate local Markdown + `metadata.jsonl` files without modifying cloud resources.

---

## 6. Triggering via HTTP / cURL

```bash
# Using GCS-hosted targets.json
curl -X POST "https://us-central1-my-gcp-project.cloudfunctions.net/site-datastore-ingestor" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{}'

# Or passing dynamic targets directly in the payload
curl -X POST "https://us-central1-my-gcp-project.cloudfunctions.net/site-datastore-ingestor" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{
    "targets": [
      {"name": "docs", "url": "https://docs.example.com", "max_depth": 3, "max_pages": 100}
    ],
    "gcs_bucket": "my-ai-knowledge-bucket",
    "project_id": "my-gcp-project",
    "data_store_id": "web-docs-store"
  }'
```

---

## 7. Gemini Enterprise Assistant Grounding

When connecting the Data Store in the Gemini Enterprise Agent / Assistant builder:
1. In **Data Stores**, attach `web-docs-store`.
2. In **System Instructions**, add:
   > *"When answering user queries based on the website knowledge base, always reference the public live URL provided in the document metadata (`url` property). Never cite or display internal Cloud Storage (`gs://`) paths."*

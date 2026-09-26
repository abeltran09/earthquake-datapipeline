# earthquake-datapipeline

An incremental ELT pipeline that pulls earthquake events from the
[USGS FDSNWS Event API](https://earthquake.usgs.gov/fdsnws/event/1/), lands them in Google Cloud Storage as raw
NDJSON, and loads them into BigQuery — orchestrated by Airflow, running in Docker. It handles both new events
and USGS's late revisions to existing events (magnitude/location/status corrections made days or weeks after
an earthquake), without needing a full historical rescan every run.

## Prerequisites

- Docker and Docker Compose
- A GCP project with BigQuery and Cloud Storage enabled
- A GCS bucket to land raw data in (create it yourself — this pipeline doesn't create buckets, only the
  BigQuery dataset/table)
- A GCP service account with:
  - `roles/storage.objectAdmin` on that bucket (or narrower, scoped to the bucket)
  - BigQuery permissions to create/query datasets and tables in your target project (e.g.
    `roles/bigquery.dataEditor` + `roles/bigquery.jobUser`)
- That service account's JSON key, downloaded locally

## Getting started

1. **Clone the repo and copy the env template:**
   ```
   cp .env.example .env
   ```

2. **Fill in `.env`:**

   | Variable | Required | Notes |
   |---|---|---|
   | `GCP_PROJECT_ID` | yes | Your GCP project ID. |
   | `GCP_DATASET_ID` | yes | BigQuery dataset name (e.g. `earthquakes`). Created automatically on first DAG run — no manual `bq mk` needed. |
   | `GCP_CREDENTIALS_PATH` | yes | **Absolute** path to your service account key on the host machine (e.g. `keys/gcp-credentials.json`). A typo here fails silently: Docker creates an empty directory instead of erroring, which later surfaces as a confusing `IsADirectoryError` in a task log. |
   | `AIRFLOW_FERNET_KEY` | yes | Generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`. Encrypts connection/variable values in Airflow's metadata DB. |
   | `AIRFLOW_WEBSERVER_SECRET_KEY` | yes | Generate with `python -c "import secrets; print(secrets.token_hex(30))"`. Must be identical across every Airflow component (shared automatically via `docker-compose.yml`) — if unset, each component generates its own random key and task log viewing fails with a 403. |

3. **Place your service account key** at the path you set for `GCP_CREDENTIALS_PATH` (e.g. `keys/gcp-credentials.json` — this directory is gitignored, never commit the key).

4. **Start the stack:**
   ```
   docker compose up -d
   ```

5. **Set the Airflow Variables the DAGs read but don't manage themselves:**
   ```
   docker compose exec airflow-webserver airflow variables set gcs_bucket <your-bucket-name>
   docker compose exec airflow-webserver airflow variables set usgs_min_magnitude 2.5   # optional, this is the default
   ```
   Do **not** manually set `usgs_revision_watermark` — the daily DAG creates and advances that one on its own
   (see below).

6. **Open the Airflow UI** at `http://localhost:8080`, log in with `admin` / `admin` (set in
   `docker-compose.yml`'s `airflow-init` step — fine for local dev, don't reuse anywhere reachable beyond
   localhost), and unpause the DAGs.

## Pipeline overview

```
USGS Earthquake API
       │
       │  ingestion/usgs_to_gcs.py  (CLI, called by Airflow via BashOperator)
       ▼
GCS  usgs-data/year=/month=/day=/usgs_earthquakes_*.ndjson     (raw landing zone, append-only)
       │
       │  GCSToBigQueryOperator (Airflow)
       ▼
BigQuery  earthquakes.raw_usgs_earthquakes   (properties/geometry kept as JSON columns —
       │                                      schema never breaks when USGS adds a field)
       ▼
dbt (dbt/earthquakes/)  — not built yet; flatten + dedupe by event id happens here eventually
```

Three Airflow DAGs drive the GCS → BigQuery half:

| DAG | Schedule | What it does |
|---|---|---|
| `usgs_earthquakes_to_gcs` | `@daily` | Ingests new events for the day, plus an **incremental revision watermark** (catches USGS's late revisions since the last successful run — see below), then loads both into the raw BigQuery table. |
| `usgs_earthquakes_reconciliation` | `@weekly` | Wide 30-day `updated_after` sweep, independent of the daily watermark — a safety net for anything the incremental cursor might have missed (an outage, a backdated revision, etc.). |
| (shared) `usgs_common.py` | — | Not a DAG; constants (paths, BigQuery schema, project/dataset) shared by the two DAGs above so they can't drift out of sync. |

### Why a watermark instead of a fixed lookback window

USGS revises event records (magnitude, location, status) for days or weeks after an event occurs, so a naive
daily job that only asks "what's new today?" would silently miss those corrections. The fix isn't to re-query
a fixed N-day window from scratch every day (that reprocesses unchanged events repeatedly and bloats the raw
table with duplicates) — instead, `ingest_revisions` asks USGS for everything updated since an
**Airflow Variable** (`usgs_revision_watermark`) that the DAG advances itself after each successful run. A
failed run simply leaves the watermark stale, so the next successful run naturally re-covers the gap. See
[usgs_to_gcs_dag.py](airflow/dags/usgs_to_gcs_dag.py) for the mechanics.

Some duplication across raw-table loads is expected and accepted by design (append-only raw/bronze layer);
downstream deduplication (by event `id`, keeping the row with the latest `properties.updated`) is dbt's job,
not yet built.

## Repo layout

| Path | Purpose |
|---|---|
| `ingestion/usgs_to_gcs.py` | CLI tool: USGS → NDJSON → GCS. See [ingestion/README.md](ingestion/README.md) for flags and behavior. |
| `airflow/dags/` | The three pieces described above. |
| `dbt/earthquakes/` | dbt project scaffold — staging/mart models (flatten + dedupe) not built yet. |
| `docker-compose.yml` | Airflow (webserver, scheduler, Postgres metadata DB) — the only way this pipeline runs; there's no non-Docker path. |
| `keys/gcp-credentials.json` | GCP service account key (gitignored). |
| `.env` / `.env.example` | Local config — see Getting started above. |

## Operational notes

- **Timezone**: everything (schedule, `{{ ds }}`, the watermark) runs in UTC. Don't fight this — USGS's own API
  params are UTC-native, and Airflow's scheduler computes intervals in UTC regardless of DAG-level timezone
  settings. For **manual** triggers, always pass an explicit date (`airflow dags trigger ... -e
  <date>T00:00:00+00:00`, or the positional date on `airflow dags test`) rather than relying on "now" — a
  late-evening trigger in a US timezone can already be "tomorrow" in UTC.
- **Rebuilding containers**: after editing `.env`/`docker-compose.yml`, run `docker compose up -d` again (add
  `--force-recreate` if a change doesn't seem to take). Never `docker compose down -v` unless you intend to
  wipe Airflow's metadata DB (DAG history, Variables, the admin user) — plain `docker compose down` preserves
  it.

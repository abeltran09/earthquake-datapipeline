# USGS → GCS Ingestion

Pulls earthquake events from the [USGS FDSNWS Event API](https://earthquake.usgs.gov/fdsnws/event/1/) for a
date window, converts them to newline-delimited JSON (NDJSON), and lands them in GCS as a raw/bronze layer,
partitioned by date. Built to be called directly from the CLI today and imported into an Airflow DAG later —
`run()` is the single entry point both paths use.

## How it works

1. `_fetch_count` — asks USGS how many events match the window (`starttime`/`endtime`/`minmagnitude`) before
   pulling any data.
2. `collect_ndjson` — refuses to proceed if that count exceeds USGS's hard per-query cap of 20,000 results
   (see [Limitations](#limitations)), otherwise fetches the full GeoJSON `FeatureCollection` via `_fetch_data`,
   retrying on `429`/`5xx` with exponential backoff.
3. `geojson_collection_to_ndjson` — flattens the collection into one JSON object per line, tagging each record
   with `_ingested_at`, `source_url`, and `_collection_generated` so downstream (dbt/BigQuery) can tell when and
   from where a record was pulled.
4. `gcs_object_path` — builds a Hive-style partitioned path from the partition date:
   ```
   usgs-data/year=2026/month=09/day=20/usgs_earthquakes_20260920.ndjson
   ```
5. `upload_to_gcs` — uploads the NDJSON to that path, unless an object is already there (see
   [Idempotency](#idempotency)).

## Setup

Install dependencies from the repo root:

```
pip install -r requirements.txt
```

### Credentials

The script authenticates to GCS via a service-account key and reads config from a `.env` file at the repo
root (loaded automatically via `python-dotenv`):

```
GCP_CREDENTIALS_PATH=C:\path\to\gcp-credentials.json   # absolute path — required
```

- Locally, `GCP_CREDENTIALS_PATH` must be an **absolute** path. A relative path resolves against your current
  working directory, not the repo root, and will fail to find the file if you run the script from anywhere else.
- Inside Docker/Airflow, the script never reads `GCP_CREDENTIALS_PATH` directly — `docker-compose.yml` bind-mounts
  the same key into the container and sets `GOOGLE_APPLICATION_CREDENTIALS` there instead, which the GCS client
  picks up automatically (Application Default Credentials). `GCP_CREDENTIALS_PATH` in `.env` is still needed in
  that case, but only so docker-compose knows which host file to mount.
- Never commit the key or `.env` — both are gitignored. Scope the service account's IAM role to just the bucket
  it needs (e.g. `roles/storage.objectAdmin` on that bucket), not a project-wide role.

## Usage

```
python ingestion/usgs_to_gcs.py --bucket my-bucket --start 2026-09-19 --end 2026-09-20
```

| Flag | Required | Description |
|---|---|---|
| `--bucket` | yes | destination GCS bucket |
| `--start` | yes | window start date, `YYYY-MM-DD` |
| `--end` | yes | window end date, `YYYY-MM-DD` (also the default partition date) |
| `--min_magnitude` | no | minimum event magnitude, default `2.5` |
| `--partition_date` | no | overrides the partition date, for backfills where `--start`/`--end` span more than one day |
| `--overwrite` | no | re-upload even if an object already exists at the target path |

For daily ingestion, keep `--start`/`--end` to a single day so the query window and the partition date line up.

## Idempotency

The object path is derived from the partition date only (`usgs_earthquakes_YYYYMMDD.ndjson`) — not a run
timestamp — so re-running the same day always targets the same object:

- **Object already exists, no `--overwrite`**: the run fetches from USGS, then skips the upload and returns the
  existing path untouched. Safe default for Airflow retries — a retried task can't create duplicate objects.
- **Object already exists, `--overwrite` passed**: the file is replaced. Use this to pull in USGS's later
  revisions to past events (magnitudes/locations are often revised for weeks after an event).
- **Bucket doesn't exist or isn't accessible**: raises `RuntimeError` before attempting any upload, rather than
  surfacing a raw GCS API error.

## Limitations

- **20,000-result cap**: USGS rejects any single query over 20,000 results. `collect_ndjson` currently raises
  rather than silently truncating — there's no automatic window-bisection yet. In practice this only matters for
  very wide date ranges or a very low `--min_magnitude`; single-day windows at the default magnitude are well
  under the cap.
- **Late revisions**: because partitions are keyed by event-time window, a magnitude/location revision USGS
  makes weeks after an event won't appear in that day's file unless the day is re-ingested with `--overwrite`.
  Handling this automatically (e.g. a trailing re-ingest window) isn't built yet.

## Airflow integration (planned)

`run(bucket, start, end, min_magnitude, partition_date=None, overwrite=False)` is the intended call point for a
`PythonOperator`/`@task`, importable as `from ingestion.usgs_to_gcs import run`. `--start`/`--end` map naturally
to a DAG's `data_interval_start`/`data_interval_end` for a daily schedule.

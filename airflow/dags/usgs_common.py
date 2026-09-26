from __future__ import annotations

import os

SCRIPT_PATH = "/opt/airflow/ingestion/usgs_to_gcs.py"

# Static per-deployment config, read once at DAG-parse time -- already set as
# container env vars for this exact purpose (see docker-compose.yml).
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_DATASET_ID = os.environ.get("GCP_DATASET_ID")
RAW_TABLE = "raw_usgs_earthquakes"

# Mirrors gcs_object_path()/PATH_PREFIX in ingestion/usgs_to_gcs.py exactly --
# the object a load task reads must match what the matching ingest task wrote.
GCS_PARTITION_PREFIX = "usgs-data/year={{ ds[:4] }}/month={{ ds[5:7] }}/day={{ ds[8:10] }}"

# properties/geometry stay JSON (not autodetected STRUCT) so a new USGS field
# never breaks the load -- it just rides along unflattened in the blob until
# a later dbt model flattens it.
RAW_SCHEMA_FIELDS = [
    {"name": "id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "properties", "type": "JSON", "mode": "NULLABLE"},
    {"name": "geometry", "type": "JSON", "mode": "NULLABLE"},
    {"name": "_ingested_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "source_url", "type": "STRING", "mode": "NULLABLE"},
    {"name": "_collection_generated", "type": "STRING", "mode": "NULLABLE"},
]

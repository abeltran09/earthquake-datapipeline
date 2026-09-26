from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from usgs_common import (
    GCP_DATASET_ID,
    GCP_PROJECT_ID,
    GCS_PARTITION_PREFIX,
    RAW_SCHEMA_FIELDS,
    RAW_TABLE,
    SCRIPT_PATH,
)

default_args = {
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="usgs_earthquakes_to_gcs",
    description="Daily USGS earthquake ingestion into GCS/BigQuery: new events only. "
    "Revisions are handled solely by the weekly usgs_earthquakes_reconciliation DAG.",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "usgs", "gcs"],
) as dag:

    # New events that occurred during this run's day. Partitioned by --end (= ds),
    # matching how gcs_object_path/compute_partition_date default the partition date.
    ingest_new_events = BashOperator(
        task_id="ingest_new_events",
        bash_command=(
            "python3 " + SCRIPT_PATH +
            " --bucket {{ var.value.gcs_bucket }}"
            " --start {{ macros.ds_add(ds, -1) }}"
            " --end {{ ds }}"
            " --min_magnitude {{ var.value.get('usgs_min_magnitude', 2.5) }}"
        ),
    )

    # BigQuery load jobs auto-create the destination table (CREATE_IF_NEEDED,
    # below) but never the dataset, so this is the one infra-as-code step needed.
    ensure_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="ensure_bq_dataset",
        project_id=GCP_PROJECT_ID,
        dataset_id=GCP_DATASET_ID,
        location="US",
        exists_ok=True,
    )

    # Appends that day's new-events object into the raw table. A retry re-appends
    # the same rows (no dedup at this layer) -- acceptable for an append-only raw
    # landing zone; real dedup is a later dbt concern.
    load_new_events_to_bq = GCSToBigQueryOperator(
        task_id="load_new_events_to_bq",
        bucket="{{ var.value.gcs_bucket }}",
        source_objects=[GCS_PARTITION_PREFIX + "/usgs_earthquakes_{{ ds_nodash }}.ndjson"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{RAW_TABLE}",
        source_format="NEWLINE_DELIMITED_JSON",
        schema_fields=RAW_SCHEMA_FIELDS,
        time_partitioning={"type": "DAY", "field": "_ingested_at"},
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        max_bad_records=0,
    )

    ensure_bq_dataset >> load_new_events_to_bq
    ingest_new_events >> load_new_events_to_bq

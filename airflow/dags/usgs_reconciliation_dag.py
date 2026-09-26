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

# Independent of usgs_revision_watermark on purpose: a wide, infrequent audit
# pass that heals anything the daily incremental cursor might have drifted on
# (backdated updates, a run that succeeded but wrote incomplete data, etc.),
# not another cursor to advance. Never writes to usgs_revision_watermark.
RECONCILIATION_LOOKBACK_DAYS = 30

default_args = {
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="usgs_earthquakes_reconciliation",
    description="Weekly wide-lookback safety net for USGS revisions the daily watermark may have missed",
    schedule="@weekly",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "usgs", "gcs", "reconciliation"],
) as dag:

    # Own copy, idempotent (exists_ok=True) -- keeps this DAG runnable standalone
    # even if the daily DAG is ever disabled.
    ensure_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="ensure_bq_dataset",
        project_id=GCP_PROJECT_ID,
        dataset_id=GCP_DATASET_ID,
        location="US",
        exists_ok=True,
    )

    reconcile_revisions = BashOperator(
        task_id="reconcile_revisions",
        bash_command=(
            "python3 " + SCRIPT_PATH +
            " --bucket {{ var.value.gcs_bucket }}"
            " --updated_after {{ macros.ds_add(ds, -%d) }}" % RECONCILIATION_LOOKBACK_DAYS +
            " --partition_date {{ ds }}"
            " --min_magnitude {{ var.value.get('usgs_min_magnitude', 2.5) }}"
            " --tag reconciliation"
        ),
    )

    load_reconciliation_to_bq = GCSToBigQueryOperator(
        task_id="load_reconciliation_to_bq",
        bucket="{{ var.value.gcs_bucket }}",
        source_objects=[GCS_PARTITION_PREFIX + "/usgs_earthquakes_reconciliation_{{ ds_nodash }}.ndjson"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{RAW_TABLE}",
        source_format="NEWLINE_DELIMITED_JSON",
        schema_fields=RAW_SCHEMA_FIELDS,
        time_partitioning={"type": "DAY", "field": "_ingested_at"},
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        max_bad_records=0,
    )

    ensure_bq_dataset >> reconcile_revisions >> load_reconciliation_to_bq

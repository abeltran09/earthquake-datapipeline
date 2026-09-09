from __future__ import annotations

import requests
from datetime import datetime, date
import json
import datetime as dt
import time
import argparse
import sys

API_BASE = "https://earthquake.usgs.gov/fdsnws/event/1"
USER_AGENT = "usgs-earthquake-pipeline/1.0 (data-engineering)"
MAX_PER_QUERY = 20000
PATH_PREFIX = 'usgs-data'

def _get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"user-agent":USER_AGENT})
    return session

def _base_params(starttime: date, endtime: date, mag: float | None):
    params = {"format":"geojson", "starttime": starttime, "endtime": endtime, "minmagnitude":mag}
    return params

def _fetch_count(session: requests.Session,start: date, end: date, min_mag: float):
    url = f"{API_BASE}/count"
    response = session.get(url, params=_base_params(start, end, min_mag), timeout=60)
    response.raise_for_status()
    return response.json().get("count")

def _fetch_data(session: requests.Session, start: date, end: date, min_mag: float):
    url = f"{API_BASE}/query"
    for attempt in range(3):
        response = session.get(url, params=_base_params(start, end, min_mag), timeout=120)
        if response.status_code == 200:
            return response.json()
        if response.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 ** attempt)
            continue
        response.raise_for_status()
    response.raise_for_status()
    raise RuntimeError("unreachable")


def geojson_collection_to_ndjson(fc: dict, ingested_at: str, source_url: str, add_metadata: bool = True):
    generated = fc.get('metadata', {}).get('generated', '')
    lines = []
    for feature in fc.get('features', []):
        if add_metadata:
            record = {
                **feature,
                "_ingested_at": ingested_at,
                "source_url": source_url,
                "_collection_generated": generated
            }
        else:
            record = feature
        lines.append(json.dumps(record, separators=(",",":"), ensure_ascii=False))
    return "\n".join(lines) + ("\n" if lines else "")


def collect_ndjson(session: requests.Session, start: date, end: date, min_mag: float, ingested_at: str):
    count = _fetch_count(session, start, end, min_mag)
    print(count)
    if count > MAX_PER_QUERY:
        pass
    fc = _fetch_data(session, start, end, min_mag)
    return geojson_collection_to_ndjson(fc, ingested_at, fc.get('metadata', {}).get('url', ''))


def compute_partition_date(end: date, override: date | None = None) -> date:
    """Partition is derived from the query window unless explicitly overridden (backfills)."""
    return override or end


def compute_run_ts(logical_dt: dt.datetime | None = None) -> str:
    """
    Deterministic identifier for this run's output file.
    Pass the DAG's logical_date/data_interval here so retries and backfills
    are idempotent (same run -> same object path -> overwrite, not duplicate).
    Falls back to wall-clock UTC now() for ad-hoc/manual script runs.
    """
    ts = logical_dt or dt.datetime.now(dt.timezone.utc)
    return ts.strftime("%Y%m%dT%H%M%SZ")


def gcs_object_path(partition_date: date, run_ts: str, prefix: str = PATH_PREFIX) -> str:
    return (
        f"{prefix}/"
        f"year={partition_date:%Y}/month={partition_date:%m}/day={partition_date:%d}/"
        f"usgs_earthquakes_{run_ts}.ndjson"
    )

def upload_to_gcs(bucket_name: str, object_path: str, data: str) -> str:
    from google.cloud import storage
    from google.api_core.exceptions import NotFound

    try:
        client = storage.Client.from_service_account_json(
            r"C:\Users\aabel\earthquake-datapipeline\keys\gcp-credentials.json"
        )
        blob = client.bucket(bucket_name).blob(object_path)
        blob.upload_from_string(data, content_type="application/x-ndjson")
        return f"gs://{bucket_name}/{object_path}"
    except NotFound:
        print(f"Bucket {bucket_name} does not exist")
        return None
    


def run(bucket, start, end, min_magnitude, partition_date=None, run_ts=None):
    session = _get_session()
    ingested_at = dt.datetime.now(dt.timezone.utc).isoformat()
    ndjson_collection = collect_ndjson(session, start, end, min_magnitude, ingested_at)
    resolved_partition_date = compute_partition_date(end, partition_date)
    resolved_run_ts = run_ts or compute_run_ts()
    object_path = gcs_object_path(resolved_partition_date, resolved_run_ts)
    return upload_to_gcs(bucket, object_path, ndjson_collection)
    

def test_func(bucket):
    from google.cloud import storage
    from google.api_core.exceptions import NotFound
    try:
        storage_client = storage.Client.from_service_account_json(
            r"C:\Users\aabel\earthquake-datapipeline\keys\gcp-credentials.json"
        )
        resp = storage_client.get_bucket(bucket, timeout=60)
        print(resp)
        return resp
    except NotFound:
        print(f"Bucket {bucket} does not exist")
        return None






def _parse_dt(s:str) -> date:
    return date.fromisoformat(s)

def main():
    '''
    payload = {"format":"geojson"}
    response = requests.get(API_BASE, params=payload)
    print(response.json())
    print(response.status_code)
    print(response.headers)
    '''
    parser = argparse.ArgumentParser(description="USGS earthquake -> GCS raw landing")
    parser.add_argument("--bucket", required=True, help='add bucket name in GCS')
    parser.add_argument("--start", required=True, type=_parse_dt)
    parser.add_argument("--end", required=True, type=_parse_dt)
    parser.add_argument("--min_magnitude", required=False, type=float, default=2.5)
    args = parser.parse_args()

    #session = _get_session()
    #print(_fetch_count(session, args.start, args.end, args.min_magnitude))

    test = run(args.bucket, args.start, args.end, args.min_magnitude)
    print(test)
    #test_func(args.bucket)
    #session = _get_session()
    #print((session, args.start, args.end, args.min_magnitude))


    #print(partition_date)
    #print(run_ts)

    '''
    session = _get_session()
    params = _base_params('2026-06-29', '2026-06-30', 5)
    #response = _fetch_count(session, params)
    response = _fetch_data(session, params)
    print(response)
    '''
if __name__== '__main__':
    main()
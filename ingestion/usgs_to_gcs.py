from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import time
from datetime import date

import requests
from dotenv import load_dotenv

load_dotenv()

API_BASE = "https://earthquake.usgs.gov/fdsnws/event/1"
USER_AGENT = "usgs-earthquake-pipeline/1.0 (data-engineering)"
MAX_PER_QUERY = 20000
PATH_PREFIX = "usgs-data"

# Optional explicit path to a service-account JSON key. When unset, the GCS
# client falls back to Application Default Credentials / GOOGLE_APPLICATION_CREDENTIALS.
CREDENTIALS_PATH = os.environ.get("GCP_CREDENTIALS_PATH")

log = logging.getLogger(__name__)


def _get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"user-agent": USER_AGENT})
    return session


def _base_params(starttime: date, endtime: date, mag: float | None):
    return {
        "format": "geojson",
        "starttime": starttime,
        "endtime": endtime,
        "minmagnitude": mag,
        "orderby": "time-asc",
    }


def _fetch_count(session: requests.Session, start: date, end: date, min_mag: float | None) -> int:
    url = f"{API_BASE}/count"
    response = session.get(url, params=_base_params(start, end, min_mag), timeout=60)
    response.raise_for_status()
    return response.json().get("count", 0)


def _fetch_data(session: requests.Session, start: date, end: date, min_mag: float | None):
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
    generated = fc.get("metadata", {}).get("generated", "")
    lines = []
    for feature in fc.get("features", []):
        if add_metadata:
            record = {
                **feature,
                "_ingested_at": ingested_at,
                "source_url": source_url,
                "_collection_generated": generated,
            }
        else:
            record = feature
        lines.append(json.dumps(record, separators=(",", ":"), ensure_ascii=False))
    return "\n".join(lines) + ("\n" if lines else "")


def collect_ndjson(session: requests.Session, start: date, end: date, min_mag: float | None, ingested_at: str):
    count = _fetch_count(session, start, end, min_mag)
    log.info("USGS reports %s events for %s..%s (min_mag=%s)", count, start, end, min_mag)
    if count > MAX_PER_QUERY:
        raise RuntimeError(
            f"{count} results exceed the USGS per-query cap of {MAX_PER_QUERY}; narrow the window"
        )
    fc = _fetch_data(session, start, end, min_mag)
    return geojson_collection_to_ndjson(fc, ingested_at, fc.get("metadata", {}).get("url", ""))


def compute_partition_date(end: date, override: date | None = None) -> date:
    """Partition is derived from the query window unless explicitly overridden (backfills)."""
    return override or end


def gcs_object_path(partition_date: date, prefix: str = PATH_PREFIX) -> str:
    return (
        f"{prefix}/"
        f"year={partition_date:%Y}/month={partition_date:%m}/day={partition_date:%d}/"
        f"usgs_earthquakes_{partition_date:%Y%m%d}.ndjson"
    )


def _storage_client():
    from google.cloud import storage

    if CREDENTIALS_PATH:
        return storage.Client.from_service_account_json(CREDENTIALS_PATH)
    return storage.Client()


def upload_to_gcs(bucket_name: str, object_path: str, data: str, overwrite: bool = False) -> str:
    client = _storage_client()
    bucket = client.bucket(bucket_name)
    target = f"gs://{bucket_name}/{object_path}"

    if not bucket.exists():
        raise RuntimeError(f"bucket gs://{bucket_name} does not exist or is not accessible")

    blob = bucket.blob(object_path)
    if blob.exists() and not overwrite:
        log.info("object already exists, skipping upload: %s", target)
        return target

    blob.upload_from_string(data, content_type="application/x-ndjson")
    log.info("uploaded %d bytes to %s", len(data.encode("utf-8")), target)
    return target


def run(bucket, start, end, min_magnitude, partition_date=None, overwrite=False):
    session = _get_session()
    ingested_at = dt.datetime.now(dt.timezone.utc).isoformat()
    ndjson_collection = collect_ndjson(session, start, end, min_magnitude, ingested_at)
    resolved_partition_date = compute_partition_date(end, partition_date)
    object_path = gcs_object_path(resolved_partition_date)
    return upload_to_gcs(bucket, object_path, ndjson_collection, overwrite=overwrite)


def _parse_dt(s: str) -> date:
    return date.fromisoformat(s)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    parser = argparse.ArgumentParser(description="USGS earthquake -> GCS raw landing")
    parser.add_argument("--bucket", required=True, help="destination GCS bucket name")
    parser.add_argument("--start", required=True, type=_parse_dt)
    parser.add_argument("--end", required=True, type=_parse_dt)
    parser.add_argument("--min_magnitude", required=False, type=float, default=2.5)
    parser.add_argument(
        "--partition_date",
        type=_parse_dt,
        default=None,
        help="override the partition date (for backfills); defaults to --end",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="re-upload even if the target object already exists",
    )
    args = parser.parse_args()

    target = run(
        args.bucket,
        args.start,
        args.end,
        args.min_magnitude,
        partition_date=args.partition_date,
        overwrite=args.overwrite,
    )
    log.info("done: %s", target)


if __name__ == "__main__":
    main()

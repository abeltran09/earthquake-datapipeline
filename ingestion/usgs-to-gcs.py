from __future__ import annotations

import requests
from datetime import datetime, date
import datetime as dt
import time
import argparse

API_BASE = "https://earthquake.usgs.gov/fdsnws/event/1"
USER_AGENT = "usgs-earthquake-pipeline/1.0 (data-engineering)"
MAX_PER_QUERY = 20000

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
    return response.json()

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

def gcs_object_path(partition_date: date, run_ts: str, prefix: str = PATH_PREFIX) -> str:
    return (
        f"{prefix}/"
        f"year={partition_date:%Y}/month={partition_date:%m}/day={partition_date:%d}/"
        f"usgs_earthquakes_{run_ts}.ndjson"
    )



def run(bucket, start, end, min_magnitude, partition_date, run_ts):
    pass





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
    #parser.add_argument("--bucket", required=True, help='add bucket name in GCS')
    parser.add_argument("--start", required=True, type=_parse_dt)
    parser.add_argument("--end", required=True, type=_parse_dt)
    parser.add_argument("--min_magnitude", required=False, type=float, default=2.5)
    parser.add_argument("--partition_date", type=lambda s: date.fromisoformat(s), default=None)
    parser.add_argument("--run_ts", default=None)
    args = parser.parse_args()

    partition_date = args.partition_date or args.end
    run_ts = args.run_ts or dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    test = run(args.bucket, args.start, args.end, args.min_magnitude, args.partition_date, args.run_ts)



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
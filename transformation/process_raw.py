"""Phase 3 orchestrator: pull latest raw file per city from S3,
validate it, transform it, and write a curated JSON locally
(the database arrives in Phase 4).
"""

import json
import os
import sys

import boto3

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import CITIES, RAW_DATA_DIR, S3_BUCKET, S3_KEY_PREFIX
from transformation.transform import reshape_payload, clean_records
from transformation.validate import DataValidationError, validate_records

CURATED_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "curated",
)


def _latest_raw_key(s3_client, city_name: str) -> str:
    """Find the most recently uploaded raw file for a city in S3.

    Prefix search = we only ever list objects under raw/city=<city>/,
    never the whole bucket (efficient and correct).
    """
    prefix = f"{S3_KEY_PREFIX}/city={city_name}/"
    resp = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)

    objects = resp.get("Contents", [])
    if not objects:
        raise FileNotFoundError(f"No raw files found in S3 for {city_name}")

    # LastModified is a datetime; max() picks the newest file
    latest = max(objects, key=lambda o: o["LastModified"])
    return latest["Key"]


def _download_json(s3_client, key: str) -> dict:
    resp = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))


def run_processing() -> list:
    s3 = boto3.client("s3")
    os.makedirs(CURATED_DIR, exist_ok=True)
    curated_files = []

    for city_name in CITIES:
        key = _latest_raw_key(s3, city_name)
        print(f"[{city_name}] reading {key}")

        payload = _download_json(s3, key)
        raw_records = reshape_payload(payload, city_name)   # columns -> rows
        validate_records(raw_records)                        # THE GATE
        clean = clean_records(raw_records)                   # types + derived field

        out_path = os.path.join(CURATED_DIR, f"{city_name}_curated.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(clean, f, indent=2)

        print(f"[{city_name}] OK -> {len(clean)} clean rows -> {out_path}")
        curated_files.append(out_path)

    return curated_files


if __name__ == "__main__":
    files = run_processing()
    print(f"\nDone. {len(files)} curated file(s) written to '{CURATED_DIR}/'")
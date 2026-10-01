"""Download raw weather data for the configured cities."""
import json
import os
import sys
from datetime import datetime, timezone

import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.s3_uploader import upload_raw_file

from config.settings import (
    CITIES,
    DAILY_VARIABLES,
    FORECAST_DAYS,
    OPEN_METEO_BASE_URL,
    RAW_DATA_DIR,
    TIMEZONE,
)


def fetch_city_weather(city_name, coords):
    """Get one city's weather response from Open-Meteo."""
    latitude, longitude = coords
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": ",".join(DAILY_VARIABLES),
        "timezone": TIMEZONE,
        "forecast_days": FORECAST_DAYS,
    }

    print(f"Fetching {city_name}...")
    response = requests.get(OPEN_METEO_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def save_raw_json(city_name: str, payload: dict) -> tuple:
    """Save the raw payload to data/raw. Returns (filepath, run_date)."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"{city_name}_{timestamp}.json"
    filepath = os.path.join(RAW_DATA_DIR, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    run_date = timestamp[:4] + "-" + timestamp[4:6] + "-" + timestamp[6:8]
    return filepath, run_date


def run_ingestion() -> list:
    """Ingest all cities: capture raw -> save local -> upload to S3."""
    saved_uris = []
    for city_name, coords in CITIES.items():
        payload = fetch_city_weather(city_name, coords)
        filepath, run_date = save_raw_json(city_name, payload)

        s3_uri = upload_raw_file(filepath, city_name, run_date)
        print(f"[{city_name}] local -> {filepath}")
        print(f"[{city_name}] s3    -> {s3_uri}")

        saved_uris.append(s3_uri)
    return saved_uris


if __name__ == "__main__":
    uris = run_ingestion()
    print(f"\nDone. {len(uris)} raw file(s) now in S3.")
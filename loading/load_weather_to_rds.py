"""Load raw weather JSON files into the RDS Postgres database."""

import json
from pathlib import Path

import psycopg2

from config.settings import RAW_DATA_DIR
from loading.load_postgres import DB_NAME, ensure_database, get_connection_kwargs

TABLE_NAME = "weather_daily"


def extract_daily_rows(payload: dict, city_name: str, source_file: str):
    """Turn one raw Open-Meteo payload into insert-ready rows."""
    daily = payload.get("daily", {})
    dates = daily.get("time", [])
    if not dates:
        return []

    row_map = {
        "temperature_2m_max": daily.get("temperature_2m_max", []),
        "temperature_2m_min": daily.get("temperature_2m_min", []),
        "precipitation_sum": daily.get("precipitation_sum", []),
        "wind_speed_10m_max": daily.get("wind_speed_10m_max", []),
    }

    rows = []
    for index, measurement_date in enumerate(dates):
        row = {
            "city": city_name,
            "measurement_date": measurement_date,
            "source_file": source_file,
            "temperature_2m_max": row_map["temperature_2m_max"][index]
            if index < len(row_map["temperature_2m_max"])
            else None,
            "temperature_2m_min": row_map["temperature_2m_min"][index]
            if index < len(row_map["temperature_2m_min"])
            else None,
            "precipitation_sum": row_map["precipitation_sum"][index]
            if index < len(row_map["precipitation_sum"])
            else None,
            "wind_speed_10m_max": row_map["wind_speed_10m_max"][index]
            if index < len(row_map["wind_speed_10m_max"])
            else None,
        }
        rows.append(row)

    return rows


def get_raw_files(raw_dir: str = RAW_DATA_DIR):
    """Return all raw weather files in the configured folder."""
    directory = Path(raw_dir)
    if not directory.exists():
        return []
    return sorted(directory.glob("*.json"))


def ensure_table(cur) -> None:
    """Create the weather table if it does not exist."""
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            city VARCHAR(50) NOT NULL,
            measurement_date DATE NOT NULL,
            source_file VARCHAR(200) NOT NULL,
            temperature_2m_max DOUBLE PRECISION,
            temperature_2m_min DOUBLE PRECISION,
            precipitation_sum DOUBLE PRECISION,
            wind_speed_10m_max DOUBLE PRECISION,
            inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (city, measurement_date)
        )
        """
    )


def load_weather_to_rds(raw_dir: str = RAW_DATA_DIR, database_name: str | None = None) -> int:
    """Load all JSON weather files into Postgres and return the number of rows written."""
    target_db = database_name or DB_NAME

    ensure_database()

    conn = psycopg2.connect(**get_connection_kwargs(target_db))
    try:
        with conn.cursor() as cur:
            ensure_table(cur)

            total_rows = 0
            for file_path in get_raw_files(raw_dir):
                with file_path.open("r", encoding="utf-8") as infile:
                    payload = json.load(infile)

                city_name = file_path.stem.split("_", 1)[0]
                rows = extract_daily_rows(payload, city_name, file_path.name)
                if not rows:
                    continue

                insert_sql = f"""
                    INSERT INTO {TABLE_NAME} (
                        city,
                        measurement_date,
                        source_file,
                        temperature_2m_max,
                        temperature_2m_min,
                        precipitation_sum,
                        wind_speed_10m_max
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (city, measurement_date)
                    DO UPDATE SET
                        source_file = EXCLUDED.source_file,
                        temperature_2m_max = EXCLUDED.temperature_2m_max,
                        temperature_2m_min = EXCLUDED.temperature_2m_min,
                        precipitation_sum = EXCLUDED.precipitation_sum,
                        wind_speed_10m_max = EXCLUDED.wind_speed_10m_max,
                        inserted_at = NOW()
                """

                values = [
                    (
                        row["city"],
                        row["measurement_date"],
                        row["source_file"],
                        row["temperature_2m_max"],
                        row["temperature_2m_min"],
                        row["precipitation_sum"],
                        row["wind_speed_10m_max"],
                    )
                    for row in rows
                ]

                cur.executemany(insert_sql, values)
                total_rows += len(values)
                print(f"Loaded {len(values)} row(s) from {file_path.name} into {target_db}.")

        conn.commit()
        return total_rows
    finally:
        conn.close()


if __name__ == "__main__":
    total = load_weather_to_rds()
    print(f"Weather load complete. {total} row(s) processed.")

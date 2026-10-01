import json
import os
from pathlib import Path

import psycopg2


DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "weather_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_SSLMODE = os.getenv("DB_SSLMODE") or "disable"
CURATED_DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "curated"


def get_connection_kwargs(database_name: str = "postgres") -> dict:
    """Build the connection kwargs for Postgres."""
    kwargs = {
        "host": DB_HOST,
        "port": DB_PORT,
        "dbname": database_name,
        "user": DB_USER,
        "password": DB_PASSWORD,
        "connect_timeout": 15,
        "sslmode": DB_SSLMODE,
    }
    return kwargs


def ensure_database() -> None:
    """Create database if it doesn't exist."""
    conn = psycopg2.connect(**get_connection_kwargs("postgres"))
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{DB_NAME}"')
                print(f"Created database '{DB_NAME}'.")
            else:
                print(f"Database '{DB_NAME}' already exists.")
    finally:
        conn.close()


def run_load() -> int:
    """Load validated weather records into Postgres.
    
    This task runs daily and loads the latest curated weather data,
    upserting records so new data updates existing records and adds new ones.
    This allows the database to accumulate daily weather records.
    """
    curated_files = sorted(CURATED_DATA_DIR.glob("*_curated.json"))
    if not curated_files:
        raise FileNotFoundError(f"No curated weather files found in {CURATED_DATA_DIR}")

    print(f"Found {len(curated_files)} curated file(s) to load")
    
    ensure_database()
    
    conn = psycopg2.connect(**get_connection_kwargs(DB_NAME))
    total_rows = 0
    try:
        with conn.cursor() as cur:
            # Create table
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS weather_daily (
                    city VARCHAR(50) NOT NULL,
                    measurement_date DATE NOT NULL,
                    source_file VARCHAR(200) NOT NULL,
                    temperature_2m_max DOUBLE PRECISION,
                    temperature_2m_min DOUBLE PRECISION,
                    precipitation_sum DOUBLE PRECISION,
                    wind_speed_10m_max DOUBLE PRECISION,
                    temp_range_c DOUBLE PRECISION,
                    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (city, measurement_date)
                )
                """
            )
            cur.execute(
                "ALTER TABLE weather_daily "
                "ADD COLUMN IF NOT EXISTS temp_range_c DOUBLE PRECISION"
            )
            cur.execute(
                "ALTER TABLE weather_daily "
                "ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()"
            )

            insert_sql = """
                INSERT INTO weather_daily (
                    city, measurement_date, source_file, temperature_2m_max,
                    temperature_2m_min, precipitation_sum, wind_speed_10m_max,
                    temp_range_c
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (city, measurement_date) DO UPDATE SET
                    source_file = EXCLUDED.source_file,
                    temperature_2m_max = EXCLUDED.temperature_2m_max,
                    temperature_2m_min = EXCLUDED.temperature_2m_min,
                    precipitation_sum = EXCLUDED.precipitation_sum,
                    wind_speed_10m_max = EXCLUDED.wind_speed_10m_max,
                    temp_range_c = EXCLUDED.temp_range_c,
                    updated_at = NOW()
            """

            for file_path in curated_files:
                with file_path.open("r", encoding="utf-8") as infile:
                    records = json.load(infile)
                if not isinstance(records, list):
                    raise ValueError(f"Expected a JSON list in {file_path}")

                values = [
                    (
                        record["city"],
                        record["date"],
                        file_path.name,
                        record["temp_max_c"],
                        record["temp_min_c"],
                        record["precipitation_mm"],
                        record["wind_max_kmh"],
                        record["temp_range_c"],
                    )
                    for record in records
                ]
                if values:
                    cur.executemany(insert_sql, values)
                    total_rows += len(values)
                    print(f"Loaded {len(values)} curated row(s) from {file_path.name}.")

        conn.commit()
        print(f"Successfully loaded {total_rows} total rows into weather_daily table.")
        return total_rows
    except Exception as e:
        conn.rollback()
        print(f"Error loading data: {e}")
        raise
    finally:
        conn.close()

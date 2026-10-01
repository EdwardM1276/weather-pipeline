# Weather Data Pipeline

An end-to-end weather data engineering pipeline for Durban, Cape Town, Johannesburg, and Pretoria. The project fetches seven-day forecasts, stores raw data in Amazon S3, validates and transforms the records, loads curated data into PostgreSQL, and orchestrates the workflow with Apache Airflow.

## Architecture

```text
Open-Meteo API
      |
      v
Fetch and save raw JSON
      |
      v
Amazon S3 raw layer
      |
      v
Validate -> transform -> curate
      |
      v
PostgreSQL weather_daily table
```

Airflow runs the stages in this order:

```text
ingest_to_s3 -> validate_transform -> load_postgres
```

## Pipeline Stages

1. **Configuration**: Defines cities, coordinates, API variables, timezone, forecast period, and storage settings in `config/settings.py`.
2. **Ingestion**: Requests forecast data from Open-Meteo, saves timestamped JSON under `data/raw`, and uploads each file to S3.
3. **Raw storage**: Stores objects using the partitioned key format `raw/city=<city>/date=<date>/<filename>.json`.
4. **Quality and transformation**: Reads the latest S3 object for each city, reshapes daily arrays into records, validates the data, converts types, and derives `temp_range_c`.
5. **Database loading**: Writes curated JSON to PostgreSQL using an idempotent upsert keyed by `(city, measurement_date)`.
6. **Orchestration**: Airflow schedules the DAG daily at 06:00 South African time, retries failed tasks, and reports task failures.

## Technology Stack

- Python
- Open-Meteo REST API
- Amazon S3 and boto3
- PostgreSQL and psycopg2
- Apache Airflow 2.9.3
- Docker and Docker Compose
- pytest-compatible validation tests

## Prerequisites

- Docker Desktop with the Linux engine enabled
- AWS credentials with access to the configured S3 bucket
- PostgreSQL/RDS credentials for the weather database
- Python 3.10+ for local development and tests

## Configuration

Copy the example environment file and set values for your environment:

```powershell
Copy-Item .env.example .env
```

Required variables:

```env
DB_HOST=your-rds-endpoint
DB_PORT=5432
DB_NAME=your-database-name
DB_USER=your-database-user
DB_PASSWORD=your-database-password
AWS_DEFAULT_REGION=eu-west-1
```

Do not commit `.env`, AWS keys, database passwords, certificates, virtual environments, or generated data. These are excluded by `.gitignore`.

The S3 bucket and prefix are currently defined in `config/settings.py`. Update them for your own AWS account before running ingestion.

## Run with Docker Compose

Build and start the Airflow services:

```powershell
docker compose up --build -d
```

Open the Airflow web interface at:

```text
http://localhost:8080
```

The local development account is created by `airflow-init`. Change the demo credentials before using the deployment beyond local development.

To view service status and logs:

```powershell
docker compose ps
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver
```

To stop the services without deleting the database volume:

```powershell
docker compose down
```

The Postgres service uses a named volume, a healthcheck, and `restart: unless-stopped` so Airflow waits for a healthy metadata database and the database can recover from a stopped container.

## Run Individual Stages Locally

Activate the virtual environment and install dependencies:

```powershell
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Run ingestion and processing in order:

```powershell
python -m ingestion.fetch_weather
python -m transformation.process_raw
```

The Airflow DAG imports the same stage functions. The database task reads curated files from `data/curated` and calls `loading.load_postgres.run_load`.

## Data Quality Rules

The validation gate rejects records with:

- Missing or null required fields
- Malformed dates
- Non-numeric measurements
- Temperatures outside the configured plausible range
- Maximum temperature below minimum temperature
- Negative precipitation or wind values

Invalid batches fail with a complete violation report instead of silently dropping records.

## Verification

The diagnostic run successfully fetched and uploaded four city payloads, processed seven days per city, and produced 28 curated records. Validation cases for malformed and valid data passed, Python compilation/import checks passed, and the Docker Compose configuration validated successfully.

The database loader was tested with a mocked connection. A complete live load requires valid PostgreSQL/RDS credentials and network access from the Airflow containers.

## Project Structure

```text
config/          Shared configuration
dags/            Airflow DAG
ingestion/       API ingestion and S3 upload
loading/         PostgreSQL loaders
transformation/  Reshaping, validation, and curation
tests/           Validation and loader tests
data/            Local runtime data, ignored by Git
Dockerfile       Airflow image definition
docker-compose.yml  Local Airflow and PostgreSQL services
```

## Future Improvements

- Add a live PostgreSQL integration test
- Move credentials to a secrets backend
- Add monitoring for task failures, database health, and data freshness
- Add S3 lifecycle policies and local data retention
- Preserve forecast snapshots if historical forecast revisions are required

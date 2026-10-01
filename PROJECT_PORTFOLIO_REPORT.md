 # Weather Data Pipeline: Data Engineering Project Report

## Project Overview

This project implements an end-to-end weather data engineering workflow for Durban, Cape Town, Johannesburg, and Pretoria. It collects seven-day forecasts, stores raw data, validates and transforms records, loads curated data into PostgreSQL, and orchestrates the workflow with Apache Airflow.

## Pipeline Stages

### 1. Configuration and Data Source Design

The project centralizes API settings, city coordinates, requested weather variables, timezone, forecast length, local paths, and S3 configuration. This keeps environment-specific values separate from processing logic and makes the pipeline easier to maintain.

**Skills demonstrated:** configuration design, API planning, modular Python development.

### 2. Weather Data Ingestion

The ingestion process calls the Open-Meteo forecast API for each city. It requests daily maximum and minimum temperature, precipitation, and maximum wind speed. Each response is checked for HTTP errors and saved as timestamped raw JSON.

**Skills demonstrated:** REST API integration, request timeouts, error handling, JSON processing, reproducible raw capture.

### 3. Amazon S3 Raw Data Layer

Raw files are uploaded to S3 using partitioned keys:

```text
raw/city=<city>/date=<date>/<filename>.json
```

The S3 uploader uses boto3, retry configuration, timeouts, content metadata, and explicit error handling for missing credentials or failed uploads.

**Skills demonstrated:** AWS S3, boto3, cloud object storage, partition design, credential-aware programming.

### 4. Data Quality and Transformation

The processing stage retrieves the latest S3 file for each city and reshapes the API's column-oriented arrays into daily records. Validation occurs before type conversion and checks:

- Required and non-null fields
- Valid ISO dates
- Numeric measurements
- Plausible temperature ranges
- Maximum temperature greater than or equal to minimum temperature
- Non-negative precipitation and wind values

Clean records are converted to numeric types and enriched with `temp_range_c`. The results are written as curated JSON files.

**Skills demonstrated:** data-quality gates, schema validation, defensive programming, data cleansing, feature derivation.

### 5. PostgreSQL Loading

The curated loader creates the target database and `weather_daily` table when required, then upserts records using `(city, measurement_date)` as the key. This makes repeated runs idempotent and prevents duplicate daily records.

The table stores weather measurements, the source filename, the derived temperature range, and an insertion timestamp. Database connections support timeouts and SSL for non-local hosts.

**Skills demonstrated:** relational data modelling, SQL DDL, PostgreSQL, upserts, idempotent loading, transaction handling.

### 6. Airflow and Docker Orchestration

Airflow coordinates the workflow through the dependency chain:

```text
ingest_to_s3 -> validate_transform -> load_postgres
```

The DAG runs daily at 06:00 South African time, disables catchup, retries failed tasks three times, and logs task failures through a callback. Docker Compose packages Airflow and PostgreSQL, mounts the project into the containers, persists database data in a named volume, and uses a PostgreSQL healthcheck before starting Airflow services.

**Skills demonstrated:** Apache Airflow, DAG design, scheduling, retries, Docker, Docker Compose, service healthchecks, environment-based configuration.

## Verification and Results

The diagnostic run successfully:

- Fetched forecasts for all four cities
- Uploaded four raw payloads to S3
- Retrieved and validated the latest objects
- Produced seven curated records per city, 28 records total
- Passed the validation test cases for malformed and valid data
- Passed Python compilation/import checks
- Passed Docker Compose configuration validation

The loader was also checked with a mocked database connection. A live PostgreSQL/RDS load still requires the target database username and configured credentials.

## Data Engineer Skills Demonstrated

- Python development for ingestion, transformation, and loading
- REST APIs and JSON data handling
- AWS S3 and boto3
- Data validation and quality control
- Data modelling and PostgreSQL
- Idempotent ETL design
- Apache Airflow orchestration
- Docker-based development and deployment
- Logging, retries, healthchecks, and failure handling
- Troubleshooting across application, database, and infrastructure layers

## Challenges and Lessons

The Airflow DAG initially referenced a missing `run_load()` function. Implementing the curated-data loader resolved the task contract and ensured that only validated records reach PostgreSQL. The local PostgreSQL container also exited and interrupted Airflow metadata connectivity; a persistent volume, healthcheck, and restart policy improved resilience.

The main lesson was to build and verify each layer independently before connecting the full workflow. Separating raw storage, validation, transformation, loading, and orchestration made failures easier to isolate and the pipeline easier to extend.

## Next Steps

- Complete a live Airflow-to-PostgreSQL/RDS integration run
- Add automated loader and database integration tests
- Move credentials to a secrets backend
- Add monitoring for task failures, database health, and data freshness

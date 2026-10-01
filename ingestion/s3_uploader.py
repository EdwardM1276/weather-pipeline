"""Phase 2: S3 raw data layer.

Uploads raw JSON files to S3 using a Hive-style partitioned key structure:

    raw/city=<city>/date=<yyyy-mm-dd>/<original_filename>

Nothing here knows about weather data or APIs — it only knows how to
move a local file to S3 safely. That separation keeps it reusable and
testable in isolation.
"""

import os
import sys

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
except ImportError as exc:  # pragma: no cover - depends on runtime env
    boto3 = None
    Config = None
    BotoCoreError = ClientError = NoCredentialsError = Exception
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import S3_BUCKET, S3_KEY_PREFIX


def _build_s3_key(local_filepath: str, city_name: str, run_date: str) -> str:
    """Build the partitioned S3 key for a raw file.

    run_date is the YYYY-MM-DD date the data *belongs to* (taken from the
    filename timestamp we stamped at capture time), not today's date.
    """
    filename = os.path.basename(local_filepath)
    return f"{S3_KEY_PREFIX}/city={city_name}/date={run_date}/{filename}"


def _make_s3_client():
    """Build an S3 client with explicit retry behavior."""
    if boto3 is None:
        raise RuntimeError(
            "boto3 is not installed. Install the project requirements or run "
            "'pip install -r requirements.txt' before using S3 upload."
        ) from _IMPORT_ERROR

    return boto3.client(
        "s3",
        config=Config(
            retries={"max_attempts": 3, "mode": "standard"},
            connect_timeout=10,
            read_timeout=30,
        ),
    )


def upload_raw_file(local_filepath: str, city_name: str, run_date: str) -> str:
    """Upload one raw file to S3. Returns the s3:// URI on success.

    Raises on failure — a pipeline stage that silently half-completes is
    worse than one that loudly fails (Airflow will retry it in Phase 5).
    """
    if not os.path.exists(local_filepath):
        raise FileNotFoundError(f"Local file missing: {local_filepath}")

    key = _build_s3_key(local_filepath, city_name, run_date)
    client = _make_s3_client()

    try:
        client.upload_file(
            Filename=local_filepath,
            Bucket=S3_BUCKET,
            Key=key,
            ExtraArgs={
                "ContentType": "application/json",
                "Metadata": {"pipeline": "weather", "stage": "raw"},
            },
        )
    except NoCredentialsError:
        # Credential-chain problem — fail fast with a helpful message.
        raise RuntimeError(
            "AWS credentials not found. Run 'aws configure' or check "
            "~/.aws/credentials."
        )
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"S3 upload failed for {key}: {e}") from e

    return f"s3://{S3_BUCKET}/{key}"
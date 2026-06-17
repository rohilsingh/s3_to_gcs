import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from google.cloud import storage as gcs_storage
from google.auth import load_credentials_from_file

from src.config import BUCKET_SHORT_NAMES, STREAM_CHUNK_SIZE, WIF_CREDENTIAL_FILE

logger = logging.getLogger(__name__)
_s3 = boto3.client("s3")
_gcs_client: Optional[gcs_storage.Client] = None


def _get_gcs_client() -> gcs_storage.Client:
    global _gcs_client
    if _gcs_client is None:
        creds, _ = load_credentials_from_file(WIF_CREDENTIAL_FILE)
        _gcs_client = gcs_storage.Client(credentials=creds)
    return _gcs_client


def _archive_key(source_bucket: str, s3_key: str, last_modified: datetime) -> str:
    yymmdd = last_modified.strftime("%y%m%d")
    short = BUCKET_SHORT_NAMES.get(source_bucket, source_bucket)
    return f"{yymmdd}/{short}/{s3_key}"


def _try_get_s3_stream(bucket: str, key: str):
    try:
        resp = _s3.get_object(Bucket=bucket, Key=key)
        return resp["Body"], resp["ContentLength"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return None, 0
        raise


def get_s3_stream(
    source_bucket: str, s3_key: str, archive_bucket: str,
    last_modified: datetime,
) -> tuple:
    stream, size = _try_get_s3_stream(source_bucket, s3_key)
    if stream is not None:
        return stream, size, False

    logger.info("Source gone, trying archive bucket for %s", s3_key)
    archive_k = _archive_key(source_bucket, s3_key, last_modified)
    stream, size = _try_get_s3_stream(archive_bucket, archive_k)
    if stream is not None:
        return stream, size, True

    for delta in (-1, 1):
        alt_date = last_modified + timedelta(days=delta)
        alt_key = _archive_key(source_bucket, s3_key, alt_date)
        stream, size = _try_get_s3_stream(archive_bucket, alt_key)
        if stream is not None:
            logger.info("Found archive at +/- 1 day: %s", alt_key)
            return stream, size, True

    raise FileNotFoundError(
        f"Object not found in source ({source_bucket}/{s3_key}) "
        f"or archive ({archive_bucket})"
    )


def stream_to_gcs(
    s3_stream, gcs_bucket_name: str, gcs_key: str, size_bytes: int,
) -> int:
    client = _get_gcs_client()
    bucket = client.bucket(gcs_bucket_name)
    blob = bucket.blob(gcs_key)

    total = 0
    with blob.open("wb", chunk_size=STREAM_CHUNK_SIZE) as writer:
        while True:
            chunk = s3_stream.read(STREAM_CHUNK_SIZE)
            if not chunk:
                break
            writer.write(chunk)
            total += len(chunk)

    logger.info("Streamed %d bytes to gs://%s/%s", total, gcs_bucket_name, gcs_key)
    return total

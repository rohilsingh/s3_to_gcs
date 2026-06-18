import logging
from typing import Optional

from google.cloud import bigquery
from google.auth import load_credentials_from_file

from src.config import (
    GCP_PROJECT_ID, CONTROL_TABLE, WIF_CREDENTIAL_FILE, bq_table_ref,
)

logger = logging.getLogger(__name__)

_bq_client: Optional[bigquery.Client] = None


def _get_bq_client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        creds, _ = load_credentials_from_file(WIF_CREDENTIAL_FILE)
        _bq_client = bigquery.Client(project=GCP_PROJECT_ID, credentials=creds)
    return _bq_client


def _row_to_dict(row) -> dict:
    return {
        "file_id": row.file_id,
        "file_path_pattern": row.file_path_pattern,
        "source_bucket_name": row.source_bucket_name,
        "destination_bucket_name": row.destination_bucket_name,
        "destination_base": row.destination_base,
        "destination_prefix_base": row.destination_prefix_base,
        "archive_after_copy_enabled": row.archive_after_copy_enabled,
        "archive_to_bucket": row.archive_to_bucket,
    }


def match_pattern(source_bucket: str, s3_key: str) -> Optional[dict]:
    client = _get_bq_client()
    query = f"""
        SELECT file_id, file_path_pattern, source_bucket_name,
               destination_bucket_name, destination_base,
               destination_prefix_base, archive_after_copy_enabled,
               archive_to_bucket
        FROM `{bq_table_ref(CONTROL_TABLE)}`
        WHERE enabled = TRUE
          AND source_bucket_name = @source_bucket
          AND REGEXP_CONTAINS(@s3_key, file_path_pattern)
        LIMIT 2
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_bucket", "STRING", source_bucket),
            bigquery.ScalarQueryParameter("s3_key", "STRING", s3_key),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())

    if len(rows) == 0:
        return None
    if len(rows) > 1:
        raise ValueError(
            f"CONFIG BUG: key '{s3_key}' in bucket '{source_bucket}' "
            f"matched {len(rows)}+ patterns: "
            f"{[r.file_path_pattern for r in rows]}"
        )
    return _row_to_dict(rows[0])


def get_pattern_by_file_id(file_id: int) -> Optional[dict]:
    client = _get_bq_client()
    query = f"""
        SELECT file_id, file_path_pattern, source_bucket_name,
               destination_bucket_name, destination_base,
               destination_prefix_base, archive_after_copy_enabled,
               archive_to_bucket
        FROM `{bq_table_ref(CONTROL_TABLE)}`
        WHERE enabled = TRUE
          AND file_id = @file_id
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("file_id", "INT64", file_id),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return None
    return _row_to_dict(rows[0])


def get_all_source_buckets() -> list[str]:
    client = _get_bq_client()
    query = f"""
        SELECT DISTINCT source_bucket_name
        FROM `{bq_table_ref(CONTROL_TABLE)}`
        WHERE enabled = TRUE
    """
    rows = client.query(query).result()
    return [row.source_bucket_name for row in rows]

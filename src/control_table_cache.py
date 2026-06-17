import re
import time
import logging
from typing import Optional

from google.cloud import bigquery
from google.auth import load_credentials_from_file

from src.config import (
    GCP_PROJECT_ID, BIGQUERY_DATASET, CONTROL_TABLE,
    CACHE_TTL_SECONDS, WIF_CREDENTIAL_FILE, bq_table_ref,
)

logger = logging.getLogger(__name__)

_cache: Optional[dict] = None
_cache_loaded_at: float = 0.0
_bq_client: Optional[bigquery.Client] = None


def _get_bq_client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        creds, _ = load_credentials_from_file(WIF_CREDENTIAL_FILE)
        _bq_client = bigquery.Client(project=GCP_PROJECT_ID, credentials=creds)
    return _bq_client


def _load_from_bq() -> dict:
    client = _get_bq_client()
    query = f"""
        SELECT
            file_id, file_path_pattern, source_bucket_name,
            destination_bucket_name, destination_base,
            destination_prefix_base, archive_after_copy_enabled,
            archive_to_bucket, enabled
        FROM `{bq_table_ref(CONTROL_TABLE)}`
        WHERE enabled = TRUE
    """
    rows = client.query(query).result()

    by_bucket: dict = {}
    for row in rows:
        entry = {
            "file_id": row.file_id,
            "file_path_pattern": row.file_path_pattern,
            "regex": re.compile(row.file_path_pattern),
            "source_bucket_name": row.source_bucket_name,
            "destination_bucket_name": row.destination_bucket_name,
            "destination_base": row.destination_base,
            "destination_prefix_base": row.destination_prefix_base,
            "archive_after_copy_enabled": row.archive_after_copy_enabled,
            "archive_to_bucket": row.archive_to_bucket,
        }
        by_bucket.setdefault(row.source_bucket_name, []).append(entry)

    logger.info("Control table loaded: %d patterns across %d buckets",
                sum(len(v) for v in by_bucket.values()), len(by_bucket))
    return by_bucket


def get_cache() -> dict:
    global _cache, _cache_loaded_at
    now = time.time()
    if _cache is None or (now - _cache_loaded_at) > CACHE_TTL_SECONDS:
        _cache = _load_from_bq()
        _cache_loaded_at = now
    return _cache


def invalidate_cache():
    global _cache, _cache_loaded_at
    _cache = None
    _cache_loaded_at = 0.0


def match_pattern(source_bucket: str, s3_key: str) -> Optional[dict]:
    cache = get_cache()
    patterns = cache.get(source_bucket, [])
    matches = [p for p in patterns if p["regex"].fullmatch(s3_key)]
    if len(matches) == 0:
        return None
    if len(matches) > 1:
        raise ValueError(
            f"CONFIG BUG: key '{s3_key}' in bucket '{source_bucket}' "
            f"matched {len(matches)} patterns: "
            f"{[m['file_path_pattern'] for m in matches]}"
        )
    return matches[0]

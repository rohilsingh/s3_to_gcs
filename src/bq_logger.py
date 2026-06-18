import logging
from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from src.config import (
    GCP_PROJECT_ID, WIF_CREDENTIAL_FILE, bq_table_ref,
    RUN_LOG_TABLE, SUCCESS_LOG_TABLE, ERROR_LOG_TABLE, ALERT_LOG_TABLE,
)

logger = logging.getLogger(__name__)

_bq_client: Optional[bigquery.Client] = None


def _get_client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        from google.auth import load_credentials_from_file
        creds, _ = load_credentials_from_file(WIF_CREDENTIAL_FILE)
        _bq_client = bigquery.Client(project=GCP_PROJECT_ID, credentials=creds)
    return _bq_client


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _insert(table_name: str, rows: list[dict]) -> bool:
    try:
        client = _get_client()
        table_ref = bq_table_ref(table_name)
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            logger.error("BQ insert errors for %s: %s", table_ref, errors)
            return False
        return True
    except GoogleAPIError:
        logger.exception("BQ unreachable for %s", table_name)
        return False


def insert_run_log(
    run_id: str, file_id: int, s3_key: str, last_modified: str,
    etag: str, size_bytes: int, gcs_key: str, attempt: int,
    trigger_source: str, read_from_archive: bool = False,
    predecessor_s3_key: str = None, predecessor_last_modified: str = None,
    started_at: str = None,
) -> bool:
    row = {
        "run_id": run_id,
        "file_id": file_id,
        "s3_key": s3_key,
        "last_modified": last_modified,
        "etag": etag,
        "size_bytes": size_bytes,
        "gcs_key": gcs_key,
        "attempt": attempt,
        "trigger_source": trigger_source,
        "read_from_archive": read_from_archive,
        "predecessor_s3_key": predecessor_s3_key,
        "predecessor_last_modified": predecessor_last_modified,
        "started_at": started_at or _now_iso(),
        "inserted_at": _now_iso(),
    }
    return _insert(RUN_LOG_TABLE, [row])


def insert_success_log(
    run_id: str, file_id: int, s3_key: str, last_modified: str,
    etag: str, gcs_key: str, bytes_copied: int, archived: bool = False,
    is_manual: bool = False, resolved_by: str = None,
    finished_at: str = None,
) -> bool:
    row = {
        "run_id": run_id,
        "file_id": file_id,
        "s3_key": s3_key,
        "last_modified": last_modified,
        "etag": etag,
        "gcs_key": gcs_key,
        "bytes_copied": bytes_copied,
        "archived": archived,
        "is_manual": is_manual,
        "resolved_by": resolved_by,
        "finished_at": finished_at or _now_iso(),
        "inserted_at": _now_iso(),
    }
    return _insert(SUCCESS_LOG_TABLE, [row])


def insert_error_log(
    run_id: str, file_id: int, s3_key: str, last_modified: str,
    etag: str, gcs_key: str, attempt: int, error_class: str,
    error_detail: str, retryable: bool = True, waiting: bool = False,
    predecessor_s3_key: str = None, predecessor_last_modified: str = None,
    step: str = None,
) -> bool:
    row = {
        "run_id": run_id,
        "file_id": file_id,
        "s3_key": s3_key,
        "last_modified": last_modified,
        "etag": etag,
        "gcs_key": gcs_key,
        "attempt": attempt,
        "waiting": waiting,
        "step": step,
        "error_class": error_class,
        "error_detail": error_detail,
        "predecessor_s3_key": predecessor_s3_key,
        "predecessor_last_modified": predecessor_last_modified,
        "retryable": retryable,
        "inserted_at": _now_iso(),
    }
    return _insert(ERROR_LOG_TABLE, [row])


def insert_alert_log(
    incident_run_id: str, file_id: int, s3_key: str,
    last_modified: str, servicenow_incident_id: str, action: str,
) -> bool:
    row = {
        "incident_run_id": incident_run_id,
        "file_id": file_id,
        "s3_key": s3_key,
        "last_modified": last_modified,
        "servicenow_incident_id": servicenow_incident_id,
        "action": action,
        "alerted_at": _now_iso(),
    }
    return _insert(ALERT_LOG_TABLE, [row])


def check_success_exists(file_id: int, s3_key: str, last_modified: str, etag: str) -> bool:
    try:
        client = _get_client()
        query = f"""
            SELECT 1 FROM `{bq_table_ref(SUCCESS_LOG_TABLE)}`
            WHERE file_id = @file_id
              AND s3_key = @s3_key
              AND last_modified = @last_modified
              AND etag = @etag
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("file_id", "INT64", file_id),
                bigquery.ScalarQueryParameter("s3_key", "STRING", s3_key),
                bigquery.ScalarQueryParameter("last_modified", "TIMESTAMP", last_modified),
                bigquery.ScalarQueryParameter("etag", "STRING", etag),
            ]
        )
        result = client.query(query, job_config=job_config).result()
        return result.total_rows > 0
    except GoogleAPIError:
        logger.exception("BQ unreachable checking success_log")
        return False


def check_predecessor_success(file_id: int, s3_key: str, last_modified: str) -> bool:
    try:
        client = _get_client()
        query = f"""
            SELECT 1 FROM `{bq_table_ref(SUCCESS_LOG_TABLE)}`
            WHERE file_id = @file_id
              AND s3_key = @s3_key
              AND last_modified = @last_modified
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("file_id", "INT64", file_id),
                bigquery.ScalarQueryParameter("s3_key", "STRING", s3_key),
                bigquery.ScalarQueryParameter("last_modified", "TIMESTAMP", last_modified),
            ]
        )
        result = client.query(query, job_config=job_config).result()
        return result.total_rows > 0
    except GoogleAPIError:
        logger.exception("BQ unreachable checking predecessor success")
        return False


def check_run_log_exists(file_id: int, s3_key: str, last_modified: str) -> bool:
    try:
        client = _get_client()
        query = f"""
            SELECT 1 FROM `{bq_table_ref(RUN_LOG_TABLE)}`
            WHERE file_id = @file_id
              AND s3_key = @s3_key
              AND last_modified = @last_modified
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("file_id", "INT64", file_id),
                bigquery.ScalarQueryParameter("s3_key", "STRING", s3_key),
                bigquery.ScalarQueryParameter("last_modified", "TIMESTAMP", last_modified),
            ]
        )
        result = client.query(query, job_config=job_config).result()
        return result.total_rows > 0
    except GoogleAPIError:
        logger.exception("BQ unreachable checking run_log")
        return False


def get_latest_attempt(file_id: int, s3_key: str, last_modified: str, etag: str) -> int:
    try:
        client = _get_client()
        query = f"""
            SELECT MAX(attempt) as max_attempt FROM `{bq_table_ref(RUN_LOG_TABLE)}`
            WHERE file_id = @file_id
              AND s3_key = @s3_key
              AND last_modified = @last_modified
              AND etag = @etag
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("file_id", "INT64", file_id),
                bigquery.ScalarQueryParameter("s3_key", "STRING", s3_key),
                bigquery.ScalarQueryParameter("last_modified", "TIMESTAMP", last_modified),
                bigquery.ScalarQueryParameter("etag", "STRING", etag),
            ]
        )
        result = list(client.query(query, job_config=job_config).result())
        if result and result[0].max_attempt is not None:
            return result[0].max_attempt
        return 0
    except GoogleAPIError:
        logger.exception("BQ unreachable getting latest attempt")
        return 0

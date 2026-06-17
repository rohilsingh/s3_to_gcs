import uuid
import logging
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery
from google.auth import load_credentials_from_file
from google.api_core.exceptions import GoogleAPIError

from src.config import (
    GCP_PROJECT_ID, WIF_CREDENTIAL_FILE, bq_table_ref,
    RUN_LOG_TABLE, SUCCESS_LOG_TABLE, ERROR_LOG_TABLE, ALERT_LOG_TABLE,
    RETRY_COOLDOWN_MINUTES,
)
from src.control_table_cache import get_cache, match_pattern
from src.gcs_key import build_gcs_key
from src.bq_logger import (
    insert_run_log, insert_success_log, insert_error_log, insert_alert_log,
    check_success_exists,
)
from src.s3_fallback import (
    list_markers, delete_marker, check_success_marker_exists,
)
from src.streaming_copy import get_s3_stream, stream_to_gcs
from src.predecessor import resolve_predecessor, is_predecessor_done
from src.archival import self_archive
from src.servicenow import create_incident, update_incident

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def handler(event, context):
    logger.info("Maintenance Lambda started")
    _reconcile_fallback_markers()
    _advance_waiting()
    _retry_failed()
    _gap_detection()
    _servicenow_alerts()
    logger.info("Maintenance Lambda complete")


# ── PF-5: Reconcile S3 fallback markers into BQ ─────────────────────────
def _reconcile_fallback_markers():
    cache = get_cache()
    all_buckets = set()
    for patterns in cache.values():
        for p in patterns:
            all_buckets.add(p["source_bucket_name"])

    for bucket in all_buckets:
        for marker in list_markers(bucket, "success"):
            ok = insert_success_log(
                run_id=marker["run_id"],
                file_id=marker["file_id"],
                s3_key=marker["s3_key"],
                last_modified=marker["last_modified"],
                etag=marker.get("etag", ""),
                gcs_key=marker.get("gcs_key", ""),
                bytes_copied=marker.get("bytes_copied", 0),
                archived=marker.get("archived", False),
            )
            if ok:
                delete_marker(bucket, marker["_marker_key"])

        for marker in list_markers(bucket, "failure"):
            ok = insert_error_log(
                run_id=marker["run_id"],
                file_id=marker["file_id"],
                s3_key=marker["s3_key"],
                last_modified=marker["last_modified"],
                etag=marker.get("etag", ""),
                gcs_key=marker.get("gcs_key", ""),
                attempt=1,
                error_class=marker.get("error_class", "UNKNOWN"),
                error_detail=marker.get("error_detail", ""),
                retryable=True,
            )
            if ok:
                delete_marker(bucket, marker["_marker_key"])

    logger.info("Fallback marker reconciliation complete")


# ── Advance WAITING files whose predecessors are now done ────────────────
def _advance_waiting():
    client = _get_bq_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=RETRY_COOLDOWN_MINUTES)).isoformat()

    query = f"""
        SELECT e.run_id, e.file_id, e.s3_key, e.last_modified, e.etag,
               e.gcs_key, e.attempt, e.predecessor_s3_key,
               e.predecessor_last_modified
        FROM `{bq_table_ref(ERROR_LOG_TABLE)}` e
        LEFT JOIN `{bq_table_ref(SUCCESS_LOG_TABLE)}` s
          ON e.file_id = s.file_id
         AND e.s3_key = s.s3_key
         AND e.last_modified = s.last_modified
        WHERE e.waiting = TRUE
          AND s.run_id IS NULL
          AND e.inserted_at < @cutoff
        ORDER BY e.last_modified ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("cutoff", "TIMESTAMP", cutoff),
        ]
    )

    try:
        rows = list(client.query(query, job_config=job_config).result())
    except GoogleAPIError:
        logger.exception("Failed to query WAITING items")
        return

    for row in rows:
        _try_transfer(
            file_id=row.file_id,
            s3_key=row.s3_key,
            last_modified=row.last_modified.isoformat() if hasattr(row.last_modified, 'isoformat') else str(row.last_modified),
            etag=row.etag or "",
            gcs_key=row.gcs_key or "",
            attempt=row.attempt + 1,
            predecessor_s3_key=row.predecessor_s3_key,
            predecessor_last_modified=(
                row.predecessor_last_modified.isoformat()
                if row.predecessor_last_modified and hasattr(row.predecessor_last_modified, 'isoformat')
                else row.predecessor_last_modified
            ),
        )

    logger.info("Advance-WAITING complete, processed %d items", len(rows))


# ── Retry FAILED files (non-waiting, last attempt > 20 min ago) ──────────
def _retry_failed():
    client = _get_bq_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=RETRY_COOLDOWN_MINUTES)).isoformat()

    query = f"""
        SELECT e.run_id, e.file_id, e.s3_key, e.last_modified, e.etag,
               e.gcs_key, e.attempt, e.predecessor_s3_key,
               e.predecessor_last_modified
        FROM `{bq_table_ref(ERROR_LOG_TABLE)}` e
        LEFT JOIN `{bq_table_ref(SUCCESS_LOG_TABLE)}` s
          ON e.file_id = s.file_id
         AND e.s3_key = s.s3_key
         AND e.last_modified = s.last_modified
        WHERE e.waiting = FALSE
          AND e.retryable = TRUE
          AND s.run_id IS NULL
          AND e.inserted_at < @cutoff
        ORDER BY e.last_modified ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("cutoff", "TIMESTAMP", cutoff),
        ]
    )

    try:
        rows = list(client.query(query, job_config=job_config).result())
    except GoogleAPIError:
        logger.exception("Failed to query FAILED items")
        return

    for row in rows:
        _try_transfer(
            file_id=row.file_id,
            s3_key=row.s3_key,
            last_modified=row.last_modified.isoformat() if hasattr(row.last_modified, 'isoformat') else str(row.last_modified),
            etag=row.etag or "",
            gcs_key=row.gcs_key or "",
            attempt=row.attempt + 1,
            predecessor_s3_key=row.predecessor_s3_key,
            predecessor_last_modified=(
                row.predecessor_last_modified.isoformat()
                if row.predecessor_last_modified and hasattr(row.predecessor_last_modified, 'isoformat')
                else row.predecessor_last_modified
            ),
        )

    logger.info("Retry-FAILED complete, processed %d items", len(rows))


# ── PF-7: Gap detection ─────────────────────────────────────────────────
def _gap_detection():
    client = _get_bq_client()

    query = f"""
        SELECT e.file_id, e.s3_key, e.last_modified,
               e.predecessor_s3_key, e.predecessor_last_modified
        FROM `{bq_table_ref(ERROR_LOG_TABLE)}` e
        LEFT JOIN `{bq_table_ref(SUCCESS_LOG_TABLE)}` s
          ON e.file_id = s.file_id
         AND e.s3_key = s.s3_key
         AND e.last_modified = s.last_modified
        WHERE e.waiting = TRUE
          AND s.run_id IS NULL
          AND e.predecessor_s3_key IS NOT NULL
    """

    try:
        waiting_rows = list(client.query(query).result())
    except GoogleAPIError:
        logger.exception("Failed to query for gap detection")
        return

    for row in waiting_rows:
        pred_key = row.predecessor_s3_key
        pred_lm = row.predecessor_last_modified

        if not pred_key:
            continue

        check_query = f"""
            SELECT 1 FROM `{bq_table_ref(RUN_LOG_TABLE)}`
            WHERE file_id = @file_id AND s3_key = @pred_key
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("file_id", "INT64", row.file_id),
                bigquery.ScalarQueryParameter("pred_key", "STRING", pred_key),
            ]
        )
        try:
            result = list(client.query(check_query, job_config=job_config).result())
            if not result:
                logger.warning(
                    "GAP DETECTED: file_id=%d, waiting on predecessor %s "
                    "which has never been seen (no run_log row)",
                    row.file_id, pred_key,
                )
        except GoogleAPIError:
            logger.exception("Gap detection query failed for file_id=%d", row.file_id)

    logger.info("Gap detection complete")


# ── PF-10 / FR-31: ServiceNow alerts (daily dedup) ──────────────────────
def _servicenow_alerts():
    client = _get_bq_client()
    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    query = f"""
        SELECT e.file_id, e.s3_key, e.last_modified, e.etag,
               e.error_class, e.error_detail,
               a.servicenow_incident_id, a.alerted_at
        FROM `{bq_table_ref(ERROR_LOG_TABLE)}` e
        LEFT JOIN `{bq_table_ref(SUCCESS_LOG_TABLE)}` s
          ON e.file_id = s.file_id
         AND e.s3_key = s.s3_key
         AND e.last_modified = s.last_modified
        LEFT JOIN (
            SELECT file_id, s3_key, last_modified,
                   servicenow_incident_id, alerted_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY file_id, s3_key, last_modified
                       ORDER BY alerted_at DESC
                   ) as rn
            FROM `{bq_table_ref(ALERT_LOG_TABLE)}`
        ) a
          ON e.file_id = a.file_id
         AND e.s3_key = a.s3_key
         AND e.last_modified = a.last_modified
         AND a.rn = 1
        WHERE e.waiting = FALSE
          AND e.retryable = TRUE
          AND s.run_id IS NULL
        GROUP BY e.file_id, e.s3_key, e.last_modified, e.etag,
                 e.error_class, e.error_detail,
                 a.servicenow_incident_id, a.alerted_at
    """

    try:
        rows = list(client.query(query).result())
    except GoogleAPIError:
        logger.exception("Failed to query for ServiceNow alerts")
        return

    for row in rows:
        last_alerted = row.alerted_at
        incident_id = row.servicenow_incident_id
        lm_str = row.last_modified.isoformat() if hasattr(row.last_modified, 'isoformat') else str(row.last_modified)

        if last_alerted and last_alerted.isoformat() > cutoff_24h:
            continue

        run_id = str(uuid.uuid4())

        if incident_id:
            try:
                update_incident(incident_id, row.file_id, row.s3_key, lm_str)
                insert_alert_log(
                    incident_run_id=run_id,
                    file_id=row.file_id,
                    s3_key=row.s3_key,
                    last_modified=lm_str,
                    servicenow_incident_id=incident_id,
                    action="reminder",
                )
            except Exception:
                logger.exception("Failed to refresh ServiceNow incident %s", incident_id)
        else:
            try:
                new_id = create_incident(
                    row.file_id, row.s3_key, lm_str,
                    row.error_class or "", row.error_detail or "",
                )
                if new_id:
                    insert_alert_log(
                        incident_run_id=run_id,
                        file_id=row.file_id,
                        s3_key=row.s3_key,
                        last_modified=lm_str,
                        servicenow_incident_id=new_id,
                        action="created",
                    )
            except Exception:
                logger.exception(
                    "Failed to create ServiceNow incident for file_id=%d",
                    row.file_id,
                )

    logger.info("ServiceNow alerts complete")


# ── Shared transfer logic (used by advance-waiting and retry-failed) ─────
def _try_transfer(
    file_id: int, s3_key: str, last_modified: str, etag: str,
    gcs_key: str, attempt: int,
    predecessor_s3_key: str = None, predecessor_last_modified: str = None,
):
    cache = get_cache()
    pattern = None
    for patterns in cache.values():
        for p in patterns:
            if p["file_id"] == file_id:
                pattern = p
                break
        if pattern:
            break

    if not pattern:
        logger.warning("No pattern found for file_id=%d, skipping", file_id)
        return

    source_bucket = pattern["source_bucket_name"]
    gcs_bucket = pattern["destination_bucket_name"]
    archive_bucket = pattern.get("archive_to_bucket", "")
    if not gcs_key:
        gcs_key = build_gcs_key(s3_key, pattern)

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()

    if check_success_exists(file_id, s3_key, last_modified, etag):
        logger.info("Already succeeded, skipping maintenance retry: %s", s3_key)
        return

    if predecessor_s3_key and not is_predecessor_done(
        file_id, predecessor_s3_key, predecessor_last_modified, source_bucket
    ):
        logger.info("Predecessor still not done for %s, re-marking WAITING", s3_key)
        insert_error_log(
            run_id=run_id, file_id=file_id, s3_key=s3_key,
            last_modified=last_modified, etag=etag, gcs_key=gcs_key,
            attempt=attempt, error_class="ORDER_BLOCKED",
            error_detail=f"Still waiting on predecessor: {predecessor_s3_key}",
            waiting=True, retryable=True,
            predecessor_s3_key=predecessor_s3_key,
            predecessor_last_modified=predecessor_last_modified,
        )
        return

    insert_run_log(
        run_id=run_id, file_id=file_id, s3_key=s3_key,
        last_modified=last_modified, etag=etag, size_bytes=0,
        gcs_key=gcs_key, attempt=attempt, trigger_source="maintenance",
        predecessor_s3_key=predecessor_s3_key,
        predecessor_last_modified=predecessor_last_modified,
        started_at=started_at,
    )

    try:
        lm_dt = datetime.fromisoformat(last_modified) if isinstance(last_modified, str) else last_modified
        s3_stream, actual_size, read_from_archive = get_s3_stream(
            source_bucket, s3_key, archive_bucket, lm_dt,
        )
    except FileNotFoundError as e:
        insert_error_log(
            run_id=run_id, file_id=file_id, s3_key=s3_key,
            last_modified=last_modified, etag=etag, gcs_key=gcs_key,
            attempt=attempt, error_class="S3_READ", error_detail=str(e),
            retryable=True,
        )
        return
    except Exception as e:
        insert_error_log(
            run_id=run_id, file_id=file_id, s3_key=s3_key,
            last_modified=last_modified, etag=etag, gcs_key=gcs_key,
            attempt=attempt, error_class="S3_READ", error_detail=str(e),
            retryable=True,
        )
        return

    try:
        bytes_copied = stream_to_gcs(s3_stream, gcs_bucket, gcs_key, actual_size)
    except Exception as e:
        insert_error_log(
            run_id=run_id, file_id=file_id, s3_key=s3_key,
            last_modified=last_modified, etag=etag, gcs_key=gcs_key,
            attempt=attempt, error_class="GCS_WRITE", error_detail=str(e),
            retryable=True,
        )
        return

    archived = False
    if pattern.get("archive_after_copy_enabled") and archive_bucket:
        try:
            self_archive(source_bucket, s3_key, archive_bucket, lm_dt)
            archived = True
        except Exception as e:
            insert_error_log(
                run_id=run_id, file_id=file_id, s3_key=s3_key,
                last_modified=last_modified, etag=etag, gcs_key=gcs_key,
                attempt=attempt, error_class="ARCHIVE", error_detail=str(e),
                retryable=True,
            )
            return

    insert_success_log(
        run_id=run_id, file_id=file_id, s3_key=s3_key,
        last_modified=last_modified, etag=etag, gcs_key=gcs_key,
        bytes_copied=bytes_copied, archived=archived,
    )

    logger.info("Maintenance transfer complete: %s -> gs://%s/%s", s3_key, gcs_bucket, gcs_key)


# ── BQ client ────────────────────────────────────────────────────────────
_bq_client = None

def _get_bq_client():
    global _bq_client
    if _bq_client is None:
        creds, _ = load_credentials_from_file(WIF_CREDENTIAL_FILE)
        _bq_client = bigquery.Client(project=GCP_PROJECT_ID, credentials=creds)
    return _bq_client

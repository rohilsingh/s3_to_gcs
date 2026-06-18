import uuid
import logging
from datetime import datetime, timedelta, timezone

import boto3
from google.cloud import bigquery
from google.auth import load_credentials_from_file
from google.api_core.exceptions import GoogleAPIError

from src.config import (
    GCP_PROJECT_ID, WIF_CREDENTIAL_FILE, bq_table_ref,
    RUN_LOG_TABLE, SUCCESS_LOG_TABLE, ERROR_LOG_TABLE, ALERT_LOG_TABLE,
    RETRY_COOLDOWN_MINUTES, EVENTBRIDGE_BUS_NAME, EVENTBRIDGE_SOURCE,
)
from src.control_table_cache import get_all_source_buckets, get_pattern_by_file_id
from src.bq_logger import (
    insert_success_log, insert_error_log, insert_alert_log,
)
from src.s3_fallback import list_markers, delete_marker
from src.servicenow import create_incident, update_incident

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

_eb_client = boto3.client("events")


def handler(event, context):
    logger.info("Maintenance Lambda started")
    _reconcile_fallback_markers()
    _advance_waiting()
    _retry_failed()
    _gap_detection()
    _servicenow_alerts()
    logger.info("Maintenance Lambda complete")


# ── Emit EventBridge event so Transfer Lambda handles the actual copy ────
def _emit_transfer_event(
    source_bucket: str, s3_key: str, last_modified: str,
    etag: str, size_bytes: int = 0,
):
    import json
    entry = {
        "Source": EVENTBRIDGE_SOURCE,
        "DetailType": "MaintenanceRetry",
        "Detail": json.dumps({
            "bucket": {"name": source_bucket},
            "object": {
                "key": s3_key,
                "etag": etag,
                "size": size_bytes,
            },
        }),
        "Time": last_modified,
        "EventBusName": EVENTBRIDGE_BUS_NAME,
    }
    resp = _eb_client.put_events(Entries=[entry])
    failed = resp.get("FailedEntryCount", 0)
    if failed:
        logger.error("Failed to emit EventBridge event for %s: %s", s3_key, resp)
    else:
        logger.info("Emitted EventBridge retry event for %s", s3_key)
    return failed == 0


# ── PF-5: Reconcile S3 fallback markers into BQ ─────────────────────────
def _reconcile_fallback_markers():
    all_buckets = get_all_source_buckets()

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
                step=marker.get("step", "reconciled_from_s3_marker"),
            )
            if ok:
                delete_marker(bucket, marker["_marker_key"])

    logger.info("Fallback marker reconciliation complete")


# ── Advance WAITING files whose predecessors are now done ────────────────
def _advance_waiting():
    client = _get_bq_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=RETRY_COOLDOWN_MINUTES)).isoformat()

    query = f"""
        SELECT e.file_id, e.s3_key, e.last_modified, e.etag
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

    emitted = 0
    for row in rows:
        pattern = get_pattern_by_file_id(row.file_id)
        if not pattern:
            logger.warning("No pattern for file_id=%d, skipping", row.file_id)
            continue
        lm_str = row.last_modified.isoformat() if hasattr(row.last_modified, 'isoformat') else str(row.last_modified)
        if _emit_transfer_event(
            source_bucket=pattern["source_bucket_name"],
            s3_key=row.s3_key,
            last_modified=lm_str,
            etag=row.etag or "",
        ):
            emitted += 1

    logger.info("Advance-WAITING complete, emitted %d / %d events", emitted, len(rows))


# ── Retry FAILED files (non-waiting, last attempt > 20 min ago) ──────────
def _retry_failed():
    client = _get_bq_client()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=RETRY_COOLDOWN_MINUTES)).isoformat()

    query = f"""
        SELECT e.file_id, e.s3_key, e.last_modified, e.etag
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

    emitted = 0
    for row in rows:
        pattern = get_pattern_by_file_id(row.file_id)
        if not pattern:
            logger.warning("No pattern for file_id=%d, skipping", row.file_id)
            continue
        lm_str = row.last_modified.isoformat() if hasattr(row.last_modified, 'isoformat') else str(row.last_modified)
        if _emit_transfer_event(
            source_bucket=pattern["source_bucket_name"],
            s3_key=row.s3_key,
            last_modified=lm_str,
            etag=row.etag or "",
        ):
            emitted += 1

    logger.info("Retry-FAILED complete, emitted %d / %d events", emitted, len(rows))


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


# ── BQ client ────────────────────────────────────────────────────────────
_bq_client = None

def _get_bq_client():
    global _bq_client
    if _bq_client is None:
        creds, _ = load_credentials_from_file(WIF_CREDENTIAL_FILE)
        _bq_client = bigquery.Client(project=GCP_PROJECT_ID, credentials=creds)
    return _bq_client

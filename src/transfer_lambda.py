import uuid
import logging
from datetime import datetime, timezone

from src.config import bq_table_ref
from src.control_table_cache import match_pattern
from src.gcs_key import build_gcs_key
from src.bq_logger import (
    insert_run_log, insert_success_log, insert_error_log,
    check_success_exists, get_latest_attempt,
)
from src.s3_fallback import (
    write_success_marker, write_failure_marker,
    check_success_marker_exists,
)
from src.streaming_copy import get_s3_stream, stream_to_gcs
from src.predecessor import resolve_predecessor, is_predecessor_done
from src.archival import self_archive

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def handler(event, context):
    for record in _extract_records(event):
        try:
            _process_record(record)
        except Exception:
            logger.exception("Unhandled error processing record: %s", record)


def _extract_records(event: dict) -> list[dict]:
    if "detail" in event:
        detail = event["detail"]
        bucket = detail.get("bucket", {}).get("name", "")
        key = detail.get("object", {}).get("key", "")
        last_modified = event.get("time", "")
        etag = detail.get("object", {}).get("etag", "")
        size = detail.get("object", {}).get("size", 0)
        return [{
            "source_bucket": bucket,
            "s3_key": key,
            "last_modified": last_modified,
            "etag": etag,
            "size_bytes": size,
        }]

    if "Records" in event:
        records = []
        for r in event["Records"]:
            s3_info = r.get("s3", {})
            records.append({
                "source_bucket": s3_info.get("bucket", {}).get("name", ""),
                "s3_key": s3_info.get("object", {}).get("key", ""),
                "last_modified": r.get("eventTime", ""),
                "etag": s3_info.get("object", {}).get("eTag", ""),
                "size_bytes": s3_info.get("object", {}).get("size", 0),
            })
        return records

    return []


def _process_record(record: dict):
    source_bucket = record["source_bucket"]
    s3_key = record["s3_key"]
    last_modified = record["last_modified"]
    etag = record["etag"]
    size_bytes = record["size_bytes"]

    pattern = match_pattern(source_bucket, s3_key)
    if pattern is None:
        logger.debug("No matching pattern for %s/%s, ignoring", source_bucket, s3_key)
        return

    file_id = pattern["file_id"]
    gcs_key = build_gcs_key(s3_key, pattern)
    gcs_bucket = pattern["destination_bucket_name"]
    archive_bucket = pattern.get("archive_to_bucket", "")
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    attempt = get_latest_attempt(file_id, s3_key, last_modified, etag) + 1

    pred_info = resolve_predecessor(
        file_id, s3_key,
        datetime.fromisoformat(last_modified) if isinstance(last_modified, str) else last_modified,
        pattern,
    )
    pred_s3_key = pred_info["predecessor_s3_key"] if pred_info else None
    pred_last_modified = pred_info["predecessor_last_modified"] if pred_info else None

    bq_ok = insert_run_log(
        run_id=run_id, file_id=file_id, s3_key=s3_key,
        last_modified=last_modified, etag=etag, size_bytes=size_bytes,
        gcs_key=gcs_key, attempt=attempt, trigger_source="eventbridge",
        predecessor_s3_key=pred_s3_key,
        predecessor_last_modified=pred_last_modified,
        started_at=started_at,
    )

    if check_success_exists(file_id, s3_key, last_modified, etag):
        logger.info("Already succeeded (BQ), skipping: %s", s3_key)
        return
    if check_success_marker_exists(source_bucket, file_id, s3_key, last_modified, etag):
        logger.info("Already succeeded (S3 marker), skipping: %s", s3_key)
        return

    if pred_info and not is_predecessor_done(
        file_id, pred_s3_key, pred_last_modified, source_bucket
    ):
        logger.info("Predecessor not done, marking WAITING: %s", s3_key)
        ok = insert_error_log(
            run_id=run_id, file_id=file_id, s3_key=s3_key,
            last_modified=last_modified, etag=etag, gcs_key=gcs_key,
            attempt=attempt, error_class="ORDER_BLOCKED",
            error_detail=f"Waiting on predecessor: {pred_s3_key}",
            waiting=True, retryable=True,
            predecessor_s3_key=pred_s3_key,
            predecessor_last_modified=pred_last_modified,
            step="transfer_lambda._process_record.check_predecessor",
        )
        if not ok:
            write_failure_marker(
                source_bucket, run_id, file_id, s3_key,
                last_modified, etag, gcs_key,
                "ORDER_BLOCKED",
                f"Waiting on predecessor: {pred_s3_key}",
                step="transfer_lambda._process_record.check_predecessor",
            )
        return

    try:
        lm_dt = (datetime.fromisoformat(last_modified)
                 if isinstance(last_modified, str) else last_modified)
        s3_stream, actual_size, read_from_archive = get_s3_stream(
            source_bucket, s3_key, archive_bucket, lm_dt,
        )
    except FileNotFoundError as e:
        _log_error(
            run_id, file_id, s3_key, last_modified, etag, gcs_key,
            attempt, "S3_READ", str(e), True, source_bucket, bq_ok,
            pred_s3_key, pred_last_modified,
            step="transfer_lambda._process_record.get_s3_stream",
        )
        return
    except Exception as e:
        _log_error(
            run_id, file_id, s3_key, last_modified, etag, gcs_key,
            attempt, "S3_READ", str(e), True, source_bucket, bq_ok,
            pred_s3_key, pred_last_modified,
            step="transfer_lambda._process_record.get_s3_stream",
        )
        return

    try:
        bytes_copied = stream_to_gcs(s3_stream, gcs_bucket, gcs_key, actual_size)
    except Exception as e:
        _log_error(
            run_id, file_id, s3_key, last_modified, etag, gcs_key,
            attempt, "GCS_WRITE", str(e), True, source_bucket, bq_ok,
            pred_s3_key, pred_last_modified,
            step="transfer_lambda._process_record.stream_to_gcs",
        )
        return

    archived = False
    if pattern.get("archive_after_copy_enabled") and archive_bucket:
        try:
            self_archive(source_bucket, s3_key, archive_bucket, lm_dt)
            archived = True
        except Exception as e:
            _log_error(
                run_id, file_id, s3_key, last_modified, etag, gcs_key,
                attempt, "ARCHIVE", str(e), True, source_bucket, bq_ok,
                pred_s3_key, pred_last_modified,
                step="transfer_lambda._process_record.self_archive",
            )
            return

    ok = insert_success_log(
        run_id=run_id, file_id=file_id, s3_key=s3_key,
        last_modified=last_modified, etag=etag, gcs_key=gcs_key,
        bytes_copied=bytes_copied, archived=archived,
    )
    if not ok:
        write_success_marker(
            source_bucket, run_id, file_id, s3_key,
            last_modified, etag, gcs_key, bytes_copied, archived,
        )

    logger.info(
        "Transfer complete: %s -> gs://%s/%s (%d bytes, archived=%s)",
        s3_key, gcs_bucket, gcs_key, bytes_copied, archived,
    )


def _log_error(
    run_id, file_id, s3_key, last_modified, etag, gcs_key,
    attempt, error_class, error_detail, retryable,
    source_bucket, bq_ok, pred_s3_key=None, pred_last_modified=None,
    step=None,
):
    logger.error("[%s] %s error for %s: %s", step, error_class, s3_key, error_detail)
    ok = insert_error_log(
        run_id=run_id, file_id=file_id, s3_key=s3_key,
        last_modified=last_modified, etag=etag, gcs_key=gcs_key,
        attempt=attempt, error_class=error_class,
        error_detail=error_detail, retryable=retryable,
        predecessor_s3_key=pred_s3_key,
        predecessor_last_modified=pred_last_modified,
        step=step,
    )
    if not ok:
        write_failure_marker(
            source_bucket, run_id, file_id, s3_key,
            last_modified, etag, gcs_key, error_class, error_detail,
            step=step,
        )

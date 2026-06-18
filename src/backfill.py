import uuid
import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from src.config import BUCKET_SHORT_NAMES
from src.control_table_cache import get_cache
from src.gcs_key import build_gcs_key
from src.bq_logger import (
    insert_run_log, insert_success_log, insert_error_log,
    check_success_exists,
)
from src.streaming_copy import get_s3_stream, stream_to_gcs
from src.predecessor import resolve_predecessor, is_predecessor_done
from src.archival import self_archive

logger = logging.getLogger(__name__)
_s3 = boto3.client("s3")


def replay_single(file_id: int, s3_key: str, last_modified: str, etag: str):
    cache = get_cache()
    pattern = _find_pattern(cache, file_id)
    if not pattern:
        logger.error("No pattern found for file_id=%d", file_id)
        return

    _do_transfer(pattern, s3_key, last_modified, etag)


def replay_range(file_id: int, start_date: str, end_date: str):
    cache = get_cache()
    pattern = _find_pattern(cache, file_id)
    if not pattern:
        logger.error("No pattern found for file_id=%d", file_id)
        return

    source_bucket = pattern["source_bucket_name"]
    regex = pattern["regex"]
    prefix = ""

    objects = _list_matching_objects(
        source_bucket, prefix, regex, start_date, end_date,
    )

    archive_bucket = pattern.get("archive_to_bucket", "")
    if archive_bucket:
        short = BUCKET_SHORT_NAMES.get(source_bucket, source_bucket)
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
        from datetime import timedelta
        dt = start_dt
        while dt <= end_dt:
            archive_prefix = f"{dt.strftime('%y%m%d')}/{short}/"
            archive_objects = _list_matching_objects(
                archive_bucket, archive_prefix, regex, start_date, end_date,
                strip_prefix_parts=2,
            )
            objects.extend(archive_objects)
            dt += timedelta(days=1)

    objects.sort(key=lambda o: o["last_modified"])

    logger.info("Replaying %d files for file_id=%d in range [%s, %s]",
                len(objects), file_id, start_date, end_date)

    for obj in objects:
        _do_transfer(pattern, obj["key"], obj["last_modified"], obj["etag"])


def backfill_handler(event, context):
    action = event.get("action", "single")
    file_id = event["file_id"]

    if action == "single":
        replay_single(file_id, event["s3_key"], event["last_modified"], event["etag"])
    elif action == "range":
        replay_range(file_id, event["start_date"], event["end_date"])
    else:
        logger.error("Unknown backfill action: %s", action)


def _find_pattern(cache: dict, file_id: int):
    for patterns in cache.values():
        for p in patterns:
            if p["file_id"] == file_id:
                return p
    return None


def _list_matching_objects(
    bucket: str, prefix: str, regex, start_date: str, end_date: str,
    strip_prefix_parts: int = 0,
) -> list[dict]:
    objects = []
    paginator = _s3.get_paginator("list_objects_v2")
    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.fromisoformat(end_date)

    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                lm = obj["LastModified"]
                if not (start_dt <= lm <= end_dt):
                    continue
                key = obj["Key"]
                if strip_prefix_parts:
                    parts = key.split("/", strip_prefix_parts)
                    key = parts[-1] if len(parts) > strip_prefix_parts else key
                if regex.fullmatch(key):
                    objects.append({
                        "key": key,
                        "last_modified": lm.isoformat(),
                        "etag": obj.get("ETag", "").strip('"'),
                    })
    except ClientError:
        logger.exception("Failed listing s3://%s/%s", bucket, prefix)

    return objects


def _do_transfer(pattern: dict, s3_key: str, last_modified: str, etag: str):
    file_id = pattern["file_id"]
    source_bucket = pattern["source_bucket_name"]
    gcs_bucket = pattern["destination_bucket_name"]
    archive_bucket = pattern.get("archive_to_bucket", "")
    gcs_key = build_gcs_key(s3_key, pattern)
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()

    if check_success_exists(file_id, s3_key, last_modified, etag):
        logger.info("Already succeeded, skipping backfill: %s", s3_key)
        return

    pred_info = resolve_predecessor(
        file_id, s3_key,
        datetime.fromisoformat(last_modified) if isinstance(last_modified, str) else last_modified,
        pattern,
    )
    pred_s3_key = pred_info["predecessor_s3_key"] if pred_info else None
    pred_last_modified = pred_info["predecessor_last_modified"] if pred_info else None

    if pred_info and not is_predecessor_done(
        file_id, pred_s3_key, pred_last_modified, source_bucket
    ):
        logger.info("Predecessor not done during backfill, marking WAITING: %s", s3_key)
        insert_error_log(
            run_id=run_id, file_id=file_id, s3_key=s3_key,
            last_modified=last_modified, etag=etag, gcs_key=gcs_key,
            attempt=1, error_class="ORDER_BLOCKED",
            error_detail=f"Backfill waiting on predecessor: {pred_s3_key}",
            waiting=True, retryable=True,
            predecessor_s3_key=pred_s3_key,
            predecessor_last_modified=pred_last_modified,
            step="backfill._do_transfer.check_predecessor",
        )
        return

    insert_run_log(
        run_id=run_id, file_id=file_id, s3_key=s3_key,
        last_modified=last_modified, etag=etag, size_bytes=0,
        gcs_key=gcs_key, attempt=1, trigger_source="maintenance",
        predecessor_s3_key=pred_s3_key,
        predecessor_last_modified=pred_last_modified,
        started_at=started_at,
    )

    try:
        lm_dt = datetime.fromisoformat(last_modified) if isinstance(last_modified, str) else last_modified
        s3_stream, actual_size, read_from_archive = get_s3_stream(
            source_bucket, s3_key, archive_bucket, lm_dt,
        )
    except Exception as e:
        insert_error_log(
            run_id=run_id, file_id=file_id, s3_key=s3_key,
            last_modified=last_modified, etag=etag, gcs_key=gcs_key,
            attempt=1, error_class="S3_READ", error_detail=str(e),
            retryable=True,
            step="backfill._do_transfer.get_s3_stream",
        )
        return

    try:
        bytes_copied = stream_to_gcs(s3_stream, gcs_bucket, gcs_key, actual_size)
    except Exception as e:
        insert_error_log(
            run_id=run_id, file_id=file_id, s3_key=s3_key,
            last_modified=last_modified, etag=etag, gcs_key=gcs_key,
            attempt=1, error_class="GCS_WRITE", error_detail=str(e),
            retryable=True,
            step="backfill._do_transfer.stream_to_gcs",
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
                attempt=1, error_class="ARCHIVE", error_detail=str(e),
                retryable=True,
                step="backfill._do_transfer.self_archive",
            )
            return

    insert_success_log(
        run_id=run_id, file_id=file_id, s3_key=s3_key,
        last_modified=last_modified, etag=etag, gcs_key=gcs_key,
        bytes_copied=bytes_copied, archived=archived,
    )

    logger.info("Backfill transfer complete: %s -> gs://%s/%s", s3_key, gcs_bucket, gcs_key)

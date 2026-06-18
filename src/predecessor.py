import re
import logging
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.exceptions import ClientError

from src.config import BUCKET_SHORT_NAMES, FALLBACK_PREFIX
from src.bq_logger import check_predecessor_success, check_run_log_exists
from src.s3_fallback import check_predecessor_success_marker

logger = logging.getLogger(__name__)
_s3 = boto3.client("s3")


def _extract_literal_prefix(pattern_str: str) -> str:
    prefix = []
    i = 0
    while i < len(pattern_str):
        c = pattern_str[i]
        if c in r"\.^$*+?{}[]|()":
            break
        prefix.append(c)
        i += 1
    return "".join(prefix)


def _list_objects_with_prefix(bucket: str, prefix: str) -> list[dict]:
    objects = []
    try:
        paginator = _s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                objects.append({
                    "key": obj["Key"],
                    "last_modified": obj["LastModified"],
                    "etag": obj.get("ETag", "").strip('"'),
                })
    except ClientError:
        logger.exception("Failed listing s3://%s/%s", bucket, prefix)
    return objects


def _find_predecessor_in_objects(
    objects: list[dict], regex: re.Pattern, current_last_modified: datetime,
) -> Optional[dict]:
    candidates = []
    for obj in objects:
        if regex.fullmatch(obj["key"]) and obj["last_modified"] < current_last_modified:
            candidates.append(obj)
    if not candidates:
        return None
    candidates.sort(key=lambda o: o["last_modified"], reverse=True)
    return candidates[0]


def resolve_predecessor(
    file_id: int, s3_key: str, last_modified: datetime,
    pattern: dict,
) -> Optional[dict]:
    source_bucket = pattern["source_bucket_name"]
    archive_bucket = pattern.get("archive_to_bucket")
    regex = pattern["regex"]
    literal_prefix = _extract_literal_prefix(pattern["file_path_pattern"])

    all_objects = []

    all_objects.extend(
        _list_objects_with_prefix(source_bucket, literal_prefix)
    )

    if archive_bucket:
        short = BUCKET_SHORT_NAMES.get(source_bucket, source_bucket)
        for delta in range(-1, 2):
            from datetime import timedelta
            dt = last_modified + timedelta(days=delta)
            archive_prefix = f"{dt.strftime('%y%m%d')}/{short}/{literal_prefix}"
            for obj in _list_objects_with_prefix(archive_bucket, archive_prefix):
                parts = obj["key"].split("/", 2)
                if len(parts) >= 3:
                    obj["key"] = parts[2]
                all_objects.extend([obj])

        fallback_prefix = f"{FALLBACK_PREFIX}/success/"
        for obj in _list_objects_with_prefix(source_bucket, fallback_prefix):
            pass

    predecessor = _find_predecessor_in_objects(all_objects, regex, last_modified)
    if predecessor:
        return {
            "predecessor_s3_key": predecessor["key"],
            "predecessor_last_modified": predecessor["last_modified"].isoformat(),
        }
    return None


def is_predecessor_done(
    file_id: int, pred_s3_key: str, pred_last_modified: str,
    source_bucket: str,
) -> bool:
    if check_predecessor_success_marker(
        source_bucket, file_id, pred_s3_key, pred_last_modified
    ):
        return True

    if check_predecessor_success(file_id, pred_s3_key, pred_last_modified):
        return True

    if not check_run_log_exists(file_id, pred_s3_key, pred_last_modified):
        logger.info(
            "Predecessor %s (last_modified=%s) has no run_log entry — "
            "predates our system, treating as done",
            pred_s3_key, pred_last_modified,
        )
        return True

    return False

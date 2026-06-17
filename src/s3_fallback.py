import json
import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from src.config import FALLBACK_PREFIX

logger = logging.getLogger(__name__)
_s3 = boto3.client("s3")


def _marker_key(kind: str, run_id: str) -> str:
    return f"{FALLBACK_PREFIX}/{kind}/{run_id}.json"


def write_success_marker(
    bucket: str, run_id: str, file_id: int, s3_key: str,
    last_modified: str, etag: str, gcs_key: str,
    bytes_copied: int, archived: bool,
):
    body = {
        "run_id": run_id,
        "file_id": file_id,
        "s3_key": s3_key,
        "last_modified": last_modified,
        "etag": etag,
        "gcs_key": gcs_key,
        "bytes_copied": bytes_copied,
        "archived": archived,
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    key = _marker_key("success", run_id)
    _s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(body).encode())
    logger.info("Wrote success marker: s3://%s/%s", bucket, key)


def write_failure_marker(
    bucket: str, run_id: str, file_id: int, s3_key: str,
    last_modified: str, etag: str, gcs_key: str,
    error_class: str, error_detail: str,
):
    body = {
        "run_id": run_id,
        "file_id": file_id,
        "s3_key": s3_key,
        "last_modified": last_modified,
        "etag": etag,
        "gcs_key": gcs_key,
        "error_class": error_class,
        "error_detail": error_detail,
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    key = _marker_key("failure", run_id)
    _s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(body).encode())
    logger.info("Wrote failure marker: s3://%s/%s", bucket, key)


def list_markers(bucket: str, kind: str) -> list[dict]:
    prefix = f"{FALLBACK_PREFIX}/{kind}/"
    markers = []
    paginator = _s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            try:
                resp = _s3.get_object(Bucket=bucket, Key=obj["Key"])
                body = json.loads(resp["Body"].read().decode())
                body["_marker_key"] = obj["Key"]
                markers.append(body)
            except (ClientError, json.JSONDecodeError):
                logger.exception("Failed to read marker %s", obj["Key"])
    return markers


def delete_marker(bucket: str, marker_key: str):
    _s3.delete_object(Bucket=bucket, Key=marker_key)
    logger.info("Deleted marker: s3://%s/%s", bucket, marker_key)


def check_success_marker_exists(
    bucket: str, file_id: int, s3_key: str, last_modified: str, etag: str,
) -> bool:
    markers = list_markers(bucket, "success")
    for m in markers:
        if (m.get("file_id") == file_id
                and m.get("s3_key") == s3_key
                and m.get("last_modified") == last_modified
                and m.get("etag") == etag):
            return True
    return False


def check_predecessor_success_marker(
    bucket: str, file_id: int, s3_key: str, last_modified: str,
) -> bool:
    markers = list_markers(bucket, "success")
    for m in markers:
        if (m.get("file_id") == file_id
                and m.get("s3_key") == s3_key
                and m.get("last_modified") == last_modified):
            return True
    return False

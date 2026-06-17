import logging
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

from src.config import BUCKET_SHORT_NAMES

logger = logging.getLogger(__name__)
_s3 = boto3.client("s3")


def self_archive(
    source_bucket: str, s3_key: str,
    archive_bucket: str, last_modified: datetime,
):
    yymmdd = last_modified.strftime("%y%m%d")
    short = BUCKET_SHORT_NAMES.get(source_bucket, source_bucket)
    archive_key = f"{yymmdd}/{short}/{s3_key}"

    try:
        _s3.head_object(Bucket=source_bucket, Key=s3_key)
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.info("Source already gone, skipping self-archive: %s", s3_key)
            return True
        raise

    _s3.copy_object(
        Bucket=archive_bucket,
        Key=archive_key,
        CopySource={"Bucket": source_bucket, "Key": s3_key},
    )
    logger.info("Archived s3://%s/%s -> s3://%s/%s",
                source_bucket, s3_key, archive_bucket, archive_key)

    _s3.delete_object(Bucket=source_bucket, Key=s3_key)
    logger.info("Deleted source: s3://%s/%s", source_bucket, s3_key)
    return True

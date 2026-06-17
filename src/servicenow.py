import json
import logging
from typing import Optional

import boto3
import requests
from botocore.exceptions import ClientError

from src.config import SERVICENOW_SECRET_NAME, SERVICENOW_INSTANCE_URL

logger = logging.getLogger(__name__)


def _get_credentials() -> tuple[str, str]:
    sm = boto3.client("secretsmanager")
    resp = sm.get_secret_value(SecretId=SERVICENOW_SECRET_NAME)
    secret = json.loads(resp["SecretString"])
    return secret["username"], secret["password"]


def create_incident(
    file_id: int, s3_key: str, last_modified: str,
    error_class: str, error_detail: str,
) -> Optional[str]:
    if not SERVICENOW_INSTANCE_URL:
        logger.warning("ServiceNow instance URL not configured, skipping incident creation")
        return None

    username, password = _get_credentials()
    url = f"{SERVICENOW_INSTANCE_URL}/api/now/table/incident"

    payload = {
        "short_description": (
            f"S3-GCS transfer failure: file_id={file_id} key={s3_key}"
        ),
        "description": (
            f"File transfer stuck.\n"
            f"file_id: {file_id}\n"
            f"s3_key: {s3_key}\n"
            f"last_modified: {last_modified}\n"
            f"error_class: {error_class}\n"
            f"error_detail: {error_detail}"
        ),
        "urgency": "2",
        "impact": "2",
    }

    resp = requests.post(
        url,
        json=payload,
        auth=(username, password),
        headers={"Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json().get("result", {})
    incident_id = result.get("sys_id", "")
    logger.info("Created ServiceNow incident %s for file_id=%d", incident_id, file_id)
    return incident_id


def update_incident(
    incident_id: str, file_id: int, s3_key: str,
    last_modified: str,
) -> bool:
    if not SERVICENOW_INSTANCE_URL:
        return False

    username, password = _get_credentials()
    url = f"{SERVICENOW_INSTANCE_URL}/api/now/table/incident/{incident_id}"

    payload = {
        "work_notes": (
            f"Daily reminder: file_id={file_id} s3_key={s3_key} "
            f"last_modified={last_modified} still unresolved."
        ),
    }

    resp = requests.patch(
        url,
        json=payload,
        auth=(username, password),
        headers={"Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    logger.info("Updated ServiceNow incident %s for file_id=%d", incident_id, file_id)
    return True

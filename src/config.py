import os

ENV = os.environ.get("env", "dev")
GCP_PROJECT_ID = os.environ["gcp_project_id"]
BIGQUERY_DATASET = os.environ["bigquery_dataset"]

CONTROL_TABLE = os.environ.get("control_table", "control_table")
RUN_LOG_TABLE = os.environ.get("run_log_table", "run_log")
SUCCESS_LOG_TABLE = os.environ.get("success_log_table", "success_log")
ERROR_LOG_TABLE = os.environ.get("error_log_table", "error_log")
ALERT_LOG_TABLE = os.environ.get("alert_log_table", "alert_log")

CACHE_TTL_SECONDS = int(os.environ.get("cache_ttl_seconds", "600"))
RETRY_COOLDOWN_MINUTES = int(os.environ.get("retry_cooldown_minutes", "20"))

FALLBACK_PREFIX = os.environ.get("fallback_prefix", "_transfer_fallback")

SERVICENOW_SECRET_NAME = os.environ.get("servicenow_secret_name", "servicenow-credentials")
SERVICENOW_INSTANCE_URL = os.environ.get("servicenow_instance_url", "")

WIF_CREDENTIAL_FILE = os.environ.get("wif_credential_file", "wif.json")

BUCKET_SHORT_NAMES = {
    "<source-bucket-1>": "src1",
    "<source-bucket-2>": "src2",
}

STREAM_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


def bq_table_ref(table_name: str) -> str:
    return f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

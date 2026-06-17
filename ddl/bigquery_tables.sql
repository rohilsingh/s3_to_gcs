-- =============================================================================
-- S3 -> GCS File Transfer — BigQuery DDL
-- Replace ${PROJECT_ID} and ${DATASET} before running.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. control_table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET}.control_table` (
    file_id         INT64         NOT NULL,
    file_path_pattern       STRING  NOT NULL,
    source_bucket_name      STRING  NOT NULL,
    destination_bucket_name STRING  NOT NULL,
    destination_base        STRING,
    destination_prefix_base STRING,
    archive_after_copy_enabled BOOL NOT NULL DEFAULT FALSE,
    archive_to_bucket       STRING,
    enabled                 BOOL   NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- 2. run_log — one row per sighting / attempt (append-only)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET}.run_log` (
    run_id                    STRING    NOT NULL,
    file_id                   INT64     NOT NULL,
    s3_key                    STRING    NOT NULL,
    last_modified             TIMESTAMP NOT NULL,
    etag                      STRING,
    size_bytes                INT64,
    gcs_key                   STRING,
    attempt                   INT64     NOT NULL DEFAULT 1,
    trigger_source            STRING    NOT NULL,  -- 'eventbridge' | 'maintenance'
    read_from_archive         BOOL      NOT NULL DEFAULT FALSE,
    predecessor_s3_key        STRING,
    predecessor_last_modified TIMESTAMP,
    started_at                TIMESTAMP NOT NULL,
    inserted_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- 3. success_log — one row per successful copy or manual override (append-only)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET}.success_log` (
    run_id        STRING    NOT NULL,  -- 'manual' sentinel for overrides
    file_id       INT64     NOT NULL,
    s3_key        STRING    NOT NULL,
    last_modified TIMESTAMP NOT NULL,
    etag          STRING,
    gcs_key       STRING,
    bytes_copied  INT64,
    archived      BOOL      NOT NULL DEFAULT FALSE,
    is_manual     BOOL      NOT NULL DEFAULT FALSE,
    resolved_by   STRING,
    finished_at   TIMESTAMP,
    inserted_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- 4. error_log — one row per failure or waiting event (append-only)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET}.error_log` (
    run_id                    STRING    NOT NULL,
    file_id                   INT64     NOT NULL,
    s3_key                    STRING    NOT NULL,
    last_modified             TIMESTAMP NOT NULL,
    etag                      STRING,
    gcs_key                   STRING,
    attempt                   INT64     NOT NULL DEFAULT 1,
    waiting                   BOOL      NOT NULL DEFAULT FALSE,
    error_class               STRING    NOT NULL,  -- S3_READ | GCS_WRITE | BQ | ARCHIVE | CONFIG | ORDER_BLOCKED
    error_detail              STRING,
    predecessor_s3_key        STRING,
    predecessor_last_modified TIMESTAMP,
    retryable                 BOOL      NOT NULL DEFAULT TRUE,
    inserted_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------------
-- 5. alert_log — ServiceNow dedup tracking (append-only)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET}.alert_log` (
    incident_run_id        STRING    NOT NULL,
    file_id                INT64     NOT NULL,
    s3_key                 STRING    NOT NULL,
    last_modified          TIMESTAMP NOT NULL,
    servicenow_incident_id STRING,
    action                 STRING    NOT NULL,  -- 'created' | 'reminder'
    alerted_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

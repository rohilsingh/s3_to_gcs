# S3 → GCS File Transfer Service

**Owner:** Rohil · **Status:** Production · **Last Updated:** 2026-06-18

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [How It Works](#how-it-works)
4. [Components](#components)
5. [Control Table Configuration](#control-table-configuration)
6. [GCS Key Construction](#gcs-key-construction)
7. [Ordering & Blocking](#ordering--blocking)
8. [Source Archival](#source-archival)
9. [Logging & Observability](#logging--observability)
10. [Error Handling & Retries](#error-handling--retries)
11. [ServiceNow Alerting](#servicenow-alerting)
12. [Backfill / Replay Tool](#backfill--replay-tool)
13. [Environment Configuration](#environment-configuration)
14. [Manual Operations Runbook](#manual-operations-runbook)
15. [Troubleshooting](#troubleshooting)
16. [BigQuery Table Reference](#bigquery-table-reference)

---

## Overview

The S3 → GCS File Transfer Service is an event-driven, serverless pipeline that streams files from two source S3 buckets to Google Cloud Storage (GCS). It is governed by a BigQuery control table and provides:

- **Pattern-based routing** — each source bucket/key combination is matched against configurable regex patterns
- **Strict per-pattern ordering** — files within a pattern are processed in `LastModified` order, each blocking on its predecessor
- **Streaming copy** — low memory/disk footprint; files are never fully buffered
- **Archive race protection** — if a source file is archived before the Lambda runs, the service reads from the archive bucket instead
- **Append-only audit trail** — every attempt, success, and failure is recorded in BigQuery
- **Automatic retries** — a scheduled Maintenance Lambda retries failures and advances blocked files
- **ServiceNow escalation** — persistent failures generate ServiceNow incidents with daily dedup
- **Backfill/replay** — re-drive individual files or date ranges through the pipeline

---

## Architecture

```
┌──────────────────────┐
│  S3 Source Buckets    │
│  (Bucket 1, Bucket 2)│
└──────────┬───────────┘
           │ EventBridge (Object Created)
           ▼
┌──────────────────────┐         ┌──────────────────┐
│   Transfer Lambda    │────────▶│   Google Cloud    │
│                      │ stream  │   Storage (GCS)   │
│  • Match pattern     │         └──────────────────┘
│  • Check ordering    │
│  • Stream copy       │         ┌──────────────────┐
│  • Log to BigQuery   │────────▶│   BigQuery        │
│  • Self-archive      │         │  (control_table,  │
└──────────────────────┘         │   run_log,        │
                                 │   success_log,    │
┌──────────────────────┐         │   error_log,      │
│  EventBridge         │         │   alert_log)      │
│  Scheduler (10 min)  │         └──────────────────┘
└──────────┬───────────┘
           ▼                     ┌──────────────────┐
┌──────────────────────┐         │   ServiceNow     │
│  Maintenance Lambda  │────────▶│  (incidents)      │
│                      │         └──────────────────┘
│  • Reconcile markers │
│  • Advance WAITING   │         ┌──────────────────┐
│  • Retry FAILED      │         │   AWS Secrets     │
│  • Gap detection     │         │   Manager         │
│  • ServiceNow alerts │         │  (ServiceNow      │
└──────────────────────┘         │   credentials)    │
                                 └──────────────────┘
```

**Authentication:**
- GCS + BigQuery: Workload Identity Federation (`wif.json`)
- ServiceNow: Username/password from AWS Secrets Manager

---

## How It Works

### Transfer Lambda (event-driven)

When a new file lands in a source S3 bucket, EventBridge triggers the Transfer Lambda. The Lambda:

1. **Matches the S3 key** against enabled control table patterns for that bucket
2. **Writes a `run_log` row** ("seen" — this attempt is tracked)
3. **Checks the double-copy guard** — if this file identity is already in `success_log` or has an S3 success marker, it skips (idempotent)
4. **Resolves the predecessor** — finds the previous file for the same pattern (by `LastModified`) by scanning the source bucket, archive bucket, and fallback markers
5. **Checks predecessor status** — if the predecessor isn't done, marks this file as `WAITING` in `error_log` and stops
6. **Streams the file** from S3 → GCS using chunked reads (8 MB chunks)
7. **Logs success** to `success_log` in BigQuery
8. **Self-archives** the source file (if enabled for this pattern)

### Maintenance Lambda (every 10 minutes)

A scheduled Lambda that runs five maintenance tasks each cycle:

| Task | Description |
|------|-------------|
| **Reconcile fallback markers** | Reads S3 fallback markers (written when BQ was down), inserts them into BQ, deletes the markers |
| **Advance WAITING files** | Re-checks predecessors for WAITING files; if predecessor is now done, retries the transfer |
| **Retry FAILED files** | Retries failed transfers whose last attempt was >20 minutes ago |
| **Gap detection** | Identifies WAITING files whose predecessors have never been seen (no `run_log` row) |
| **ServiceNow alerts** | Creates/refreshes incidents for persistently failed files (daily dedup) |

---

## Components

| File | Purpose |
|------|---------|
| `src/config.py` | Environment variables, constants, table references |
| `src/control_table_cache.py` | In-memory BQ control table cache with TTL and per-bucket regex index |
| `src/gcs_key.py` | GCS destination key construction |
| `src/bq_logger.py` | Append-only BigQuery logging (all 4 log tables) |
| `src/s3_fallback.py` | S3 fallback marker read/write when BQ is unreachable |
| `src/streaming_copy.py` | Streaming S3 → GCS transfer with archive fallback |
| `src/predecessor.py` | S3-first predecessor resolution and status checking |
| `src/archival.py` | Self-archive source file (copy to archive bucket + delete source) |
| `src/servicenow.py` | ServiceNow incident creation and refresh |
| `src/transfer_lambda.py` | **Transfer Lambda handler** — `src.transfer_lambda.handler` |
| `src/maintenance_lambda.py` | **Maintenance Lambda handler** — `src.maintenance_lambda.handler` |
| `src/backfill.py` | **Backfill/Replay tool** — `src.backfill.backfill_handler` |
| `ddl/bigquery_tables.sql` | BigQuery DDL for all 5 tables |

---

## Control Table Configuration

The control table lives in BigQuery at `{gcp_project_id}.{bigquery_dataset}.control_table` and defines which files to transfer and where.

### Columns

| Column | Type | Description |
|--------|------|-------------|
| `file_id` | INT64 | Unique pattern identifier |
| `file_path_pattern` | STRING | Regex matched against the full S3 key (uses `fullmatch`) |
| `source_bucket_name` | STRING | Which source bucket this pattern applies to |
| `destination_bucket_name` | STRING | Target GCS bucket |
| `destination_base` | STRING | GCS key base path (NULL = derive from first segment of S3 key) |
| `destination_prefix_base` | STRING | Optional prefix prepended to GCS key (NULL = no prefix) |
| `archive_after_copy_enabled` | BOOL | Whether to self-archive the source file after successful transfer |
| `archive_to_bucket` | STRING | Archive bucket name (also used for fallback reads) |
| `enabled` | BOOL | Only `TRUE` rows are considered |
| `created_at` | TIMESTAMP | Row creation time |
| `updated_at` | TIMESTAMP | Last modification time |

### Rules

- A `(source_bucket, key)` combination must match **exactly one** enabled pattern
- **Zero matches** → file is silently ignored
- **Multiple matches** → error + alert (this is a configuration bug)
- Patterns are **cached in-memory** for ~10 minutes (configurable via `cache_ttl_seconds`)
- Only `enabled = TRUE` rows are loaded

### Example Rows

```sql
-- Bucket 1: Fixed report names
INSERT INTO control_table VALUES (
  1,
  'data/reports/\\d{4}_\\d{2}_\\d{2}/sales_summary\\.csv',
  'source-bucket-1',
  'dest-gcs-bucket',
  'reports/',          -- destination_base
  'ingested/',         -- destination_prefix_base
  TRUE,                -- archive_after_copy_enabled
  'archive-bucket',
  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- Bucket 2: Tenant data (destination_base derived from first segment)
INSERT INTO control_table VALUES (
  2,
  '[a-z0-9]+/[a-z0-9]+/transactions/daily/\\d{4}_\\d{2}_\\d{2}/.*\\.csv',
  'source-bucket-2',
  'dest-gcs-bucket-2',
  NULL,                -- derived from first segment of s3_key
  NULL,                -- no prefix
  FALSE,               -- don't self-archive
  'archive-bucket-2',
  TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);
```

---

## GCS Key Construction

The GCS destination key is built from three parts:

```
gcs_key = destination_prefix_base + destination_base + remainder
```

Where:
- **`destination_prefix_base`** = value from control table (normalized with trailing `/`), or empty if NULL
- **`destination_base`** = value from control table, OR first segment of S3 key if NULL
- **`remainder`** = everything after the first `/` in the S3 key (`s3_key.split('/', 1)[1]`)

### Examples

| S3 Key | destination_base | destination_prefix_base | GCS Key |
|--------|------------------|------------------------|---------|
| `data/reports/2026_06_18/sales.csv` | `reports/` | `ingested/` | `ingested/reports/reports/2026_06_18/sales.csv` |
| `tenant1/sub2/txns/daily/2026_06_18/12_30.csv` | NULL | NULL | `tenant1/sub2/txns/daily/2026_06_18/12_30.csv` |

Trailing slash normalization (`rstrip('/') + '/'`) ensures consistent keys regardless of whether operators include trailing slashes in the control table.

---

## Ordering & Blocking

### How ordering works

Within each `file_id` (pattern), files are ordered by their S3 `LastModified` timestamp. The system enforces **strict sequential processing**: file N+1 cannot proceed until file N is marked as "done."

### File identity

A file is uniquely identified by the tuple: `(file_id, s3_key, last_modified, etag)`

A re-uploaded file with the same name but different `LastModified`/`ETag` is treated as a new, distinct file.

### Predecessor resolution (S3-first)

The predecessor is always determined from **S3, not BigQuery**, because the predecessor's Lambda may never have fired. The system scans:

1. The **source bucket** for matching files with `LastModified < T`
2. The **archive bucket** at `YYMMDD/<short_name>/<key>`
3. **Fallback marker** folders

### Blocking behavior

| Scenario | Behavior |
|----------|----------|
| No predecessor exists | This is the first file → **proceed** |
| Predecessor has `success_log` entry | **Proceed** |
| Predecessor has S3 success marker | **Proceed** |
| Predecessor not done | Mark as **WAITING** in `error_log` and stop |

### Manual override

To unblock a stuck pattern, insert a manual `success_log` entry:

```sql
INSERT INTO `{project}.{dataset}.success_log`
(run_id, file_id, s3_key, last_modified, etag, gcs_key, bytes_copied,
 archived, is_manual, resolved_by, finished_at, inserted_at)
VALUES
('manual', {file_id}, '{s3_key}', TIMESTAMP('{last_modified}'), '{etag}',
 '{gcs_key}', 0, FALSE, TRUE, 'your_name',
 CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
```

This marks the file as "done" and allows successors to proceed.

---

## Source Archival

### Archive race protection

Source files may be archived by upstream processes before the Lambda runs. When the source object is not found:

1. Look in `archive_to_bucket` at `YYMMDD/<bucket_short_name>/<original_key>`
2. If not found, probe **±1 day** to handle date skew
3. If still not found → `S3_READ` error, retryable

### Self-archival

When `archive_after_copy_enabled = TRUE` in the control table:

1. Only runs **after** GCS copy + BQ logging succeed
2. Copies source → `archive_to_bucket` at `YYMMDD/<short_name>/<key>`
3. Deletes the source object
4. **Idempotent**: if the source is already gone on a re-run, it skips silently

### Bucket short names

Archive paths use short names configured in `src/config.py`:

```python
BUCKET_SHORT_NAMES = {
    "<source-bucket-1>": "src1",
    "<source-bucket-2>": "src2",
}
```

> **Action required**: Replace placeholder values before deployment.

---

## Logging & Observability

All logging is **append-only** (no UPDATEs) using BigQuery streaming inserts (`insert_rows_json`). Every attempt is traceable via a unique `run_id` (UUID).

### Log tables

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `run_log` | One row per sighting/attempt | `run_id`, `file_id`, `s3_key`, `last_modified`, `etag`, `attempt`, `trigger_source` |
| `success_log` | One row per successful copy or manual override | `run_id`, `file_id`, `s3_key`, `bytes_copied`, `is_manual`, `archived` |
| `error_log` | One row per failure or WAITING | `run_id`, `file_id`, `error_class`, `error_detail`, `waiting`, `retryable` |
| `alert_log` | ServiceNow dedup tracking | `file_id`, `servicenow_incident_id`, `action`, `alerted_at` |

### Error classes

| Class | Meaning |
|-------|---------|
| `S3_READ` | Failed to read source or archive file |
| `GCS_WRITE` | Failed to write to GCS |
| `BQ` | BigQuery insert failure |
| `ARCHIVE` | Self-archival failure |
| `CONFIG` | Configuration error (e.g., multiple pattern matches) |
| `ORDER_BLOCKED` | Waiting on predecessor (not alertable) |

### S3 fallback markers

When BigQuery is unreachable, the Transfer Lambda writes JSON markers to S3:

- Success: `_transfer_fallback/success/<run_id>.json`
- Failure: `_transfer_fallback/failure/<run_id>.json`

The Maintenance Lambda reconciles these into BQ on each cycle and deletes the markers.

---

## Error Handling & Retries

### Retry mechanism

The Maintenance Lambda (every 10 minutes) handles retries:

| Item State | Action | Condition |
|------------|--------|-----------|
| **WAITING** (`waiting=TRUE`) | Re-check predecessor, retry if done | Last attempt > 20 min ago |
| **FAILED** (`waiting=FALSE, retryable=TRUE`) | Retry the full transfer | Last attempt > 20 min ago |
| **Resolved** (entry in `success_log`) | Skip | — |

The 20-minute cooldown ensures the Maintenance Lambda never re-runs something that may still be in flight.

### Gap detection

Each cycle, the Maintenance Lambda checks for WAITING files whose predecessors have never been seen (no `run_log` row). This detects "gaps" — missing files that would silently stall a pattern forever.

---

## ServiceNow Alerting

Persistent failures are escalated to ServiceNow:

1. **Initial incident**: Created when a FAILED (non-WAITING) file exceeds the retry threshold
2. **Daily reminders**: Existing incidents are refreshed once per day until resolved
3. **Dedup**: One incident per failing file identity, tracked in `alert_log`
4. **Resolution**: Stops when a `success_log` entry appears (real or manual)

**WAITING files never generate alerts** — they are expected to resolve once their predecessors complete.

Credentials are read from AWS Secrets Manager (secret name configurable via `servicenow_secret_name` env var).

---

## Backfill / Replay Tool

For recovering from outages, corrected control table entries, or manual overrides.

### Single file replay

Invoke the backfill Lambda with:

```json
{
  "action": "single",
  "file_id": 1,
  "s3_key": "data/reports/2026_06_18/sales.csv",
  "last_modified": "2026-06-18T10:30:00Z",
  "etag": "abc123"
}
```

### Date range replay

Re-process all matching files in a date range:

```json
{
  "action": "range",
  "file_id": 1,
  "start_date": "2026-06-15T00:00:00Z",
  "end_date": "2026-06-18T23:59:59Z"
}
```

### Safety guarantees

- **Idempotent**: Files already in `success_log` are skipped
- **Ordering preserved**: Files are processed in `last_modified` order with predecessor checks
- **Audit trail**: All replay attempts are logged to `run_log`

---

## Environment Configuration

### Lambda environment variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `env` | No | `dev` | Environment name |
| `gcp_project_id` | **Yes** | — | GCP project ID for BigQuery and GCS |
| `bigquery_dataset` | **Yes** | — | BigQuery dataset (one per environment) |
| `control_table` | No | `control_table` | Control table name |
| `run_log_table` | No | `run_log` | Run log table name |
| `success_log_table` | No | `success_log` | Success log table name |
| `error_log_table` | No | `error_log` | Error log table name |
| `alert_log_table` | No | `alert_log` | Alert log table name |
| `cache_ttl_seconds` | No | `600` | Control table cache TTL (~10 min) |
| `retry_cooldown_minutes` | No | `20` | Maintenance Lambda retry cooldown |
| `fallback_prefix` | No | `_transfer_fallback` | S3 fallback marker prefix |
| `servicenow_secret_name` | No | `servicenow-credentials` | Secrets Manager secret name |
| `servicenow_instance_url` | No | — | ServiceNow instance base URL |
| `wif_credential_file` | No | `wif.json` | Path to WIF credential file |

### Environment isolation

All BigQuery references resolve to `{gcp_project_id}.{bigquery_dataset}.<table>`. Switching `bigquery_dataset` switches the entire control + logging surface, providing full environment isolation.

### Lambda entry points

| Lambda | Handler | Trigger |
|--------|---------|---------|
| Transfer | `src.transfer_lambda.handler` | EventBridge (S3 Object Created) |
| Maintenance | `src.maintenance_lambda.handler` | EventBridge Scheduler (every 10 min) |
| Backfill | `src.backfill.backfill_handler` | Manual invocation |

---

## Manual Operations Runbook

### Unblock a stuck file

```sql
-- Insert manual success entry to unblock successors
INSERT INTO `{project}.{dataset}.success_log`
(run_id, file_id, s3_key, last_modified, etag, gcs_key, bytes_copied,
 archived, is_manual, resolved_by, finished_at, inserted_at)
VALUES
('manual', 1, 'data/reports/2026_06_15/report.csv',
 TIMESTAMP('2026-06-15T08:00:00Z'), 'etag123',
 'ingested/reports/reports/2026_06_15/report.csv', 0,
 FALSE, TRUE, 'your_name', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
```

### Check for stuck patterns

```sql
-- Find all WAITING files (blocked on predecessor)
SELECT e.file_id, e.s3_key, e.last_modified,
       e.predecessor_s3_key, e.predecessor_last_modified,
       e.inserted_at
FROM `{project}.{dataset}.error_log` e
LEFT JOIN `{project}.{dataset}.success_log` s
  ON e.file_id = s.file_id
 AND e.s3_key = s.s3_key
 AND e.last_modified = s.last_modified
WHERE e.waiting = TRUE AND s.run_id IS NULL
ORDER BY e.file_id, e.last_modified;
```

### Check recent failures

```sql
-- Find FAILED files (non-waiting, not yet resolved)
SELECT e.file_id, e.s3_key, e.last_modified,
       e.error_class, e.error_detail, e.attempt, e.inserted_at
FROM `{project}.{dataset}.error_log` e
LEFT JOIN `{project}.{dataset}.success_log` s
  ON e.file_id = s.file_id
 AND e.s3_key = s.s3_key
 AND e.last_modified = s.last_modified
WHERE e.waiting = FALSE AND s.run_id IS NULL
ORDER BY e.inserted_at DESC
LIMIT 50;
```

### Check transfer history for a file

```sql
-- Full audit trail for a specific file
SELECT * FROM `{project}.{dataset}.run_log`
WHERE file_id = 1
  AND s3_key = 'data/reports/2026_06_18/sales.csv'
ORDER BY started_at;
```

### Verify control table patterns

```sql
-- List all enabled patterns
SELECT file_id, file_path_pattern, source_bucket_name,
       destination_bucket_name, destination_base,
       archive_after_copy_enabled
FROM `{project}.{dataset}.control_table`
WHERE enabled = TRUE
ORDER BY source_bucket_name, file_id;
```

### Replay a date range after an outage

```bash
aws lambda invoke \
  --function-name s3-gcs-backfill \
  --payload '{
    "action": "range",
    "file_id": 1,
    "start_date": "2026-06-15T00:00:00Z",
    "end_date": "2026-06-18T23:59:59Z"
  }' \
  response.json
```

---

## Troubleshooting

### File not being transferred

1. **Check pattern match**: Does the S3 key match an enabled pattern in `control_table`?
2. **Check `run_log`**: Was the event received? Look for a row with the file's key
3. **Check `error_log`**: Is it WAITING (blocked on predecessor) or FAILED?
4. **Check predecessor**: Has the predecessor been transferred? Check `success_log`

### File stuck in WAITING

1. Find the predecessor: `SELECT predecessor_s3_key FROM error_log WHERE ...`
2. Check predecessor status: Look in `success_log` and S3 fallback markers
3. If predecessor is genuinely missing: insert a manual `success_log` entry or backfill it

### Transfer succeeded but file not in GCS

1. Verify in `success_log` — was `bytes_copied > 0`?
2. Check if downstream processes deleted the GCS file
3. The `success_log` entry guards against re-copy; to re-transfer, use the backfill tool

### Maintenance Lambda not retrying

1. Check cooldown: last attempt must be >20 minutes ago
2. Check `retryable` flag: non-retryable errors won't be retried
3. Check if already resolved: existing `success_log` entry means it's done

### BigQuery logging failures

1. Check Lambda logs for `BQ unreachable` messages
2. Look for S3 fallback markers: `s3://source-bucket/_transfer_fallback/success/` and `.../failure/`
3. Markers will be reconciled to BQ on the next Maintenance cycle

---

## BigQuery Table Reference

Full DDL is in `ddl/bigquery_tables.sql`. Replace `${PROJECT_ID}` and `${DATASET}` before running.

### Quick setup

```bash
# Set variables
export PROJECT_ID=your-project-id
export DATASET=your_dataset

# Replace placeholders and run
sed "s/\${PROJECT_ID}/$PROJECT_ID/g; s/\${DATASET}/$DATASET/g" \
  ddl/bigquery_tables.sql | bq query --use_legacy_sql=false
```

### Table sizes (expected)

| Table | Growth Rate | Retention |
|-------|------------|-----------|
| `control_table` | Static (1–2k rows) | Permanent |
| `run_log` | ~1 row per file event + retries | Consider partitioning by `inserted_at` |
| `success_log` | ~1 row per successful transfer | Permanent (audit trail) |
| `error_log` | Variable (depends on failure rate) | Consider partitioning by `inserted_at` |
| `alert_log` | ~1 row per alert/day per stuck file | Permanent |

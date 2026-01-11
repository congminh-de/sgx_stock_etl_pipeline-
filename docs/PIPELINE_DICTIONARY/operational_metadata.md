# ⚙️ Operational & Metadata Schema

## Overview
This document outlines the schema for the **Operational Layer**. These tables do not contain business data but are crucial for pipeline orchestration, state management, observability, and audit trails.

---

## 1. Table: `meta_file_status`
**Purpose:** Tracks the lifecycle of every raw data file to ensure Idempotency and SLA monitoring.
**Granularity:** One row per Source File.

| Column Name | MySQL Type | Python Origin Type | Description |
| :--- | :--- | :--- | :--- |
| `file_id` | `INT PK` | `int` | Unique ID extracted from filename. |
| `status` | `VARCHAR(20)` | `str` | State: `PENDING`, `PROCESSING`, `SUCCESS`, `FAILED`. |
| `created_at` | `TIMESTAMP` | `datetime` | Time when file was detected. |
| `updated_at` | `TIMESTAMP` | `datetime` | Time of last status change. |
| `etl_sec` | `INT` | `float` | **[SLA Metric]** Total processing duration in seconds. |
| `retry_count` | `INT` | `int` | Number of retry attempts. |

---

## 2. Table: `etl_job_logs`
**Purpose:** A detailed audit log compatible with Airflow, capturing execution metrics and errors for debugging.
**Granularity:** One row per Job Execution Run.

| Column Name | MySQL Type | Python Origin Type | Description |
| :--- | :--- | :--- | :--- |
| `log_id` | `INT PK` | `N/A` | Auto-increment Log ID. |
| `dag_id` | `VARCHAR(100)` | `str` | Airflow DAG Identifier. |
| `task_id` | `VARCHAR(100)` | `str` | Airflow Task Identifier. |
| `run_id` | `VARCHAR(255)` | `str` | Unique Run ID from Airflow. |
| `file_id` | `VARCHAR(50)` | `str` | Reference to the file being processed. |
| `status` | `VARCHAR(20)` | `str` | Execution Status: `PROCESSING`, `SUCCESS`, `FAILED`. |
| `execution_time`| `TIMESTAMP` | `datetime` | Timestamp of the log entry. |
| `row_count` | `INT` | `int` | Number of rows affected (for volume tracking). |
| `error_message` | `TEXT` | `str` | Stack trace or error details if failed. |
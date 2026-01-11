# 🔄 Workflow & Logic Flow

## 1. Execution Flow (Step-by-Step)

The pipeline executes the following sequence for each data file:

1.  **Initialization:**
    * Python queries `meta_file_status` to find files with `PENDING` status.
    * Updates status to `PROCESSING`.

2.  **Extraction (Python):**
    * Downloads the .zip file using a resilient stream with **Exponential Backoff Retry** (handling `IncompleteRead` errors).
    * Extracts content to memory/disk buffer.

3.  **Loading to Staging (Python):**
    * Reads CSV chunks using Pandas.
    * Performs minimal formatting (Type casting).
    * Bulk loads data into `stg_hourly_stats`, `stg_session_summary`, etc. using `to_sql`.

4.  **Transformation (MySQL):**
    * Python triggers the Stored Procedure `sp_etl_loads_fact`.
    * **Transaction Start:**
        * **Step A:** Sync Dimensions (Commodity, Trader Type, Date).
        * **Step B:** Run **DQ Checks**. If fail -> Rollback.
        * **Step C:** **Idempotency Clean-up:** Delete existing Fact records for the current `source_id`.
        * **Step D:** Insert new transformed records into Fact tables.
    * **Transaction Commit.**

5.  **Finalization:**
    * Python captures the execution duration.
    * Updates `meta_file_status` to `SUCCESS` (or `FAILED`) with `etl_sec`.
    * Logs the run details to `etl_job_logs`.

---

## 2. Key Engineering Concepts

### 2.1 Idempotency Strategy
To ensure the pipeline can be re-run safely without creating duplicate data:
* **Logic:** `DELETE FROM fact_table WHERE source_id = X` before `INSERT`.
* **Benefit:** Allows re-processing of any historical file to fix bugs or update logic without manual cleanup.

### 2.2 Transaction Management
The entire MySQL transformation phase is wrapped in a single Transaction.
* **ACID Guarantee:** Either all tables (Facts, Dims, Logs) are updated successfully, or none are. This prevents "Partial Data" states (e.g., data loaded in Hourly but missing in Session Summary).
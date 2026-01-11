# 🛡️ Data Quality (DQ) & SLA Policies

## 1. Service Level Agreements (SLA)
The pipeline is governed by strict timing and performance constraints to ensure data availability for T+1 reporting.

### 1.1 Timeliness Policy
* **Target:** Data must be available in the Data Warehouse by **08:00 AM** (Local Time).
* **Breach Action:** * If `current_time > 08:00 AM`, an alert is sent to the Data Engineering Channel.
    * The incident is logged in `etl_job_logs` with a specific tag.

### 1.2 Performance Policy
* **Threshold:** Single file processing time (`etl_sec`) must not exceed **30 minutes**.
* **Monitoring:** The `meta_file_status.etl_sec` column is audited daily.
* **Auto-Retry:** Jobs failing due to transient network issues are retried up to **3 times** (Exponential Backoff).

---

## 2. Data Quality Gates
DQ logic is embedded directly into the MySQL Stored Procedure (`sp_etl_loads_fact`) to act as a firewall against bad data.

### 2.1 Critical Rules (Blocker)
*If any of these rules are violated, the pipeline performs a ROLLBACK and marks the job as FAILED.*

| Rule ID | Rule Description | Implementation Logic (SQL) |
| :--- | :--- | :--- |
| **DQ_01** | **Negative Volume:** Volume cannot be less than zero. | `SELECT COUNT(*) FROM stg WHERE Volume < 0` |
| **DQ_02** | **Invalid Price:** High price cannot be lower than Low price. | `SELECT COUNT(*) FROM stg WHERE High < Low` |
| **DQ_03** | **Missing Context:** Commodity Code must exist in Dimension. | Enforced via `INSERT IGNORE` or Staging Checks. |

### 2.2 Warning Rules (Non-Blocking)
*These issues trigger a warning log but allow the pipeline to proceed.*

| Rule ID | Rule Description | Action |
| :--- | :--- | :--- |
| **DQ_W1** | **Volatility Check:** Price changes > 20% vs previous session. | Log warning to `etl_job_logs`. |
| **DQ_W2** | **Zero Turnover:** Volume > 0 but Turnover = 0. | Log warning for data review. |
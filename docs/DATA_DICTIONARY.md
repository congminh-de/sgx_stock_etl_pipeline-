# 📚 Data Dictionary (Local Pipeline)

## Overview
This document details the database schema for the **SGX Derivatives Analytics (Local Version)**.
* **Database Engine:** MySQL 8.0 (InnoDB)
* **Optimization Strategy:** Vertical scaling on limited hardware (8GB RAM).
* **Data Modeling:** Aggregated Fact Tables (No Dimension tables were used to simplify local JOINs and improve write performance).

---

## Business Logic & Definitions

### Trader Classification Logic
Derived from the `Volume` per single tick to estimate market participant size.

| Classification | Volume Range (Lots) | Description |
| :--- | :--- | :--- |
| **1. Retail** | $v \le 5$ | Individual retail traders or small algos. |
| **2. Medium** | $5 < v \le 20$ | Professional day traders or prop desks. |
| **3. Shark** | $20 < v \le 100$ | Institutional algorithms or hedge funds. |
| **4. Whale** | $v > 100$ | Market makers or large institutional rebalancing. |

---

## Database Schema: `sgx_data`

### 1. Table: `fact_hourly_stats`
**Purpose:** Stores aggregated market data at an hourly granularity for intraday analysis.
**Granularity:** One row per Commodity, per Date, per Hour.

| Column Name | MySQL Type | Python Origin Type | Description |
| :--- | :--- | :--- | :--- |
| `Comm` | `VARCHAR(20)` | `category` | Commodity Code (e.g., NK, CN). |
| `Trade_Date` | `INT` | `uint32` | Date in `YYYYMMDD` integer format (Optimized for indexing). |
| `Hour` | `TINYINT` | `uint8` | Hour of the day (0-23). |
| `Total_Volume` | `BIGINT` | `uint32` | Sum of volume in that hour. |
| `Total_Turnover` | `DOUBLE` | `float32` | Sum of (Price * Volume). |
| `Low` | `DOUBLE` | `float32` | Lowest price recorded in that hour. |
| `High` | `DOUBLE` | `float32` | Highest price recorded in that hour. |
| `source_id` | `INT` | `int` | **[Metadata]** ID of the source file for traceability. |

* **Indexes:**
    * `idx_comm_date` (`Comm`, `Trade_Date`): Optimizes time-series queries.
    * `idx_source` (`source_id`): Optimizes ETL rollback/deletion.

---

### 2. Table: `fact_daily_summary`
**Purpose:** Daily OHLC (Open, High, Low, Close) summary for standard candlestick charting.
**Granularity:** One row per Commodity, per Date.

| Column Name | MySQL Type | Description |
| :--- | :--- | :--- |
| `Comm` | `VARCHAR(20)` | Commodity Code. |
| `Trade_Date` | `INT` | Date in `YYYYMMDD`. |
| `Open` | `DOUBLE` | Price of the **first** tick in the session (Time-based). |
| `High` | `DOUBLE` | Highest price of the day. |
| `Low` | `DOUBLE` | Lowest price of the day. |
| `Close` | `DOUBLE` | Price of the **last** tick in the session (Time-based). |
| `Total_Volume` | `BIGINT` | Total daily volume. |
| `Total_Turnover`| `DOUBLE` | Total daily turnover (Liquidity proxy). |
| `source_id` | `INT` | Lineage key. |

---

### 3. Table: `fact_trade_distribution`
**Purpose:** Analysis of market composition (Retail vs. Whales).
**Granularity:** One row per Commodity, per Trader Type.

| Column Name | MySQL Type | Description |
| :--- | :--- | :--- |
| `Comm` | `VARCHAR(20)` | Commodity Code. |
| `Trader_Type` | `VARCHAR(50)` | Class label (Retail, Medium, Shark, Whale). |
| `Trade_Count` | `INT` | Number of transactions executed by this group. |
| `Total_Volume` | `BIGINT` | Total volume contributed by this group. |
| `source_id` | `INT` | Lineage key. |

---

## 🔄 Data Lineage & ETL Flow

1.  **Extraction:** Zip files are downloaded via `Requests` (Streaming Mode).
2.  **Processing (Pandas):**
    * Files read in chunks (100k rows).
    * **Partial Aggregation:** Data is grouped and aggregated within the chunk.
    * **Final Merge:** Aggregated chunks are combined to form the final datasets.
3.  **Loading (SQLAlchemy):**
    * `DELETE` existing records for the current `source_id` (Idempotency check).
    * `INSERT` new transformed records.
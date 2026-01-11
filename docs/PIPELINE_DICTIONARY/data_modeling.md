# 🗄️ Data Modeling & Schema Design

## Overview
This document outlines the database schema for the **SGX Derivatives Analytics Pipeline**. The architecture follows a **Staging-to-Fact Schema** transformation flow within MySQL 8.0.

---

## 1. Staging Layer (Transient)
These tables act as a landing zone for bulk data loaded directly from the Python Extraction Engine. They have no foreign keys to maximize write throughput.

### 1.1 Table: `stg_hourly_stats`
**Purpose:** Temporary storage for intraday market data before transformation.
**Granularity:** One row per Commodity, per Date, per Hour.

| Column Name | MySQL Type | Python Origin Type | Description |
| :--- | :--- | :--- | :--- |
| `Comm` | `VARCHAR(20)` | `category` | Commodity Code (e.g., NK, CN). |
| `Trade_Date` | `INT` | `uint32` | Date in `YYYYMMDD` integer format. |
| `Hour` | `INT` | `uint8` | Hour of the day (0-23). |
| `Total_Volume` | `BIGINT` | `uint32` | Sum of volume in that hour. |
| `Total_Turnover` | `DOUBLE` | `float64` | Sum of (Price * Volume). |
| `Low` | `DOUBLE` | `float32` | Lowest price recorded in that hour. |
| `High` | `DOUBLE` | `float32` | Highest price recorded in that hour. |
| `source_id` | `INT` | `int32` | **[Metadata]** ID of the source file for lineage. |

### 1.2 Table: `stg_session_summary`
**Purpose:** Temporary storage for daily/session OHLC summary data.
**Granularity:** One row per Commodity, per Session (File).

| Column Name | MySQL Type | Python Origin Type | Description |
| :--- | :--- | :--- | :--- |
| `Comm` | `VARCHAR(20)` | `category` | Commodity Code. |
| `Trade_Date` | `INT` | `uint32` | Date in `YYYYMMDD` format. |
| `Total_Volume` | `BIGINT` | `uint32` | Total accumulated volume. |
| `Total_Turnover` | `DOUBLE` | `float64` | Total accumulated turnover. |
| `Low` | `DOUBLE` | `float32` | Lowest price in the session. |
| `High` | `DOUBLE` | `float32` | Highest price in the session. |
| `Open` | `DOUBLE` | `float32` | First price of the session. |
| `Close` | `DOUBLE` | `float32` | Last price of the session. |
| `source_id` | `INT` | `int32` | **[Metadata]** Lineage key. |

### 1.3 Table: `stg_trade_distribution`
**Purpose:** Temporary storage for market participant analysis.
**Granularity:** One row per Commodity, per Trader Type.

| Column Name | MySQL Type | Python Origin Type | Description |
| :--- | :--- | :--- | :--- |
| `Comm` | `VARCHAR(20)` | `category` | Commodity Code. |
| `Trader_Type` | `VARCHAR(50)` | `category` | Classification (Retail, Shark, Whale). |
| `Trade_Count` | `INT` | `int32` | Number of executed trades. |
| `Total_Volume` | `BIGINT` | `uint32` | Total volume by this group. |
| `source_id` | `INT` | `int32` | **[Metadata]** Lineage key. |

---

## 2. Dimension Layer (Context)
Normalized tables providing descriptive context for the Fact tables.

### 2.1 Table: `dim_commodity`
**Purpose:** Distinct list of all traded commodities.

| Column Name | MySQL Type | Python Origin Type | Description |
| :--- | :--- | :--- | :--- |
| `Comm_id` | `INT PK` | `N/A (Auto-Inc)` | Surrogate Key for the commodity. |
| `Comm_Code` | `VARCHAR(20)` | `category` | The actual ticker symbol (e.g., NK). |

### 2.2 Table: `dim_trader_type`
**Purpose:** Reference table for trader classifications.

| Column Name | MySQL Type | Python Origin Type | Description |
| :--- | :--- | :--- | :--- |
| `Trader_Type_id` | `INT PK` | `N/A (Auto-Inc)` | Surrogate Key for trader type. |
| `Trader_Type_Name`| `VARCHAR(50)` | `category` | Name: Retail, Medium, Shark, Whale. |

### 2.3 Table: `dim_date`
**Purpose:** Calendar logic for time-series aggregation.

| Column Name | MySQL Type | Python Origin Type | Description |
| :--- | :--- | :--- | :--- |
| `Date_id` | `INT PK` | `uint32` | Date Key in `YYYYMMDD` format. |
| `Full_Date` | `DATE` | `datetime64` | Standard SQL Date format. |
| `Year` | `INT` | `int16` | Derived Year. |
| `Month` | `INT` | `int8` | Derived Month (1-12). |
| `Quarter` | `INT` | `int8` | Derived Quarter (1-4). |
| `Week_Num` | `INT` | `int8` | ISO Week Number. |
| `Day_Name` | `VARCHAR(10)` | `object` | Name of day (Monday, Tuesday...). |

---

## 3. Fact Layer (Analytics)
The core analytical tables using the Star Schema design.

### 3.1 Table: `fact_session_summary`
**Purpose:** Stores final OHLC data aggregated by Trading Session.
**Granularity:** One row per Session (Source), per Commodity.

| Column Name | MySQL Type | Python Origin Type | Description |
| :--- | :--- | :--- | :--- |
| `source_id` | `INT PK` | `int32` | **[FK]** Lineage key. |
| `Comm_id` | `INT PK` | `N/A` | **[FK]** Reference to `dim_commodity`. |
| `Date_id` | `INT` | `uint32` | **[FK]** Reference to `dim_date`. |
| `Total_Volume` | `BIGINT` | `uint32` | Total accumulated volume. |
| `Total_Turnover` | `DOUBLE` | `float64` | Total accumulated turnover. |
| `Low` | `DOUBLE` | `float32` | Session Low Price. |
| `High` | `DOUBLE` | `float32` | Session High Price. |
| `Open` | `DOUBLE` | `float32` | Session Open Price. |
| `Close` | `DOUBLE` | `float32` | Session Close Price. |

### 3.2 Table: `fact_trade_distribution`
**Purpose:** Analyzes volume share by market participant types.
**Granularity:** One row per Session, per Commodity, per Trader Type.

| Column Name | MySQL Type | Python Origin Type | Description |
| :--- | :--- | :--- | :--- |
| `source_id` | `INT PK` | `int32` | **[FK]** Lineage key. |
| `Comm_id` | `INT PK` | `N/A` | **[FK]** Reference to `dim_commodity`. |
| `Trader_Type_id`| `INT PK` | `N/A` | **[FK]** Reference to `dim_trader_type`. |
| `Trade_Count` | `INT` | `int32` | Number of trades executed. |
| `Total_Volume` | `BIGINT` | `uint32` | Volume contributed by this type. |

### 3.3 Table: `fact_hourly_stats`
**Purpose:** High-granularity table for intraday time-series analysis.
**Granularity:** One row per Hour, per Commodity.

| Column Name | MySQL Type | Python Origin Type | Description |
| :--- | :--- | :--- | :--- |
| `source_id` | `INT PK` | `int32` | **[FK]** Lineage key. |
| `Comm_id` | `INT PK` | `N/A` | **[FK]** Reference to `dim_commodity`. |
| `Date_id` | `INT` | `uint32` | **[FK]** Reference to `dim_date`. |
| `Hour` | `INT PK` | `uint8` | Hour of the day (0-23). |
| `Total_Volume` | `BIGINT` | `uint32` | Hourly Volume. |
| `Total_Turnover` | `DOUBLE` | `float64` | Hourly Turnover. |
| `Low` | `DOUBLE` | `float32` | Hourly Low Price. |
| `High` | `DOUBLE` | `float32` | Hourly High Price. |

---

## 4. Optimization: Table Partitioning
To handle the high growth rate of intraday data, **Range Partitioning** is applied to the `fact_hourly_stats` table.

> **Technical Note:** Physical Foreign Key constraints (`CONSTRAINT FK...`) are removed from this table to support MySQL Partitioning. Data integrity is enforced logically via the ETL Stored Procedure.

```sql
ALTER TABLE fact_hourly_stats
PARTITION BY RANGE (Date_id) (
    PARTITION p2025 VALUES LESS THAN (20260101),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);
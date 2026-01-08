-- samples 

SELECT * FROM etl_job_logs;
DROP TABLE IF EXISTS Comm_change; 

CREATE TABLE Comm_change AS
SELECT 
    c.Comm_Code AS Comm, 
    f.Total_Volume,
    f.Total_Turnover,
    ROUND(100.0 * (f.Close - f.Open) / NULLIF(f.Open, 0), 2) AS Pct_Change
FROM real_fact_session_summary f
JOIN dim_commodity c ON f.Comm_id = c.Comm_id 
ORDER BY Pct_Change DESC;


DROP TABLE IF EXISTS hourly_stats;

CREATE TABLE hourly_stats AS
SELECT 
    Hour,
    SUM(Total_Volume) AS Total_Volume,
    SUM(Total_Turnover) AS Total_Turnover
FROM real_fact_hourly_stats
GROUP BY Hour
ORDER BY Hour ASC;


DROP TABLE IF EXISTS Trader_Types;

CREATE TABLE Trader_Types AS 
SELECT 
    t.Trader_Type_Name AS Trader_Type, 
    SUM(f.Total_Volume) AS Total_Volume,
    SUM(f.Trade_Count) AS Total_Count 
FROM real_fact_trade_distribution f
JOIN dim_trader_type t ON f.Trader_Type_id = t.Trader_Type_id 
GROUP BY t.Trader_Type_Name;



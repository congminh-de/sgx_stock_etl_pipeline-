-- samples 
CREATE TABLE Comm_change AS
SELECT 
    Comm,
    Total_Volume,
    Total_Turnover,
    ROUND(100.0*(Close-Open)/Open,2) AS Pct_Change
FROM fact_daily_summary
ORDER BY ROUND(100.0*(Close-Open)/Open,2) DESC;

CREATE TABLE hourly_stats AS
SELECT 
    Hour,
    SUM(Total_Volume) AS Total_Volume,
    SUM(Total_Turnover) AS Total_Turnover
FROM fact_hourly_stats
GROUP BY Hour;

CREATE TABLE Trader_Types AS 
SELECT 
    Trader_Type,
    SUM(Total_Volume) AS Total_Volume,
    SUM(Total_Count) AS Total_Count
FROM fact_trade_distribution
GROUP BY `Trader_Type`;


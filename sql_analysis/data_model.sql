
DROP TABLE IF EXISTS fact_trade_distribution;
DROP TABLE IF EXISTS fact_hourly_stats;
DROP TABLE IF EXISTS fact_session_summary;
DROP TABLE IF EXISTS dim_trader_type;
DROP TABLE IF EXISTS dim_commodity;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_commodity (
    Comm_id INT AUTO_INCREMENT PRIMARY KEY,
    Comm_Code VARCHAR(20) NOT NULL UNIQUE
);


CREATE TABLE dim_trader_type (
    Trader_Type_id INT AUTO_INCREMENT PRIMARY KEY,
    Trader_Type_Name VARCHAR(50) NOT NULL UNIQUE
);


CREATE TABLE dim_date (
    Date_id INT PRIMARY KEY,
    Full_Date DATE,
    Year INT,
    Month INT,
    Quarter INT,
    Week_Num INT,
    Day_Name VARCHAR(10)
);


CREATE TABLE fact_session_summary (
    source_id INT,           
    Comm_id INT,               
    Date_id INT,                
    Total_Volume BIGINT DEFAULT 0,
    Total_Turnover DOUBLE DEFAULT 0,
    Low DOUBLE,
    High DOUBLE,
    Open DOUBLE,
    Close DOUBLE,
    PRIMARY KEY (source_id, Comm_id),
    CONSTRAINT fk_sess_comm FOREIGN KEY (Comm_id) REFERENCES dim_commodity(Comm_id),
    CONSTRAINT fk_sess_date FOREIGN KEY (Date_id) REFERENCES dim_date(Date_id)
);


CREATE TABLE fact_hourly_stats (
    source_id INT,
    Comm_id INT,
    Date_id INT,
    Hour INT,
    Total_Volume BIGINT DEFAULT 0,
    Total_Turnover DOUBLE DEFAULT 0,
    Low DOUBLE,
    High DOUBLE,
    PRIMARY KEY (source_id, Comm_id, Hour),    
    CONSTRAINT fk_hour_comm FOREIGN KEY (Comm_id) REFERENCES dim_commodity(Comm_id),
    CONSTRAINT fk_hour_date FOREIGN KEY (Date_id) REFERENCES dim_date(Date_id)
);


CREATE TABLE fact_trade_distribution (
    source_id INT,
    Comm_id INT,
    Trader_Type_id INT, 
    Trade_Count INT DEFAULT 0,
    Total_Volume BIGINT DEFAULT 0,
    PRIMARY KEY (source_id, Comm_id, Trader_Type_id),    
    CONSTRAINT fk_dist_comm FOREIGN KEY (Comm_id) REFERENCES dim_commodity(Comm_id),
    CONSTRAINT fk_dist_type FOREIGN KEY (Trader_Type_id) REFERENCES dim_trader_type(Trader_Type_id)
);
ALTER TABLE fact_hourly_stats
PARTITION BY RANGE (Date_id) (
    PARTITION p2025 VALUES LESS THAN (20260101),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

--DROP PROCEDURE IF EXISTS sp_etl_loads_fact$$
DELIMITER $$

CREATE PROCEDURE sp_etl_loads_fact()
BEGIN 

    INSERT IGNORE INTO dim_commodity (Comm_Code)
    SELECT DISTINCT Comm FROM stg_session_summary; 

    INSERT IGNORE INTO dim_trader_type (Trader_Type_Name)
    VALUES ('Retail'), ('Medium'), ('Shark'), ('Whale');   

    INSERT IGNORE INTO dim_date (Date_id, Full_Date, Year, Month, Quarter, Week_Num, Day_Name)
    SELECT DISTINCT 
        Trade_Date, 
        STR_TO_DATE(CAST(Trade_Date AS CHAR), '%Y%m%d'),
        YEAR(STR_TO_DATE(CAST(Trade_Date AS CHAR), '%Y%m%d')),
        MONTH(STR_TO_DATE(CAST(Trade_Date AS CHAR), '%Y%m%d')),
        QUARTER(STR_TO_DATE(CAST(Trade_Date AS CHAR), '%Y%m%d')),
        WEEK(STR_TO_DATE(CAST(Trade_Date AS CHAR), '%Y%m%d')),
        DAYNAME(STR_TO_DATE(CAST(Trade_Date AS CHAR), '%Y%m%d'))
    FROM stg_session_summary;   

    DELETE FROM fact_session_summary 
    WHERE source_id IN (SELECT DISTINCT source_id FROM stg_session_summary);   

    INSERT INTO fact_session_summary 
    (source_id, Comm_id, Date_id, Total_Volume, Total_Turnover, Low, High, Open, Close)
    SELECT 
        s.source_id,
        d.Comm_id,
        MAX(s.Trade_Date) as Date_id, 
        SUM(s.Total_Volume) as Total_Volume, 
        SUM(s.Total_Turnover) as Total_Turnover, 
        MIN(s.Low) as Low,   
        MAX(s.High) as High, 
        (SELECT Open FROM stg_session_summary s2 
         WHERE s2.source_id = s.source_id AND s2.Comm = s.Comm 
         ORDER BY s2.Trade_Date ASC LIMIT 1) as Open_Price,
        (SELECT Close FROM stg_session_summary s2 
         WHERE s2.source_id = s.source_id AND s2.Comm = s.Comm 
         ORDER BY s2.Trade_Date DESC LIMIT 1) as Close_Price
    FROM stg_session_summary s
    JOIN dim_commodity d ON s.Comm = d.Comm_Code
    GROUP BY s.source_id, d.Comm_id; 

    DELETE FROM fact_hourly_stats 
    WHERE source_id IN (SELECT DISTINCT source_id FROM fact_hourly_stats);

    INSERT INTO fact_hourly_stats 
    (source_id, Comm_id, Date_id, Hour, Total_Volume, Total_Turnover, Low, High)
    SELECT 
        s.source_id,
        d.Comm_id,
        s.Trade_Date,
        s.Hour,
        s.Total_Volume,
        s.Total_Turnover,
        s.Low, s.High
    FROM stg_hourly_stats s
    JOIN dim_commodity d ON s.Comm = d.Comm_Code; 

    DELETE FROM fact_trade_distribution 
    WHERE source_id IN (SELECT DISTINCT source_id FROM stg_trade_distribution); 

    INSERT INTO fact_trade_distribution 
    (source_id, Comm_id, Trader_Type_id, Trade_Count, Total_Volume)
    SELECT 
        s.source_id,
        c.Comm_id,
        t.Trader_Type_id,
        SUM(s.Trade_Count), 
        SUM(s.Total_Volume) 
    FROM stg_trade_distribution s
    JOIN dim_commodity c ON s.Comm = c.Comm_Code
    JOIN dim_trader_type t ON s.Trader_Type = t.Trader_Type_Name
    GROUP BY s.source_id, c.Comm_id, t.Trader_Type_id; 

END$$

DELIMITER ;

call sp_etl_loads_fact();
CREATE TABLE IF NOT EXISTS etl_job_logs (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    dag_id VARCHAR(100),            
    task_id VARCHAR(100),           
    run_id VARCHAR(255),            
    file_id VARCHAR(50),            
    status VARCHAR(20),             
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    row_count INT DEFAULT 0,        
    error_message TEXT              
);

CREATE TABLE IF NOT EXISTS fact_hourly_stats (
    Comm VARCHAR(20),
    Trade_Date INT,
    Hour INT,
    Total_Volume BIGINT,
    Total_Turnover DOUBLE,
    Low DOUBLE,
    High DOUBLE,
    source_id INT,
    Full_Time DATETIME,
    INDEX idx_source (source_id),
    INDEX idx_comm_date (Comm, Trade_Date)
);

CREATE TABLE IF NOT EXISTS fact_daily_summary (
    Comm VARCHAR(20),
    Trade_Date INT,
    Total_Volume BIGINT,
    Total_Turnover DOUBLE,
    Low DOUBLE,
    High DOUBLE,
    Open DOUBLE,
    Close DOUBLE,
    source_id INT,
    INDEX idx_source (source_id),
    INDEX idx_comm_date (Comm, Trade_Date)
);

CREATE TABLE IF NOT EXISTS fact_trade_distribution (
    Comm VARCHAR(20),
    Trader_Type VARCHAR(50),
    Trade_Count INT,
    Total_Volume BIGINT,
    source_id INT,
    INDEX idx_source (source_id)
);
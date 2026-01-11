import os
import sys
import logging
import configparser
import time
import requests
import zipfile
import pandas as pd
import gc
import time
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from logging.handlers import RotatingFileHandler
import traceback 
from logger_utils import log_etl_status

# CONFIGURATION
CONFIG_PATH = "/opt/airflow/config.ini"
LOG_DIR = "/opt/airflow/logs"
TEMP_DIR = "/opt/airflow/data/temp"
SLA_CONFIG = {
    "MAX_duration": 120, 
    "DEADLINE_HOUR": 8,         
}

# SETUP LOGGING
def setup_logging():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR, exist_ok=True)
    
    logger = logging.getLogger("SGX_Pandas_ETL")
    logger.setLevel(logging.INFO)
    
    if logger.hasHandlers():
        return logger

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_h = RotatingFileHandler(os.path.join(LOG_DIR, "app.log"), maxBytes=5*1024*1024, backupCount=3)
    file_h.setFormatter(formatter)
    console_h = logging.StreamHandler(sys.stdout)
    console_h.setFormatter(formatter)
    logger.addHandler(file_h)
    logger.addHandler(console_h)
    return logger
logger = setup_logging()

# CONNECTION
def get_db_engine(config):
    try:
        db = config['Database']
        conn_str = f"mysql+pymysql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
        engine = create_engine(conn_str)
        return engine
    except Exception as e:
        logger.error(f"DB Connection Failed: {e}")
        raise e

def classify_traders(vol):
    if vol <= 5: return 'Retail'
    elif vol <= 20: return 'Medium'
    elif vol <= 100: return 'Shark'
    else: return 'Whale'

# DATA QUALITY VALIDATION 
def validate_dataset(df, name):
    if df.empty:
        raise ValueError(f"Dataset {name} is NULL")
        return True
    if "Total_Turnover" in df.columns:
        if (df["Total_Turnover"]<0).any():
            raise ValueError(f"Dataset {name} has negative turnover values")
    if "Trade_Date" in df.columns:
        if (df["Trade_Date"]).isnull().any():
            raise ValueError(f"Dataset {name} has Null trade_date values")
    logger.info(f"Dataset {name} passed Data Quality Checks")
    return True

# DOWNLOAD DATA TO DISK
def extract_data(file_id, base_url, save_path):
    url = base_url.format(id=file_id)
    logger.info(f"Downloading ID {file_id} to disk...")
    try:
        with requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, stream=True, timeout=120) as r:
            if r.status_code == 404:
                logger.warning(f"ID {file_id} not found.")
                return False
            r.raise_for_status()
            
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return True
    except Exception as e:
        logger.error(f"Download error ID {file_id}: {e}")
        return False

# READ AND TRANSFORM DATA
def transform_data(zip_file_path, source_id):
    logger.info(f"Processing file: {zip_file_path}")

    # We use these to accumulate data chunk by chunk 
    running_hourly = None
    running_session = None
    running_dist = None
    
    try:
        z = zipfile.ZipFile(zip_file_path)
        csv_file = next((f for f in z.namelist() if f.endswith(('.csv', '.txt'))), None)
        
        if not csv_file:
            raise FileNotFoundError("No CSV file found in zip")

        dtypes = {'Comm': 'category', 'Contract_Type': 'category',
                    'Volume': 'uint32', 'Price': 'float32',
                    'Log_Time': 'uint32', 'Trade_Date': 'uint32'}
        
        # Read in chunks
        chunk_iter = pd.read_csv(z.open(csv_file), chunksize=100000, on_bad_lines='skip', dtype=dtypes)

        for chunk in chunk_iter:
            chunk = chunk[(chunk['Price'] > 0)].copy()
            if chunk.empty: continue

            chunk['Turnover'] = (chunk['Price'] * chunk['Volume']).astype('float32')
            chunk['Hour'] = (chunk['Log_Time'] // 10000).astype('uint8')

            # 1. Hourly aggregation
            chunk_hourly = chunk.groupby(['Comm', 'Trade_Date', 'Hour'], observed=True).agg({
                'Volume': 'sum', 'Turnover': 'sum', 'Price': ['min', 'max']})
            chunk_hourly.columns = ['Total_Volume', 'Total_Turnover', 'Low', 'High']
            chunk_hourly.reset_index(inplace=True)

            if running_hourly is None:
                running_hourly = chunk_hourly
            else:
                running_hourly = pd.concat([running_hourly, chunk_hourly])
                running_hourly = running_hourly.groupby(['Comm', 'Trade_Date', 'Hour'], observed=True).agg({
                    'Total_Volume': 'sum', 'Total_Turnover': 'sum', 'Low': 'min', 'High': 'max'}).reset_index()

            # 2.session aggregation
            # For Open/Close, we take the min/max Log_Time of the chunk to sort later
            chunk_session = chunk.groupby(['Comm', 'Trade_Date'], observed=True).agg({
                'Volume': 'sum', 'Turnover': 'sum', 'Price': ['min', 'max'], 'Log_Time': ['min', 'max']})
            chunk_session.columns = ['Total_Volume', 'Total_Turnover', 'Low', 'High', 'Time_Min', 'Time_Max']

            # Get First/Last price of chunks
            chunk.sort_values('Log_Time', inplace=True)
            first_prices = chunk.groupby(['Comm', 'Trade_Date'], observed=True)['Price'].first()
            last_prices = chunk.groupby(['Comm', 'Trade_Date'], observed=True)['Price'].last()
            
            chunk_session = chunk_session.reset_index()
            chunk_session['Open_Price'] = chunk_session.set_index(['Comm', 'Trade_Date']).index.map(first_prices)
            chunk_session['Close_Price'] = chunk_session.set_index(['Comm', 'Trade_Date']).index.map(last_prices)
            
            if running_session is None:
                running_session = chunk_session
            else:
                running_session = pd.concat([running_session, chunk_session])
                pass 

            # 3.Distribution
            chunk['Trader_Type'] = chunk['Volume'].apply(classify_traders)
            chunk_dist = chunk.groupby(['Comm', 'Trader_Type'], observed=True).agg({
                'Volume': ['count', 'sum']})
            chunk_dist.columns = ['Trade_Count', 'Total_Volume']
            chunk_dist.reset_index(inplace=True)

            if running_dist is None:
                running_dist = chunk_dist
            else:
                running_dist = pd.concat([running_dist, chunk_dist])
                running_dist = running_dist.groupby(['Comm', 'Trader_Type'], observed=True).agg({
                    'Trade_Count': 'sum', 'Total_Volume': 'sum'}).reset_index()
            # Clean up memory
            del chunk, chunk_hourly, chunk_session, chunk_dist
            gc.collect()

        logger.info("Chunks processed. Finalizing datasets...")
        
        final_datasets = {}

        # Finalize hourly
        if running_hourly is not None:
            running_hourly['source_id'] = source_id
            validate_dataset(running_hourly, "stg_hourly_stats")
            final_datasets['stg_hourly_stats'] = running_hourly

        # Finalize session 
        if running_session is not None:
            grouped = running_session.groupby(['Comm', 'Trade_Date']).agg({
                'Total_Volume': 'sum',
                'Total_Turnover': 'sum',
                'Low': 'min',
                'High': 'max'})
            # Sort by Time_Min to put earliest chunk first
            running_session.sort_values('Time_Min', inplace=True)
            global_open = running_session.groupby(['Comm', 'Trade_Date'])['Open_Price'].first()
            # Sort by Time_Max to put latest chunk first
            running_session.sort_values('Time_Max', inplace=True)
            global_close = running_session.groupby(['Comm', 'Trade_Date'])['Close_Price'].last()
            
            final_session = grouped.join(global_open.rename('Open')).join(global_close.rename('Close')).reset_index()
            final_session['source_id'] = source_id
            validate_dataset(final_session, "stg_session_summary")
            final_datasets['stg_session_summary'] = final_session

        # Finalize distribution
        if running_dist is not None:
            running_dist['source_id'] = source_id
            validate_dataset(running_dist, "stg_trade_distribution")
            final_datasets['stg_trade_distribution'] = running_dist

        return final_datasets

    except Exception as e:
        logger.error(f"Processing Error: {str(e)}")
        raise e

# LOAD SQL QUERY   
def load_query(query_name):
    path = f"/opt/airflow/sql_analysis/queries/{query_name}.sql"
    with open(path, 'r') as f:
        return f.read()

# LOAD TO MYSQL
def load_to_mysql(final_datasets, source_id, engine):
    if not final_datasets:
        return

    logger.info(f"Loading Source ID {source_id} to MySQL...")
    clean_sql = load_query("clean_old_data")
    try:
        with engine.begin() as conn:
            for table_name, df in final_datasets.items():
                if df.empty: continue       
                sql = clean_sql.replace("{{ table_name }}", table_name)        
                conn.execute(text(sql), {"sid": source_id})
                df.to_sql(table_name, con=conn, if_exists='append', index=False, chunksize=2000)
                
        logger.info("Load Completed.")
    except Exception as e:
        logger.error(f"SQL Error: {str(e)}")
        raise e
    
# SEED NEXT JOB
def seed_session_job(**kwargs):
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError("Config.ini not found")
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    engine = get_db_engine(config)
    
    try:
        logger.info("Seeding next job into Metadata...")
        with engine.begin() as conn:
            # run file SQL and run
            sql = load_query('seed_next_job')
            conn.execute(text(sql))
        logger.info("Seeding completed.")
    except Exception as e:
        logger.error(f"Seeding Failed: {e}")
        raise e
    
# SLA COMPLIANCE CHECK
def send_alert(level, message):
    print(f"{level}: {message}")
    
def check_sla_compliance(start_time, end_time, job_status):
    duration = end_time - start_time
    duration = duration.total_seconds() 
    print(f"Status: {job_status}")
    if job_status != 'SUCCESS':
        send_alert("CRITICAL", "Pipeline FAILED! Data is mostly missing or corrupt.")
        return

    # CHECK SLA 1: Performance
    if duration > SLA_CONFIG["MAX_duration"]:
        send_alert("WARNING", f"SLA BREACH: Job took {duration} seconds (Limit: {SLA_CONFIG['MAX_duration']} seconds).")
    else:
        print("Performance SLA: Passed")

    # CHECK SLA 2: Timeliness
    finish_hour = end_time.hour
    if finish_hour >= SLA_CONFIG["DEADLINE_HOUR"]:
        send_alert("WARNING", f"SLA BREACH: Data arrived late at {end_time.strftime('%H:%M')} (Deadline: {SLA_CONFIG['DEADLINE_HOUR']}:00).")
    else:
        print("Timeliness SLA: Passed")

# MAIN EXECUTION
def run_etl_process(**kwargs):
    start_time = datetime.now()
    status = "SUCCESS" # First assume
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError("Config.ini not found")
    
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    
    engine = get_db_engine(config)
    base_url = config['App']['base_url']
    
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)

    # Context from Airflow
    dag_id = kwargs.get('dag').dag_id if kwargs.get('dag') else 'manual_dag'
    task_id = kwargs.get('task').task_id if kwargs.get('task') else 'manual_task'
    run_id = kwargs.get('run_id', 'manual_run')

    current_id = None
    try:
        with engine.begin() as conn:
            sql_get = load_query("get_next_pending")
            result = conn.execute(text(sql_get)).fetchone()
            if not result:
                logger.info("No PENDING files found in Metadata")
                return
            current_id = result[0]
            sql_update = load_query("update_status")
            conn.execute(text(sql_update), {"status": "PROCESSING", "file_id": current_id, "duration": 0})
            logger.info(f"Processing PENDING file id: {current_id}")
    except Exception as e:
        logger.error(f"Metadata error: {e}")
        return 

    logger.info(f"Starting ETL for DAG: {dag_id}, Task: {task_id}")

    temp_file = os.path.join(TEMP_DIR, f"{current_id}.zip")

    try:
        success = extract_data(current_id, base_url, temp_file)
        if not success: 
            raise Exception(f"Download failed for id {current_id}")
        datasets = transform_data(temp_file, current_id)
        total_rows = sum(len(df) for df in datasets.values())
        load_to_mysql(datasets, current_id, engine)
        end_time = datetime.now()

        with engine.begin() as conn:
            sql_update = load_query("update_status")
            conn.execute(text(sql_update), {"status": "SUCCESS", "file_id": current_id, "duration": int((end_time - start_time).total_seconds())})
        log_etl_status(
            engine=engine,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            status="SUCCESS",
            file_id=str(current_id),
            row_count=total_rows
        )            
    except Exception as e:
        logger.error(f"Failed ID {current_id}: {e}")
        error_msg = traceback.format_exc()  
        try:
            with engine.begin() as conn:
                sql_update = load_query("update_status")
                conn.execute(text(sql_update), {"status": "FAILED", "file_id": current_id, "duration": int((datetime.now() - start_time).total_seconds())})
        except:
            pass
        log_etl_status(
            engine=engine,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            status="FAILED",
            file_id=str(current_id),
            error_msg=error_msg
        )
        raise e
    finally:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        check_sla_compliance(start_time, end_time, status)
        # Clean memory
        gc.collect()


import os
import sys
import logging
import configparser
import time
import requests
import zipfile
import pandas as pd
import gc
from sqlalchemy import create_engine, text
from logging.handlers import RotatingFileHandler
import traceback 
from scripts.logger_utils import log_etl_status

# CONFIGURATION
CONFIG_PATH = "/opt/airflow/config.ini"
LOG_DIR = "/opt/airflow/logs"
TEMP_DIR = "/opt/airflow/data/temp"

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
    if vol <= 5: return '1. Retail'
    elif vol <= 20: return '2. Medium'
    elif vol <= 100: return '3. Shark'
    else: return '4. Whale'

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
    running_daily = None
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

            # 2.Daily aggregation
            # For Open/Close, we take the min/max Log_Time of the chunk to sort later
            chunk_daily = chunk.groupby(['Comm', 'Trade_Date'], observed=True).agg({
                'Volume': 'sum', 'Turnover': 'sum', 'Price': ['min', 'max'], 'Log_Time': ['min', 'max']})
            chunk_daily.columns = ['Total_Volume', 'Total_Turnover', 'Low', 'High', 'Time_Min', 'Time_Max']

            # Get First/Last price of chunks
            chunk.sort_values('Log_Time', inplace=True)
            first_prices = chunk.groupby(['Comm', 'Trade_Date'], observed=True)['Price'].first()
            last_prices = chunk.groupby(['Comm', 'Trade_Date'], observed=True)['Price'].last()
            
            chunk_daily = chunk_daily.reset_index()
            chunk_daily['Open_Price'] = chunk_daily.set_index(['Comm', 'Trade_Date']).index.map(first_prices)
            chunk_daily['Close_Price'] = chunk_daily.set_index(['Comm', 'Trade_Date']).index.map(last_prices)
            
            if running_daily is None:
                running_daily = chunk_daily
            else:
                running_daily = pd.concat([running_daily, chunk_daily])
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
            del chunk, chunk_hourly, chunk_daily, chunk_dist
            gc.collect()

        logger.info("Chunks processed. Finalizing datasets...")
        
        final_datasets = {}

        # Finalize hourly
        if running_hourly is not None:
            running_hourly['source_id'] = source_id
            final_datasets['fact_hourly_stats'] = running_hourly

        # Finalize daily 
        if running_daily is not None:
            grouped = running_daily.groupby(['Comm', 'Trade_Date']).agg({
                'Total_Volume': 'sum',
                'Total_Turnover': 'sum',
                'Low': 'min',
                'High': 'max'})
            # Sort by Time_Min to put earliest chunk first
            running_daily.sort_values('Time_Min', inplace=True)
            global_open = running_daily.groupby(['Comm', 'Trade_Date'])['Open_Price'].first()
            # Sort by Time_Max to put latest chunk first
            running_daily.sort_values('Time_Max', inplace=True)
            global_close = running_daily.groupby(['Comm', 'Trade_Date'])['Close_Price'].last()
            
            final_daily = grouped.join(global_open.rename('Open')).join(global_close.rename('Close')).reset_index()
            final_daily['source_id'] = source_id
            final_datasets['fact_daily_summary'] = final_daily

        # Finalize distribution
        if running_dist is not None:
            running_dist['source_id'] = source_id
            final_datasets['fact_trade_distribution'] = running_dist

        return final_datasets

    except Exception as e:
        logger.error(f"Processing Error: {str(e)}")
        raise e

# LOAD TO MYSQL
def load_to_mysql(final_datasets, source_id, engine):
    if not final_datasets:
        return

    logger.info(f"Loading Source ID {source_id} to MySQL...")
    try:
        with engine.begin() as conn:
            for table_name, df in final_datasets.items():
                if df.empty: continue               
                conn.execute(text(f"DELETE FROM {table_name} WHERE source_id = :sid"), {"sid": source_id})
                df.to_sql(table_name, con=conn, if_exists='append', index=False, chunksize=2000)
                
        logger.info("Load Completed.")
    except Exception as e:
        logger.error(f"SQL Error: {str(e)}")
        raise e

# MAIN EXECUTION
def run_etl_process(start_id, count=1, **kwargs):
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError("Config.ini not found")
    
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    
    engine = get_db_engine(config)
    base_url = config['App']['base_url']
    
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)
    dag_id = kwargs.get('dag').dag_id if kwargs.get('dag') else 'manual_dag'
    task_id = kwargs.get('task').task_id if kwargs.get('task') else 'manual_task'
    run_id = kwargs.get('run_id', 'manual_run')

    logger.info(f"Starting ETL for DAG: {dag_id}, Task: {task_id}")
    for i in range(count):
        current_id = start_id + i
        temp_file = os.path.join(TEMP_DIR, f"{current_id}.zip")

        try:
            success = extract_data(current_id, base_url, temp_file)
            if not success: continue
            datasets = transform_data(temp_file, current_id)
            total_rows = sum(len(df) for df in datasets.values())
            load_to_mysql(datasets, current_id, engine)
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
            logger.error(f"Failed ID {current_id}")
            error_msg = traceback.format_exc()            
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
            # Clean memory
            gc.collect()


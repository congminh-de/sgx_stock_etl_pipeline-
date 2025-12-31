import pandas as pd
import datetime

def log_etl_status(engine, dag_id, task_id, run_id, status, file_id=None, row_count=0, error_msg=None):
    try:
        log_data = {
            'dag_id': dag_id,
            'task_id': task_id,
            'run_id': run_id,
            'file_id': str(file_id) if file_id else None,
            'status': status,
            'execution_time': datetime.datetime.now(),
            'row_count': row_count,
            'error_message': str(error_msg) if error_msg else None}
        df_log = pd.DataFrame([log_data])
        df_log.to_sql('etl_job_logs', con=engine, if_exists='append', index=False)
        
        print(f"[LOGGING] Task {task_id} | File {file_id} | Status {status}")

    except Exception as e:
        print(f"[LOGGING ERROR] Could not write to DB: {e}")
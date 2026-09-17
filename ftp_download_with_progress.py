from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.ftp.hooks.ftp import FTPHook
from datetime import datetime, timedelta
import logging
import os

# Define a custom progress callback
def progress_callback(data):
    logging.info(f"Retrieved {len(data)} bytes.")

def download_file_from_ftp():
    ftp_hook = FTPHook(ftp_conn_id='NAAS_FTP_LOCATION')
    local_file = '/opt/airflow/importfile/templocation/small/B2B_Segment_Report.csv'
    remote_file = f'/DWH_File_Location/small/B2B_Segment_Report_{datetime.now().strftime("%Y%m%d")}.csv'

    # Log the start of the file retrieval process
    logging.info(f"Starting to retrieve file: {remote_file} from FTP.")

    # Create a local file and download in chunks to log progress
    with open(local_file, 'wb') as f:
        def callback(data):
            f.write(data)  # Write data to the file
            progress_callback(data)  # Call the progress logging function

        # Use the FTP hook's connection to retrieve the file
        ftp_conn = ftp_hook.get_conn()
        ftp_conn.retrbinary(f'RETR {remote_file}', callback)
    
    logging.info(f"Successfully retrieved file: {remote_file} to {local_file}.")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG('ftp_download_with_progress_logging',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    download_task = PythonOperator(
        task_id='download_file',
        python_callable=download_file_from_ftp,
    )

    download_task

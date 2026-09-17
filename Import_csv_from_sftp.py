from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ftp.hooks.ftp import FTPHook 
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import ftplib
import logging
import requests

dag = DAG(
    'sftp_file_import_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
)

# Function to check for file existence on FTP
def wait_for_file_arrival():
    # Get the Airflow logger
    logger = logging.getLogger("airflow.task")
    
    ftp_host = '27.147.159.194'
    port = 3741
    ftp_user = 'mahmud'
    ftp_pass = 'ssle@mr72'
    remote_file_name = f'B2B_Segment_Report_{datetime.now().strftime("%Y%m%d")}.csv'
    logger.info("File name Generated: " + remote_file_name)
    
    with ftplib.FTP() as ftp:
        ftp.connect(ftp_host, port)
        ftp.login(user=ftp_user, passwd=ftp_pass)
        # Change to the desired directory
        ftp.cwd('/DWH_File_Location/small')
        files = ftp.nlst()  # List files in the current directory       
        
        if remote_file_name in files:
            logger.info("B2B_Segment_Report_ file found")
            return True
        else:
            logger.info("B2B_Segment_Report_ file Not found")
            return False
        
wait_for_file_arrival_task = PythonOperator(
    task_id='wait_for_file_arrival',
    python_callable=wait_for_file_arrival,
    dag=dag
) 
   
#wait_for_file_arrival_task = PythonSensor(
#    task_id='wait_for_file_arrival',
#    python_callable=wait_for_file_arrival,
#    mode='poke',
#    poke_interval=60,
#    timeout=3600,  # Timeout after 10 minutes
#)
    
def import_file_from_sftp(ftp_conn_id, local_file):
    x_filename = f'/DWH_File_Location/small/B2B_Segment_Report_{datetime.now().strftime("%Y%m%d")}.csv'
    ftp = FTPHook(ftp_conn_id)
    ftp.retrieve_file(x_filename, local_file)

import_file_from_sftp_task = PythonOperator(
    task_id='import_file_from_sftp',
    python_callable=import_file_from_sftp,
    op_kwargs={
        'ftp_conn_id': 'NAAS_FTP_LOCATION',
        'local_file': '/opt/airflow/importfile/templocation/small/B2B_Segment_Report.csv'
    },
    dag=dag
)
    
def successfull_message():
    print('successfull message')

successfull_message_task = PythonOperator(
    task_id='successfull_message',
    python_callable=successfull_message,
    dag=dag
)

# Set the dependencies between the tasks
wait_for_file_arrival_task >> import_file_from_sftp_task >> successfull_message_task
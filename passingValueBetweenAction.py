from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

dag = DAG(
    'passing_value_action',
    default_args={'start_date': days_ago(1)},
    schedule_interval='0 23 * * *',
    catchup=False
)

def starting_dags():   
    logger = logging.getLogger("airflow.task")
    logger.info("starting dags")
    
starting_dags_task = PythonOperator(
    task_id='starting_dags',
    python_callable=starting_dags,
    dag=dag
)

def generate_file_and_location_name(ti):   
    print('Generate File Name')
    x_filename = f'/DWH_File_Location/B2B_Segment_Report_{datetime.now().strftime("%Y%m%d")}.csv'
    ti.xcom_push(key='x_filename', value=x_filename)
    logger = logging.getLogger("airflow.task")
    logger.info("File name Generated: " + x_filename)
    
generate_file_and_location_name_task = PythonOperator(
    task_id='generate_file_and_location_name',
    python_callable=generate_file_and_location_name,
    dag=dag
)

def wait_for_file_arrival(ti):
    x_filename = ti.xcom_pull(task_ids='generate_file_name', key='x_filename')
    logger = logging.getLogger("airflow.task")
    logger.info("File name Received in wait_for_file_arrival: " + x_filename)
        
wait_for_file_arrival_task = PythonOperator(
    task_id='wait_for_file_arrival',
    python_callable=wait_for_file_arrival,
    dag=dag
) 

def import_file_from_sftp(ti):
    x_filename = ti.xcom_pull(task_ids='generate_file_name', key='x_filename')
    logger = logging.getLogger("airflow.task")
    logger.info("File name Received in import_file_from_sftp: " + x_filename)

import_file_from_sftp_task = PythonOperator(
    task_id='import_file_from_sftp',
    python_callable=import_file_from_sftp,
    dag=dag
)

def export_file_by_sftp(ti):
    x_filename = ti.xcom_pull(task_ids='generate_file_name', key='x_filename')
    logger = logging.getLogger("airflow.task")
    logger.info("File name Received in export_file_by_sftp: " + x_filename)

export_file_by_sftp_task = PythonOperator(
    task_id='export_file_by_sftp',
    python_callable=export_file_by_sftp,
    dag=dag
)

def upload_file_in_database(ti):
    x_filename = ti.xcom_pull(task_ids='generate_file_name', key='x_filename')
    logger = logging.getLogger("airflow.task")
    logger.info("File name Received in upload_file_in_database: " + x_filename)

upload_file_in_database_task = PythonOperator(
    task_id='upload_file_in_database',
    python_callable=upload_file_in_database,
    dag=dag
)

def check_status_in_database(ti):
    x_filename = ti.xcom_pull(task_ids='generate_file_name', key='x_filename')
    logger = logging.getLogger("airflow.task")
    logger.info("File name Received in check_status_in_database: " + x_filename)

check_status_in_database_task = PythonOperator(
    task_id='check_status_in_database',
    python_callable=check_status_in_database,
    dag=dag
)
     
def successfull_message(ti):
    x_filename = ti.xcom_pull(task_ids='generate_file_name', key='x_filename')
    logger = logging.getLogger("airflow.task")
    logger.info("File name Received in successfull_message: " + x_filename)

successfull_message_task = PythonOperator(
    task_id='successfull_message',
    python_callable=successfull_message,
    dag=dag
)

# Set the dependencies between the tasks
starting_dags_task #>> generate_file_and_location_name_task >> wait_for_file_arrival_task >> import_file_from_sftp_task >> export_file_by_sftp_task >> upload_file_in_database_task >> check_status_in_database_task >> successfull_message_task
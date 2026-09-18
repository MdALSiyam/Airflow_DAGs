from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import csv
import os
import logging

# Define connection IDs
ORACLE_CONN_ID = 'oracle_default'
SFTP_CONN_ID = 'sftp_default'

default_args = {
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'Example_Test_Data_Sync',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False,
    description='Export data from Oracle to CSV and upload to SFTP'
)

def starting_dags():
    """Logs the start of the DAG."""
    logger = logging.getLogger("airflow.task")
    logger.info("Starting DAG")

def fetch_data_from_oracle(**kwargs):
    """
    Fetches data from the Oracle database using an Airflow connection and creates a CSV file.
    """
    # Dynamic local file name with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_file_path = f'/tmp/Test_Report_{timestamp}.csv'

    try:
        # Use the Airflow OracleHook with the connection ID
        oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
        print("Connected to Oracle database successfully using Airflow Connection")

        sql_query = """
        SELECT * FROM tblbirequest
        WHERE TRUNC(create_date) = TRUNC(SYSDATE - 1)
        """

        connection = oracle_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql_query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

        print(f"Total rows fetched: {len(rows)}")
        if len(rows) == 0:
            print("No data fetched. Please check the query or data availability.")
        
        with open(local_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(columns)
            
            # Convert binary data to string representation
            for row in rows:
                converted_row = []
                for value in row:
                    try:
                        # Handle binary data (BLOB columns) - check if it has read() method (LOB objects)
                        if hasattr(value, 'read'):
                            binary_data = value.read()
                            if binary_data:
                                converted_row.append(binary_data.hex())
                            else:
                                converted_row.append('')
                        # Handle bytes objects
                        elif isinstance(value, bytes):
                            converted_row.append(value.hex())
                        # Handle None values
                        elif value is None:
                            converted_row.append('')
                        # Handle all other types
                        else:
                            converted_row.append(str(value))
                    except Exception as e:
                        # If anything fails, use empty string as fallback
                        print(f"Warning: Error converting value {type(value)}: {e}")
                        converted_row.append('')
                writer.writerow(converted_row)

        print(f"CSV file created successfully: {local_file_path}")

        cursor.close()
        connection.close()
        return local_file_path

    except Exception as e:
        print(f"Error fetching data from Oracle: {str(e)}")
        raise

def upload_csv_to_sftp(**kwargs):
    """
    Uploads the CSV file to the SFTP location using an Airflow connection.
    """
    ti = kwargs['ti']
    local_file_path = ti.xcom_pull(task_ids='fetch_data_task')

    if not local_file_path:
        raise ValueError("CSV file path not found from previous task")

    # Extract timestamp from local file name for remote path
    file_name = os.path.basename(local_file_path)
    remote_path = f'/Database_Destination_Location/sm/{file_name}'

    try:
        sftp_hook = SFTPHook(ftp_conn_id=SFTP_CONN_ID)
        print("Connected to SFTP server successfully using Airflow Connection")

        sftp_hook.store_file(remote_path, local_file_path)

        print(f"File uploaded successfully to: {remote_path}")

        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Local file deleted: {local_file_path}")

    except Exception as e:
        print(f"Error uploading file to SFTP: {str(e)}")
        raise

def cleanup_temp_files(**kwargs):
    """Cleans up the temporary files."""
    ti = kwargs['ti']
    local_file_path = ti.xcom_pull(task_ids='fetch_data_task')
    
    if local_file_path and os.path.exists(local_file_path):
        os.remove(local_file_path)
        print(f"Cleaned up temporary file: {local_file_path}")
    else:
        print("No temporary file to clean up")

def successfull_message(**kwargs):
    """Logs a success message."""
    logger = logging.getLogger("airflow.task")
    logger.info("DAG executed successfully")

# Define tasks
starting_dags_task = PythonOperator(
    task_id='starting_dags',
    python_callable=starting_dags,
    dag=dag
)

fetch_data_task = PythonOperator(
    task_id='fetch_data_task',
    python_callable=fetch_data_from_oracle,
    provide_context=True,
    dag=dag,
)

upload_to_sftp_task = PythonOperator(
    task_id='upload_to_sftp_task',
    python_callable=upload_csv_to_sftp,
    provide_context=True,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_task',
    python_callable=cleanup_temp_files,
    provide_context=True,
    dag=dag,
)

successfull_message_task = PythonOperator(
    task_id='successfull_message',
    python_callable=successfull_message,
    dag=dag,
)

# Set task dependencies
starting_dags_task >> fetch_data_task >> upload_to_sftp_task >> cleanup_task >> successfull_message_task


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ftp.hooks.ftp import FTPHook 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.models import Variable
import ftplib
import psycopg2
import logging
import requests

dag = DAG(
    'sample_data_upload_process',
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
    x_filename = f'B2B_Segment_Report'
    ti.xcom_push(key='x_filename', value=x_filename)
    x_table_name = f'TBL_B2B_SEGMENT_REPORT_UPLOAD'
    ti.xcom_push(key='x_table_name', value=x_table_name)
    
    source_loc_name = f'/DWH_File_Location/small/'
    ti.xcom_push(key='source_loc_name', value=source_loc_name)   
    source_file_name = f'B2B_Segment_Report_{datetime.now().strftime("%Y%m%d")}.csv'
    ti.xcom_push(key='source_file_name', value=source_file_name)
    
    local_loc_name = f'/opt/airflow/importfile/templocation/small/'
    ti.xcom_push(key='local_loc_name', value=local_loc_name)   
    local_file_name = f'B2B_Segment_Report.csv'
    ti.xcom_push(key='local_file_name', value=local_file_name)
    
    dest_loc_name = f'/database_file_location/small/'
    ti.xcom_push(key='dest_loc_name', value=dest_loc_name)   
    dest_file_name = f'B2B_Segment_Report.csv'
    ti.xcom_push(key='dest_file_name', value=dest_file_name)
    dest_file_location_name = f'E:\ETL_Expriment\SampleData\B2B_Sample_DWH_File.csv'
    ti.xcom_push(key='dest_file_location_name', value=dest_file_location_name)
    
    ftp_conn_id = f'NAAS_FTP_LOCATION'
    ti.xcom_push(key='ftp_conn_id', value=ftp_conn_id)
    ftp_var_id = f'NAAS_FTP_INFO'
    ti.xcom_push(key='ftp_var_id', value=ftp_var_id)
    db_conn_id = f'LOCAL_POSTGRES_POSTGRES'
    ti.xcom_push(key='db_conn_id', value=db_conn_id)
    db_var_id = f'LOCAL_POSTGRES_POSTGRES'
    ti.xcom_push(key='db_var_id', value=db_var_id)
    
    logger = logging.getLogger("airflow.task")
    logger.info("File name Generated: " + x_filename)
    
generate_file_and_location_name_task = PythonOperator(
    task_id='generate_file_and_location_name',
    python_callable=generate_file_and_location_name,
    dag=dag
)

def wait_for_file_arrival(ti):
    try:
        source_file_name = ti.xcom_pull(task_ids='generate_file_and_location_name', key='source_file_name')
        source_loc_name = ti.xcom_pull(task_ids='generate_file_and_location_name', key='source_loc_name')
        ftp_var_id = ti.xcom_pull(task_ids='generate_file_and_location_name', key='ftp_var_id')
        
        logger = logging.getLogger("airflow.task")
        logger.info("File name Received in wait_for_file_arrival: " + source_file_name)

        # Get the JSON variable and deserialize it
        config = Variable.get(ftp_var_id, deserialize_json=True)
        # Access individual values from the JSON      
        ftp_host =  config["ftp_host"]
        port =  config["port"]
        ftp_user =  config["ftp_user"]
        ftp_pass =  config["ftp_pass"]
        logger.info("FTP Information Generated: " + ftp_var_id)
        
        with ftplib.FTP() as ftp:
            ftp.connect(ftp_host, port)
            ftp.login(user=ftp_user, passwd=ftp_pass)
            # Change to the desired directory
            ftp.cwd(source_loc_name)
            files = ftp.nlst()  # List files in the current directory       
            
            if source_file_name in files:
                logger.info(source_file_name + " file found")
                return True
            else:
                logger.info(source_file_name + " file Not found")
                return False
            
    except Exception as e:
        logger.error(f"An error occurred at wait_for_file_arrival: {e}")
        return False
        
wait_for_file_arrival_task = PythonOperator(
    task_id='wait_for_file_arrival',
    python_callable=wait_for_file_arrival,
    dag=dag
) 

def import_file_from_sftp(ti):
    try:
        source_loc_name = ti.xcom_pull(task_ids='generate_file_and_location_name', key='source_loc_name')
        source_file_name = ti.xcom_pull(task_ids='generate_file_and_location_name', key='source_file_name')
        source_loc_file_name = source_loc_name + source_file_name
        local_loc_name = ti.xcom_pull(task_ids='generate_file_and_location_name', key='local_loc_name')
        local_file_name = ti.xcom_pull(task_ids='generate_file_and_location_name', key='local_file_name')
        local_loc_file_name = local_loc_name + local_file_name
        ftp_conn_id = ti.xcom_pull(task_ids='generate_file_and_location_name', key='ftp_conn_id')
        
        logger = logging.getLogger("airflow.task")
        logger.info("File name Source location in import_file_from_sftp: " + source_loc_file_name)
        logger.info("File name local location in import_file_from_sftp: " + local_loc_file_name)
        
        ftp = FTPHook(ftp_conn_id)
        ftp.retrieve_file(source_loc_file_name, local_loc_file_name)       
    except Exception as e:
        logger.error(f"An error occurred at import_file_from_sftp: {e}")
        return False

import_file_from_sftp_task = PythonOperator(
    task_id='import_file_from_sftp',
    python_callable=import_file_from_sftp,
    dag=dag
)

def export_file_by_sftp(ti):
    try:
        dest_loc_name = ti.xcom_pull(task_ids='generate_file_and_location_name', key='dest_loc_name')
        dest_file_name = ti.xcom_pull(task_ids='generate_file_and_location_name', key='dest_file_name')
        dest_loc_file_name = dest_loc_name + dest_file_name
        local_loc_name = ti.xcom_pull(task_ids='generate_file_and_location_name', key='local_loc_name')
        local_file_name = ti.xcom_pull(task_ids='generate_file_and_location_name', key='local_file_name')
        local_loc_file_name = local_loc_name + local_file_name
        ftp_conn_id = ti.xcom_pull(task_ids='generate_file_and_location_name', key='ftp_conn_id')
        
        logger = logging.getLogger("airflow.task")
        logger.info("File name destination location in export_file_by_sftp: " + dest_loc_file_name)
        logger.info("File name local location in export_file_by_sftp: " + local_loc_file_name)
        
        ftp = FTPHook(ftp_conn_id)
        # Open the local file in binary mode for reading
        with open(local_loc_file_name, 'rb') as file_data:
            # Upload the file to the remote FTP location
            ftp.store_file(dest_loc_file_name, file_data)
            
    except Exception as e:
        logger.error(f"An error occurred at export_file_by_sftp: {e}")
        return False

export_file_by_sftp_task = PythonOperator(
    task_id='export_file_by_sftp',
    python_callable=export_file_by_sftp,
    dag=dag
)

def upload_file_in_database(ti):
    try:
        x_filename = ti.xcom_pull(task_ids='generate_file_and_location_name', key='x_filename')
        db_var_id = ti.xcom_pull(task_ids='generate_file_and_location_name', key='db_var_id')     
        logger = logging.getLogger("airflow.task")
        logger.info("File name Received in upload_file_in_database: " + x_filename)
        
        # Get the JSON variable and deserialize it
        config = Variable.get(db_var_id, deserialize_json=True)
        
        # Establish a connection to the PostgreSQL database
        connection = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"]
        )

        # Create a cursor object
        cursor = connection.cursor()

        # Define a multi-line SQL query with placeholders
        query = """
            INSERT INTO AIRFLOW_DATA_UPLOAD_TRACK (TABLE_NAME)
            VALUES (%s)
            RETURNING ID;
        """

        # Execute the query with the variables
        cursor.execute(query, (x_filename))

        # Fetch the result (if needed, e.g., for the RETURNING clause)
        table_id = cursor.fetchone()[0]

        # Commit the transaction
        connection.commit()

        # Close the cursor and connection
        cursor.close()
        connection.close()

        # Print the result
        logger.info(f"Inserted user with ID: {table_id}")
        ti.xcom_push(key='table_id', value=table_id)
        
    except Exception as e:
        logger.error(f"An error occurred at upload_file_in_database: {e}")
        return False

def upload_csv_to_postgres(conn, csv_file, table_name):
    try:
        logger = logging.getLogger("airflow.task")
        cursor = conn.cursor()
        # Prepare the COPY command with placeholders
        copy_sql = f"COPY {table_name} FROM '{csv_file}' DELIMITER '|' CSV HEADER"
        # Execute the prepared statement with parameters
        cursor.execute(copy_sql)
        conn.commit()
        logger.error(f"CSV data uploaded successfully to table '{table_name}'.")
    except psycopg2.Error as e:
        logger.error(f"An error occurred at upload_file_in_database: {e}")
        conn.rollback()
    
def upload_file_in_database_v2(ti):
    try:
        x_table_name = ti.xcom_pull(task_ids='generate_file_and_location_name', key='x_table_name')
        dest_file_location_name = ti.xcom_pull(task_ids='generate_file_and_location_name', key='dest_file_location_name')
        db_conn_id = ti.xcom_pull(task_ids='generate_file_and_location_name', key='db_conn_id')
        logger = logging.getLogger("airflow.task")
        logger.info("File name Received in upload_file_in_database_v2: " + dest_file_location_name)
        
        postgres_hook = PostgresHook(postgres_conn_id=db_conn_id)  
        logger.info(f"upload_file_in_database_v2, connection stablished")
              
        query = "COPY "+x_table_name+" FROM '"+dest_file_location_name+"' DELIMITER '|' CSV HEADER;"
        postgres_hook.run(query)
        query = "INSERT INTO AIRFLOW_DATA_UPLOAD_TRACK (TABLE_NAME) VALUES ('"+x_table_name+"');"
        postgres_hook.run(query)
        
        # Define the SQL query
        sql = "SELECT MAX(ID) TABLE_ID  FROM AIRFLOW_DATA_UPLOAD_TRACK WHERE TABLE_NAME = '"+x_table_name+"';"
        # Run the query
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(sql)
        # Fetch one row (you can also use fetchall() or fetchmany())
        result = cursor.fetchone()  # or cursor.fetchall() for all rows
        # Print or log the result (or return if needed)
        table_id = result[0]
        # Print the result
        logger.info(f"Inserted user with ID: {table_id}")
        ti.xcom_push(key='table_id', value=table_id)
        
    except Exception as e:
        logger.error(f"An error occurred at upload_file_in_database: {e}")
        return False

upload_file_in_database_task = PythonOperator(
    task_id='upload_file_in_database',
    python_callable=upload_file_in_database_v2,
    dag=dag
)

def update_status_in_database(ti):
    try:
        x_filename = ti.xcom_pull(task_ids='generate_file_and_location_name', key='x_filename')
        db_conn_id = ti.xcom_pull(task_ids='generate_file_and_location_name', key='db_conn_id')     
        table_id = ti.xcom_pull(task_ids='upload_file_in_database', key='table_id')  
        logger = logging.getLogger("airflow.task")
        logger.info("File name Received in check_status_in_database: " + x_filename)
        
        # Initialize the PostgresHook with the connection ID 
        postgres_hook = PostgresHook(postgres_conn_id=db_conn_id)   
        # Define your SQL query (example: selecting from a table)
        #WHERE ID = %s;
        query = "UPDATE AIRFLOW_DATA_UPLOAD_TRACK SET STATUS = 'Upload Completed', UPLOAD_END_TIME = CURRENT_TIMESTAMP, UPDATED_TIME = CURRENT_TIMESTAMP WHERE ID = " + table_id +";"
        # Execute the query using PostgresHook's 'run' method
        postgres_hook.run(query)
    except Exception as e:
        logger.error(f"An error occurred at check_status_in_database: {e}")
        return False

update_status_in_database_task = PythonOperator(
    task_id='update_status_in_database',
    python_callable=update_status_in_database,
    dag=dag
)
     
def successfull_message(ti):
    try:
        logger = logging.getLogger("airflow.task")
        logger.info("Dag executed")
    except Exception as e:
        logger.error(f"An error occurred at successfull_message: {e}")
        return False

successfull_message_task = PythonOperator(
    task_id='successfull_message',
    python_callable=successfull_message,
    dag=dag
)

# Set the dependencies between the tasks
starting_dags_task >> generate_file_and_location_name_task >> wait_for_file_arrival_task >> import_file_from_sftp_task >> export_file_by_sftp_task >> upload_file_in_database_task >> update_status_in_database_task >> successfull_message_task
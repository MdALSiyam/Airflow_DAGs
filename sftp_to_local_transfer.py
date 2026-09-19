from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
import os
import logging
from airflow.exceptions import AirflowException

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# DAG definition
dag = DAG(
    'sftp_to_local_transfer',
    default_args=default_args,
    description='Transfer CSV files from SFTP to local folder daily',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=datetime(2025, 9, 3),
    catchup=False,
    tags=['sftp', 'csv', 'file_transfer'],
)

# Configuration - Using your specific paths
SFTP_CONN_ID = 'sftp_default'  # Using your existing connection
SFTP_REMOTE_PATH = '/Database_Destination_Location/sm/'  # Your SFTP path
LOCAL_DESTINATION_PATH = '/opt/airflow/data/dump_data/'  # Changed to container path

def check_sftp_connection(**kwargs):
    """
    Test SFTP connection before attempting file transfer
    """
    try:
        hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
        sftp_client = hook.get_conn()
        
        # Try to list directory to verify connection works
        files = sftp_client.listdir(SFTP_REMOTE_PATH)
        logging.info(f"SFTP connection successful. Found {len(files)} items in directory.")
        
        # Log first few files for debugging
        if files:
            logging.info(f"Sample files: {files[:5]}")
        
        sftp_client.close()
        return f"SFTP connection verified successfully. Found {len(files)} items."
        
    except Exception as e:
        error_msg = f"SFTP connection failed: {str(e)}"
        logging.error(error_msg)
        raise AirflowException(error_msg)

def transfer_sftp_files(**kwargs):
    """
    Transfer all CSV files from SFTP to local directory
    """
    try:
        # Create local directory if it doesn't exist
        os.makedirs(LOCAL_DESTINATION_PATH, exist_ok=True)
        logging.info(f"Ensured local directory exists: {LOCAL_DESTINATION_PATH}")
        
        # Establish SFTP connection using your existing connection
        hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
        logging.info(f"Connected to SFTP server using connection: {SFTP_CONN_ID}")
        
        # Get SFTP client
        sftp_client = hook.get_conn()
        
        # Change to remote directory
        sftp_client.chdir(SFTP_REMOTE_PATH)
        
        # List all files in remote directory
        remote_files = sftp_client.listdir()
        logging.info(f"Found {len(remote_files)} items in remote directory")
        
        # Filter for CSV files (case insensitive)
        csv_files = [f for f in remote_files if f.lower().endswith('.csv')]
        
        if not csv_files:
            # Also check for other possible CSV extensions
            csv_files = [f for f in remote_files if any(f.lower().endswith(ext) for ext in ['.csv', '.CSV'])]
        
        if not csv_files:
            logging.warning("No CSV files found in remote directory. Available files:")
            for file in remote_files[:10]:  # Log first 10 files for debugging
                logging.warning(f"  - {file}")
            if len(remote_files) > 10:
                logging.warning(f"  ... and {len(remote_files) - 10} more files")
            
            sftp_client.close()
            return "No CSV files to transfer"
        
        logging.info(f"Found {len(csv_files)} CSV files: {csv_files}")
        
        # Transfer each CSV file
        transferred_files = []
        for csv_file in csv_files:
            try:
                remote_file_path = os.path.join(SFTP_REMOTE_PATH, csv_file)
                local_file_path = os.path.join(LOCAL_DESTINATION_PATH, csv_file)
                
                # Check if file already exists locally
                if os.path.exists(local_file_path):
                    # Compare file sizes to see if it's been updated
                    remote_size = sftp_client.stat(remote_file_path).st_size
                    local_size = os.path.getsize(local_file_path)
                    
                    if remote_size == local_size:
                        logging.info(f"File {csv_file} already exists with same size, skipping")
                        continue
                    else:
                        logging.info(f"File {csv_file} exists but sizes differ (remote: {remote_size}, local: {local_size}), will transfer")
                
                # Download file
                logging.info(f"Transferring {csv_file} from SFTP to local")
                sftp_client.get(remote_file_path, local_file_path)
                transferred_files.append(csv_file)
                
                # Verify transfer was successful
                if os.path.exists(local_file_path):
                    local_size = os.path.getsize(local_file_path)
                    remote_size = sftp_client.stat(remote_file_path).st_size
                    
                    if local_size == remote_size:
                        logging.info(f"Successfully transferred: {csv_file} (size: {remote_size} bytes)")
                    else:
                        logging.warning(f"Size mismatch after transfer: {csv_file} (remote: {remote_size}, local: {local_size})")
                else:
                    logging.error(f"Failed to verify transfer of {csv_file} - file not found locally")
                    
            except Exception as file_error:
                logging.error(f"Failed to transfer {csv_file}: {str(file_error)}")
                continue
        
        # Close connection
        sftp_client.close()
        
        if transferred_files:
            result = f"Transferred {len(transferred_files)} files: {', '.join(transferred_files)}"
            logging.info(result)
            return result
        else:
            logging.info("No new files transferred (all files already exist locally)")
            return "No new files transferred"
        
    except Exception as e:
        error_msg = f"Error transferring files: {str(e)}"
        logging.error(error_msg)
        raise AirflowException(error_msg)

def list_transferred_files(**kwargs):
    """
    List all files in the local destination directory
    """
    try:
        if not os.path.exists(LOCAL_DESTINATION_PATH):
            logging.warning(f"Local directory does not exist: {LOCAL_DESTINATION_PATH}")
            return "No local directory found"
        
        files = os.listdir(LOCAL_DESTINATION_PATH)
        csv_files = [f for f in files if f.lower().endswith('.csv')]
        
        logging.info(f"Found {len(csv_files)} CSV files in local directory: {LOCAL_DESTINATION_PATH}")
        for csv_file in csv_files:
            file_path = os.path.join(LOCAL_DESTINATION_PATH, csv_file)
            file_size = os.path.getsize(file_path)
            logging.info(f"  - {csv_file} ({file_size} bytes)")
        
        return f"Found {len(csv_files)} CSV files locally"
        
    except Exception as e:
        error_msg = f"Error listing local files: {str(e)}"
        logging.error(error_msg)
        raise AirflowException(error_msg)

# Define tasks
check_connection_task = PythonOperator(
    task_id='check_sftp_connection',
    python_callable=check_sftp_connection,
    dag=dag,
)

transfer_task = PythonOperator(
    task_id='transfer_csv_files',
    python_callable=transfer_sftp_files,
    dag=dag,
)

list_files_task = PythonOperator(
    task_id='list_transferred_files',
    python_callable=list_transferred_files,
    dag=dag,
)

# Set task dependencies
check_connection_task >> transfer_task >> list_files_task
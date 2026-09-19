from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
import socket
import os
import pandas as pd
import json
import tempfile

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'smb_csv_transfer',
    default_args=default_args,
    description='SMB to SFTP CSV file transfer',
    schedule_interval='0 8 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['smb', 'csv', 'transfer', 'sftp'],
)

SMB_CONFIG = {
    'csv_filename': f"RETAILER_GPS_{(datetime.now() - timedelta(days=1)).strftime('%Y%m%d')}.csv",
    'local_download_path': '/opt/airflow/data/dump_data/',
    'smb_connection_id': 'smb_connection',
    'sftp_connection_id': 'sftp_default',
    'sftp_remote_path': '/biometric_nfs_share/oracle/dump_biometric/',
    'batch_size': 10000,
}

def get_smb_credentials():
    conn = BaseHook.get_connection(SMB_CONFIG['smb_connection_id'])
    smb_config = {
        'server': conn.host,
        'username': conn.login,
        'password': conn.password,
        'share': '',
        'remote_path': ''
    }
    
    if conn.extra:
        extra_params = json.loads(conn.extra)
        smb_config.update({
            'share': extra_params.get('share_path', ''),
            'remote_path': extra_params.get('sub_path', '')
        })
    
    return smb_config

def test_smb_connection():
    smb_config = get_smb_credentials()
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    result = sock.connect_ex((smb_config['server'], 445))
    sock.close()
    
    if result != 0:
        raise Exception("SMB server not reachable")
    
    print("✓ SMB connection test passed")
    return True

def download_csv_from_smb():
    smb_config = get_smb_credentials()
    os.makedirs(SMB_CONFIG['local_download_path'], exist_ok=True)
    
    local_file_path = os.path.join(SMB_CONFIG['local_download_path'], SMB_CONFIG['csv_filename'])
    remote_file_path = f"{smb_config['remote_path']}/{SMB_CONFIG['csv_filename']}"
    
    print(f"Downloading: {remote_file_path}")
    
    try:
        from smb.SMBConnection import SMBConnection
        
        conn = SMBConnection(
            username=smb_config['username'],
            password=smb_config['password'],
            my_name='airflow-worker',
            remote_name=smb_config['server'],
            use_ntlm_v2=True,
            is_direct_tcp=True
        )
        
        connected = conn.connect(smb_config['server'], 445) or conn.connect(smb_config['server'], 139)
        if not connected:
            raise Exception("Failed to connect to SMB server")
        
        with open(local_file_path, 'wb') as local_file:
            file_attributes, file_size = conn.retrieveFile(smb_config['share'], remote_file_path, local_file)
        
        file_size_mb = file_size / (1024 * 1024)
        print(f"✓ Downloaded: {file_size_mb:.2f} MB")
        conn.close()
        
    except Exception as e:
        raise Exception(f"SMB download failed: {str(e)}")
    
    return local_file_path

def transfer_complete_file_to_sftp(file_path):
    try:
        hook = SFTPHook(ftp_conn_id=SMB_CONFIG['sftp_connection_id'])
        remote_path = f"{SMB_CONFIG['sftp_remote_path']}{SMB_CONFIG['csv_filename']}"
        
        try:
            hook.list_directory(SMB_CONFIG['sftp_remote_path'])
        except:
            hook.create_directory(SMB_CONFIG['sftp_remote_path'])
        
        hook.store_file(remote_path, file_path)
        print(f"✓ Uploaded to SFTP: {remote_path}")
        return True
        
    except Exception as e:
        print(f"SFTP transfer failed: {e}")
        return False

def process_and_transfer_complete_file(**context):
    local_file_path = context['ti'].xcom_pull(task_ids='download_csv_from_smb')
    
    if not local_file_path or not os.path.exists(local_file_path):
        raise Exception("CSV file not found")
    
    file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
    print(f"Processing: {file_size_mb:.2f} MB")
    
    total_rows = 0
    
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as temp_file:
            processed_file_path = temp_file.name
            first_batch = True
            
            for batch_number, batch_df in enumerate(pd.read_csv(local_file_path, chunksize=SMB_CONFIG['batch_size'], low_memory=False), 1):
                batch_rows = len(batch_df)
                total_rows += batch_rows
                
                if first_batch:
                    batch_df.iloc[0:0].to_csv(temp_file, index=False)
                    first_batch = False
                
                batch_df.to_csv(temp_file, index=False, header=False)
                print(f"✓ Batch {batch_number}: {batch_rows} rows")
            
            temp_file.close()
            
            print(f"✓ Processing completed: {total_rows:,} total rows")
            
            if transfer_complete_file_to_sftp(processed_file_path):
                print("✓ File transferred to SFTP")
            else:
                raise Exception("SFTP transfer failed")
        
        if os.path.exists(processed_file_path):
            os.remove(processed_file_path)
        
        return {
            'file_path': local_file_path,
            'file_size_mb': file_size_mb,
            'total_rows': total_rows,
        }
        
    except Exception as e:
        if 'processed_file_path' in locals() and os.path.exists(processed_file_path):
            os.remove(processed_file_path)
        raise

def transfer_summary(**context):
    result = context['ti'].xcom_pull(task_ids='validate_csv_file')
    smb_config = get_smb_credentials()
    
    print("=" * 40)
    print("TRANSFER SUMMARY")
    print("=" * 40)
    print(f"File: {SMB_CONFIG['csv_filename']}")
    print(f"Size: {result.get('file_size_mb', 0):.2f} MB")
    print(f"Rows: {result.get('total_rows', 0):,}")
    print(f"Source: {smb_config['server']}")
    print(f"Destination: {SMB_CONFIG['sftp_remote_path']}")
    print("✓ TRANSFER SUCCESSFUL!")
    print("=" * 40)

# Tasks - EXACT SAME NAMES
start_task = DummyOperator(task_id='start_transfer', dag=dag)
test_connection_task = PythonOperator(
    task_id='test_smb_connection',
    python_callable=test_smb_connection,
    dag=dag,
)
download_task = PythonOperator(
    task_id='download_csv_from_smb',
    python_callable=download_csv_from_smb,
    dag=dag,
)
validate_task = PythonOperator(
    task_id='validate_csv_file',
    python_callable=process_and_transfer_complete_file,
    dag=dag,
)
summary_task = PythonOperator(
    task_id='transfer_summary',
    python_callable=transfer_summary,
    dag=dag,
)
end_task = DummyOperator(task_id='end_transfer', dag=dag)

# Dependencies - EXACTLY THE SAME
start_task >> test_connection_task >> download_task >> validate_task >> summary_task >> end_task
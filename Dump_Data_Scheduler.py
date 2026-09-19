from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.oracle.hooks.oracle import OracleHook
import socket
import os
import pandas as pd
import json

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'Dump_Data_Scheduler',
    default_args=default_args,
    description='SMB to Oracle and SFTP transfer',
    schedule_interval='0 8 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

CONFIG = {
    'csv_filename': f"RETAILER_GPS_{(datetime.now() - timedelta(days=1)).strftime('%Y%m%d')}.csv",
    'local_download_path': '/opt/airflow/data/dump_data/',
    'smb_connection_id': 'smb_connection',
    'sftp_connection_id': 'sftp_default',
    'oracle_connection_id': 'oracle_default',
    'sftp_remote_path': '/biometric_nfs_share/oracle/dump_biometric/',
}

def get_smb_credentials():
    conn = BaseHook.get_connection(CONFIG['smb_connection_id'])
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

def TestSmbConnection():
    smb_config = get_smb_credentials()
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    result = sock.connect_ex((smb_config['server'], 445))
    sock.close()
    
    if result != 0:
        raise Exception("SMB server not reachable")
    
    print("✓ SMB connection test passed")
    return True

def DownloadCsvFromSmb():
    smb_config = get_smb_credentials()
    os.makedirs(CONFIG['local_download_path'], exist_ok=True)
    
    local_file_path = os.path.join(CONFIG['local_download_path'], CONFIG['csv_filename'])
    remote_file_path = f"{smb_config['remote_path']}/{CONFIG['csv_filename']}"
    
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
        
        connected = conn.connect(smb_config['server'], 445)
        if not connected:
            raise Exception("Failed to connect to SMB server")
        
        with open(local_file_path, 'wb') as local_file:
            conn.retrieveFile(smb_config['share'], remote_file_path, local_file)
        
        file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
        print(f"✓ Downloaded: {file_size_mb:.2f} MB")
        conn.close()
        
        return local_file_path
        
    except Exception as e:
        raise Exception(f"SMB download failed: {str(e)}")

def ProcessCsvForOracle(file_path):
    df = pd.read_csv(file_path)
    
    required_columns = ['DISTRIBUTOR', 'RETAILER_CODE', 'RSO_CODE', 'LOC_LATITUDE', 'LOC_LONGITUDE']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise Exception(f"Missing required columns: {missing_columns}")
    
    df.fillna({
        'DISTRIBUTOR': '',
        'RETAILER_CODE': '',
        'RSO_CODE': '',
        'LOC_LATITUDE': 0.0,
        'LOC_LONGITUDE': 0.0
    }, inplace=True)
    
    df['DISTRIBUTOR'] = df['DISTRIBUTOR'].astype(str)
    df['RETAILER_CODE'] = df['RETAILER_CODE'].astype(str)
    df['RSO_CODE'] = df['RSO_CODE'].astype(str)
    df['LOC_LATITUDE'] = pd.to_numeric(df['LOC_LATITUDE'], errors='coerce').fillna(0.0)
    df['LOC_LONGITUDE'] = pd.to_numeric(df['LOC_LONGITUDE'], errors='coerce').fillna(0.0)
    
    print(f"✓ CSV processing completed: {len(df)} rows")
    return df

def TruncateRetailerTable():
    oracle_hook = OracleHook(oracle_conn_id=CONFIG['oracle_connection_id'])
    
    with oracle_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.callproc("TRUNCATE_RETAILER_LATLONG_BULK")
            conn.commit()
    
    print("✓ Table truncated successfully")
    return True

def BulkInsertToOracle(**context):
    ti = context['ti']
    local_file_path = ti.xcom_pull(task_ids='Download_CSV_From_SMB')
    
    if not local_file_path or not os.path.exists(local_file_path):
        raise Exception("CSV file not found")
    
    df = ProcessCsvForOracle(local_file_path)
    oracle_hook = OracleHook(oracle_conn_id=CONFIG['oracle_connection_id'])
    
    with oracle_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            insert_query = """
            INSERT INTO TBL_DMS_RETAILER_LATLONG 
            (DISTRIBUTOR, RETAILER_CODE, RSO_CODE, LOC_LATITUDE, LOC_LONGITUDE)
            VALUES (:1, :2, :3, :4, :5)
            """
            
            data_for_bulk = []
            for _, row in df.iterrows():
                data_for_bulk.append((
                    str(row['DISTRIBUTOR']),
                    str(row['RETAILER_CODE']),
                    str(row['RSO_CODE']),
                    float(row['LOC_LATITUDE']),
                    float(row['LOC_LONGITUDE'])
                ))
            
            cursor.executemany(insert_query, data_for_bulk)
            conn.commit()
    
    print(f"✓ Bulk insert completed: {len(df)} rows")
    return {'total_rows': len(df), 'file_path': local_file_path}

def TransferToSftp(**context):
    ti = context['ti']
    local_file_path = ti.xcom_pull(task_ids='Download_CSV_From_SMB')
    
    if not local_file_path or not os.path.exists(local_file_path):
        raise Exception("CSV file not found for SFTP transfer")
    
    hook = SFTPHook(ftp_conn_id=CONFIG['sftp_connection_id'])
    remote_path = f"{CONFIG['sftp_remote_path']}{CONFIG['csv_filename']}"
    
    try:
        hook.list_directory(CONFIG['sftp_remote_path'])
    except:
        hook.create_directory(CONFIG['sftp_remote_path'])
    
    hook.store_file(remote_path, local_file_path)
    
    file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
    print(f"✓ SFTP transfer completed: {remote_path} ({file_size_mb:.2f} MB)")
    
    return {'sftp_path': remote_path, 'file_size_mb': file_size_mb}

def MonitorProcesses(**context):
    ti = context['ti']
    oracle_result = ti.xcom_pull(task_ids='Bulk_Insert_To_Oracle')
    sftp_result = ti.xcom_pull(task_ids='Transfer_To_Sftp')
    
    print("=" * 40)
    print("EXECUTION SUMMARY")
    print("=" * 40)
    
    if oracle_result:
        print(f"ORACLE: {oracle_result.get('total_rows', 0):,} rows inserted")
    
    if sftp_result:
        print(f"SFTP: {sftp_result.get('sftp_path', 'N/A')}")
    
    print("=" * 40)
    return True

def CleanupFiles(**context):
    ti = context['ti']
    oracle_result = ti.xcom_pull(task_ids='Bulk_Insert_To_Oracle')
    
    if oracle_result and 'file_path' in oracle_result:
        local_file_path = oracle_result['file_path']
        if local_file_path and os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"✓ Cleaned up: {local_file_path}")
    
    print("✓ Cleanup completed")

# Tasks
Start_Task = DummyOperator(task_id='Start_Process', dag=dag)
Test_Connection_Task = PythonOperator(task_id='Test_Smb_Connection', python_callable=TestSmbConnection, dag=dag)
Download_Task = PythonOperator(task_id='Download_CSV_From_SMB', python_callable=DownloadCsvFromSmb, dag=dag)
Truncate_Table_Task = PythonOperator(task_id='Truncate_Retailer_Table', python_callable=TruncateRetailerTable, dag=dag)
Oracle_Insert_Task = PythonOperator(task_id='Bulk_Insert_To_Oracle', python_callable=BulkInsertToOracle, dag=dag)
Sftp_Transfer_Task = PythonOperator(task_id='Transfer_To_Sftp', python_callable=TransferToSftp, dag=dag)
Monitor_Task = PythonOperator(task_id='Monitor_Processes', python_callable=MonitorProcesses, dag=dag)
Cleanup_Task = PythonOperator(task_id='Cleanup_Files', python_callable=CleanupFiles, dag=dag)
End_Task = DummyOperator(task_id='End_Process', dag=dag)

# Workflow
Start_Task >> Test_Connection_Task >> Download_Task >> Truncate_Table_Task
Truncate_Table_Task >> [Oracle_Insert_Task, Sftp_Transfer_Task]
[Oracle_Insert_Task, Sftp_Transfer_Task] >> Monitor_Task >> Cleanup_Task >> End_Task
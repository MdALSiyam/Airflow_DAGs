from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    'GET_PUSH_DUMP_DATA',
    default_args=default_args,
    description='SMB to Oracle and SFTP transfer',
    schedule_interval='0 12 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    concurrency=1,
)

CONFIG = {
    'csv_filename': f"RETAILER_GPS_{(datetime.now() - timedelta(days=1)).strftime('%Y%m%d')}.csv",
    'local_download_path': '/opt/airflow/data/dump_data/',
    'smb_connection_id': 'smb_connection',
    'sftp_connection_id': 'sftp_default',
    'oracle_connection_id': 'oracle_default',
    'sftp_remote_path': '/biometric_nfs_share/oracle/dump_biometric/',
}

def start_dag_message():
    print("✓ DAG Started")

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

def check_smb_connectivity():
    smb_config = get_smb_credentials()
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    result = sock.connect_ex((smb_config['server'], 445))
    sock.close()
    
    if result != 0:
        raise Exception("SMB server not reachable")
    
    print("✓ SMB connection test passed")
    return True

def import_csv_from_smb():
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
        
        return {'file_path': local_file_path, 'file_size_mb': file_size_mb}
        
    except Exception as e:
        raise Exception(f"SMB download failed: {str(e)}")

def process_csv_data(file_path):
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
    
    print(f"✓ Processed: {len(df)} rows")
    return df

def truncate_target_table():
    oracle_hook = OracleHook(oracle_conn_id=CONFIG['oracle_connection_id'])
    
    with oracle_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.callproc("TRUNCATE_RETAILER_LATLONG_BULK")
            conn.commit()
    
    print("✓ Table truncated")
    return True

def upload_data_to_oracle(**context):
    ti = context['ti']
    import_result = ti.xcom_pull(task_ids='Import_File_From_SMB')
    local_file_path = import_result.get('file_path') if isinstance(import_result, dict) else import_result
    
    if not local_file_path or not os.path.exists(local_file_path):
        raise Exception("CSV file not found")
    
    df = process_csv_data(local_file_path)
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
    
    print(f"✓ Inserted: {len(df)} rows")
    return {'total_rows': len(df), 'file_path': local_file_path}

def export_file_to_sftp(**context):
    ti = context['ti']
    import_result = ti.xcom_pull(task_ids='Import_File_From_SMB')
    local_file_path = import_result.get('file_path') if isinstance(import_result, dict) else import_result
    
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
    print(f"✓ SFTP uploaded: {file_size_mb:.2f} MB")
    
    return {'sftp_path': remote_path, 'file_size_mb': file_size_mb}

def cleanup_temporary_files(**context):
    ti = context['ti']
    oracle_result = ti.xcom_pull(task_ids='Upload_File_In_Database')
    
    if oracle_result and 'file_path' in oracle_result:
        local_file_path = oracle_result['file_path']
        if local_file_path and os.path.exists(local_file_path):
            os.remove(local_file_path)
            print("✓ Cleaned temp files")
    
    return True

def generate_execution_summary(**context):
    ti = context['ti']
    
    import_result = ti.xcom_pull(task_ids='Import_File_From_SMB')
    oracle_result = ti.xcom_pull(task_ids='Upload_File_In_Database')
    sftp_result = ti.xcom_pull(task_ids='Export_File_By_SFTP')
    
    # Get file size
    file_size_mb = 0
    if import_result and isinstance(import_result, dict):
        file_size_mb = import_result.get('file_size_mb', 0)
    
    total_rows = oracle_result.get('total_rows', 0) if oracle_result else 0
    
    print("=" * 50)
    print("EXECUTION SUMMARY - DUMP DATA SCEDULER")
    print("=" * 50)
    print("✓ ORACLE DATABASE:")
    print(f"   → Rows Inserted: {total_rows:,}")
    print(f"   → File Processed: {CONFIG['csv_filename']}")
    print("✓ SFTP BACKUP:")
    
    if sftp_result:
        sftp_path = sftp_result.get('sftp_path', '')
        print(f"   → Location: {sftp_path}")
        print(f"   → File Size: {file_size_mb:.2f} MB")
    else:
        print("   → Location: N/A")
        print("   → File Size: N/A")
    
    print("=" * 50)
    
    # Check if all tasks were successful
    all_success = import_result and oracle_result and sftp_result
    if all_success:
        print("✓ ALL OPERATIONS COMPLETED SUCCESSFULLY!")
    else:
        print("⚠️  SOME OPERATIONS HAD ISSUES!")
    
    print("=" * 50)
    
    return True


def end_dag_message():
    print("✓ DAG Completed")

# Tasks
Starting_DAGs = PythonOperator(task_id='Starting_DAGs', python_callable=start_dag_message, dag=dag)
Connectivity_Test = PythonOperator(task_id='Connectivity_Test', python_callable=check_smb_connectivity, dag=dag)
Import_File_From_SMB = PythonOperator(task_id='Import_File_From_SMB', python_callable=import_csv_from_smb, dag=dag)
Truncate_Database_Table = PythonOperator(task_id='Truncate_Database_Table', python_callable=truncate_target_table, dag=dag)
Upload_File_In_Database = PythonOperator(task_id='Upload_File_In_Database', python_callable=upload_data_to_oracle, dag=dag)
Export_File_By_SFTP = PythonOperator(task_id='Export_File_By_SFTP', python_callable=export_file_to_sftp, dag=dag)
Cleanup_Task = PythonOperator(task_id='Cleanup_Files', python_callable=cleanup_temporary_files, dag=dag)
Summary_Task = PythonOperator(task_id='Summary_Task', python_callable=generate_execution_summary, dag=dag)
Successfull_Message = PythonOperator(task_id='Successfull_Message', python_callable=end_dag_message, dag=dag)

# Workflow
Starting_DAGs >> Connectivity_Test >> Import_File_From_SMB >> Truncate_Database_Table
Truncate_Database_Table >> [Upload_File_In_Database, Export_File_By_SFTP]
[Upload_File_In_Database, Export_File_By_SFTP] >> Cleanup_Task >> Summary_Task >> Successfull_Message


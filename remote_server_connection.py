from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import socket
from urllib.parse import quote_plus

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'remote_server_connection',
    default_args=default_args,
    description='DAG for testing remote server connectivity',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['remote', 'connectivity', 'test'],
)

def get_connection_params(conn_id='smb_connection'):
    """
    Retrieve connection parameters from Airflow Connection
    """
    try:
        conn = BaseHook.get_connection(conn_id)
        
        # Extract connection details
        host = conn.host
        login = conn.login
        password = conn.password
        port = conn.port or 445  # Default to SMB port if not specified
        extra = conn.extra_dejson
        
        # Get share path from extra field
        share_path = extra.get('share_path', 'SharedFolder')
        sub_path = extra.get('sub_path', '')
        
        return {
            'host': host,
            'port': port,
            'username': login,
            'password': password,
            'share_path': share_path,
            'sub_path': sub_path
        }
    except Exception as e:
        error_msg = f"Failed to retrieve connection '{conn_id}': {str(e)}"
        print(f"✗ {error_msg}")
        raise Exception(error_msg)

def test_server_reachability(**context):
    """
    Test if the SMB server is reachable
    """
    try:
        # Get connection parameters from previous task
        conn_params = context['task_instance'].xcom_pull(task_ids='get_connection_params')
        host = conn_params['host']
        port = conn_params['port']
        
        print(f"Testing connection to {host}:{port}...")
        
        # Create a socket and try to connect
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((host, port))
        
        if result == 0:
            print(f"✓ Server is reachable on port {port}")
            sock.close()
            return True
        else:
            error_msg = f"Server not reachable on port {port}. Error code: {result}"
            print(f"✗ {error_msg}")
            sock.close()
            raise Exception(error_msg)
            
    except Exception as e:
        error_msg = f"Connection test failed: {str(e)}"
        print(f"✗ {error_msg}")
        raise Exception(error_msg)

def test_alternative_ports(**context):
    """
    Test common SMB ports to see which ones are open
    """
    # Get connection parameters from previous task
    conn_params = context['task_instance'].xcom_pull(task_ids='get_connection_params')
    host = conn_params['host']
    
    ports_to_test = [445, 139, 135]  # Common SMB ports
    
    print(f"Testing common SMB ports on {host}...")
    
    for port in ports_to_test:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((host, port))
            
            if result == 0:
                print(f"✓ Port {port} is open and accessible")
            else:
                print(f"✗ Port {port} is closed or filtered")
                
            sock.close()
            
        except Exception as e:
            print(f"Error testing port {port}: {str(e)}")
    
    print("Port testing completed")

def generate_connection_string(**context):
    """
    Generate and display the connection information
    """
    # Get connection parameters from previous task
    conn_params = context['task_instance'].xcom_pull(task_ids='get_connection_params')
    
    host = conn_params['host']
    username = conn_params['username']
    password = conn_params['password']
    share_path = conn_params['share_path']
    sub_path = conn_params['sub_path']
    
    full_path = f"\\\\{host}\\{share_path}"
    if sub_path:
        full_path += f"\\{sub_path}"
    
    print("=== SMB Connection Information ===")
    print(f"Server: {host}")
    print(f"Username: {username}")
    print(f"Password: {'*' * len(password)}")  # Mask password for security
    print(f"Path: {full_path}")
    print("")
    print("=== URL Encoded Connection String ===")
    
    # Create a connection string (for informational purposes)
    connection_string = f"smb://{username}:{quote_plus(password)}@{host}/{share_path}"
    if sub_path:
        connection_string += f"/{sub_path}"
    
    print(f"Connection URL: {connection_string}")
    
    return connection_string

def validate_credentials_format(**context):
    """
    Validate that credentials are in expected format
    """
    # Get connection parameters from previous task
    conn_params = context['task_instance'].xcom_pull(task_ids='get_connection_params')
    
    username = conn_params['username']
    password = conn_params['password']
    
    print("Validating credential format...")
    
    # Check if username is not empty
    if not username or not username.strip():
        raise Exception("Username is empty")
    
    # Check if password is not empty and meets basic criteria
    if not password or not password.strip():
        raise Exception("Password is empty")
    
    if len(password) < 8:
        print("⚠ Warning: Password is shorter than 8 characters")
    
    # Check for special characters (common in SMB passwords)
    special_chars = set('!@#$%^&*()_-+={}[]|\\:;"\'<>,.?/')
    if not any(char in special_chars for char in password):
        print("⚠ Warning: Password may not contain special characters")
    
    print("✓ Credentials format validation passed")
    return True

def create_connection_test_summary(**context):
    """
    Create a comprehensive test summary
    """
    # Get connection parameters from previous task
    conn_params = context['task_instance'].xcom_pull(task_ids='get_connection_params')
    
    host = conn_params['host']
    username = conn_params['username']
    share_path = conn_params['share_path']
    sub_path = conn_params['sub_path']
    
    full_path = f"\\\\{host}\\{share_path}"
    if sub_path:
        full_path += f"\\{sub_path}"
    
    print("=" * 50)
    print("REMOTE SERVER CONNECTION TEST SUMMARY")
    print("=" * 50)
    print(f"Test Time: {datetime.now()}")
    print(f"Target Server: {host}")
    print(f"Service: SMB (Windows File Sharing)")
    print("")
    
    # Test basic connectivity
    try:
        test_server_reachability(**context)
    except Exception as e:
        print(f"Connectivity Test: FAILED - {e}")
        raise e
    
    # Test alternative ports
    test_alternative_ports(**context)
    
    # Validate credentials
    try:
        validate_credentials_format(**context)
        print("Credentials Test: PASSED")
    except Exception as e:
        print(f"Credentials Test: FAILED - {e}")
        raise e
    
    # Generate connection info
    # generate_connection_string(**context)
    
    # print("")
    # print("=" * 50)
    # print("NEXT STEPS:")
    # print("1. Install smbclient: apt-get install smbclient")
    # print("2. Or install python package: pip install smbprotocol")
    # print(f"3. Test manually: smbclient //{host}/{share_path} -U {username}%<password>")
    # print("=" * 50)
    
    # return "Connection tests completed successfully"

# Tasks
get_connection_task = PythonOperator(
    task_id='get_connection_params',
    python_callable=get_connection_params,
    dag=dag,
)

test_connectivity_task = PythonOperator(
    task_id='test_server_reachability',
    python_callable=test_server_reachability,
    provide_context=True,
    dag=dag,
)

test_ports_task = PythonOperator(
    task_id='test_alternative_ports',
    python_callable=test_alternative_ports,
    provide_context=True,
    dag=dag,
)

validate_creds_task = PythonOperator(
    task_id='validate_credentials_format',
    python_callable=validate_credentials_format,
    provide_context=True,
    dag=dag,
)

generate_info_task = PythonOperator(
    task_id='generate_connection_info',
    python_callable=generate_connection_string,
    provide_context=True,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='create_test_summary',
    python_callable=create_connection_test_summary,
    provide_context=True,
    dag=dag,
)

# Task dependencies
get_connection_task >> test_connectivity_task
test_connectivity_task >> test_ports_task
test_connectivity_task >> validate_creds_task
[test_ports_task, validate_creds_task] >> generate_info_task >> summary_task


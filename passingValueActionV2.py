from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

dag = DAG(
    'passing_value_action_v2',
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


# Set the dependencies between the tasks
starting_dags_task
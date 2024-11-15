# Filename: helloworld.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    return 'Hello'

def print_world():
    return 'World'

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    dag_id='helloworld',
    default_args=default_args,
    description='A simple Helloworld DAG',
    schedule_interval='@daily',
    catchup=False,
    tags=['example']
)

# Define the tasks
task1 = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

task2 = PythonOperator(
    task_id='print_world',
    python_callable=print_world,
    dag=dag,
)

# Set the task dependencies
task1 >> task2

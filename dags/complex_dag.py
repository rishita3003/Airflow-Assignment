from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_hello():
    print("Hello from Airflow!")

def count_characters(string):
    return len(string)

def multiply_numbers(a, b):
    return a * b

def python_task():
    print("This is a Python task")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='complex_workflow',
    default_args=default_args,
    description='A complex DAG with mixed tasks',
    schedule_interval='*/30 * * * *',
    catchup=False
)

# Define the tasks using a mix of Python and Bash operators
t1 = BashOperator(
    task_id='t1',
    bash_command='echo "This is a Bash task 18"',
    dag=dag
)

t2 = BashOperator(
    task_id='t2',
    bash_command='echo "Executing task t2"',
    dag=dag,
)

t3 = BashOperator(
    task_id='t3',
    bash_command='echo "This is a Bash task 3"',
    dag=dag
)

t4 = PythonOperator(
    task_id='t4',
    python_callable=python_task,
    dag=dag
)

t5 = BashOperator(
     task_id='t5',
    bash_command='date',
    dag=dag,
)

t6 = BashOperator(
    task_id='t6',
    bash_command='date',
    dag=dag,
)

t7 = PythonOperator(
    task_id='t7',
    python_callable=lambda: print("Task 7 execution"),
    dag=dag,
)

t8 = PythonOperator(
    task_id='t8',
    python_callable=lambda: print("Task 9 execution"),
    dag=dag,
    
)

# Define more tasks up to t19 as needed, here are a few more examples
t9 = BashOperator(
    task_id='t9',
    bash_command='cat /proc/cpuinfo',
    dag=dag,
)

t10 = BashOperator(
    task_id='t10',
    bash_command='uptime',
    dag=dag,
)

t11 = PythonOperator(
    task_id='t11',
    python_callable=lambda: print("Task 9 execution"),
    dag=dag,

)

t12 = PythonOperator(
task_id='t12',
    python_callable=python_task,
    dag=dag
)

t13 = BashOperator(
    task_id='t13',
    bash_command='python3 /home/rishita/airflow/dags/helloworld.py',
    dag=dag
    
    )

t14 = BashOperator(
    task_id='t14',
    bash_command='echo "This is a Bash task 14"',
    dag=dag
        
    )

t15 = PythonOperator(
    task_id='t15',
    python_callable=python_task,
    dag=dag
        
    )

t16 = PythonOperator(
    task_id='t16',
    python_callable=python_task,
    dag=dag  
)

t17 = BashOperator(
    task_id='t17',
    bash_command='echo "This is a Bash task 17"',
    dag=dag           
)

t18 = BashOperator(
    task_id='t18',
    bash_command='echo "This is a Bash task 18"',
    dag=dag
)
t19 = PythonOperator(
    task_id='t19',
    python_callable=python_task,
    dag=dag
)


# Add all tasks and define their dependencies as per the DAG diagram
t1 >> [t5, t2, t3, t4]
t5 >> [t8, t9]
t2 >> t6
t3 >> [t7,t12]
t7 >> [t13, t14]
t13 >> t18
t8 >> [t10,t15]
t9 >> [t11,t12]
t10 >> t14
t11 >> t14
t12 >> t14
t14 >> [t16, t17]
t15 >> t18
t16 >> t19
t17 >> t18
t18 >> t19



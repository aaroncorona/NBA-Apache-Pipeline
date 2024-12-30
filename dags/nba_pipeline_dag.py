from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# This can be run locally via http://localhost:8080/dags/nba_pipeline_dag
with DAG(
    dag_id='nba_pipeline_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    task_1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    task_2 = BashOperator(
        task_id='say_hello',
        bash_command='echo "Hello, Airflow!"',
    )

    task_1 >> task_2

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    'nba_spark_pipeline',
    description='A simple Spark job for NBA processing',
    schedule_interval='@daily',  # Set the frequency for running the DAG
    start_date=datetime(2024, 12, 30),
    catchup=False,
)

# Define the Spark submit command
jar_path = '/Users/aaroncorona/airflow/pipelines/project/src/main/scala/target/scala-2.12/nba-spark-pipeline_2.12-0.1.jar'
spark_submit_command = f"""
spark-submit --class Main --master local[*] {jar_path}
"""

# Create a BashOperator task to execute the spark-submit command
run_spark_job = BashOperator(
    task_id='run_spark_job',
    bash_command=spark_submit_command,
    dag=dag,
)

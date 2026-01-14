import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# --------------------------
# 1. Add ETL folder to sys.path
# --------------------------
# This allows importing the 'run_pipeline' function from main.py
sys.path.insert(0, "/opt/airflow/etl")
from main import run_pipeline

# --------------------------
# 2. Define DAG
# --------------------------
with DAG(
    dag_id='olist_aws_s3_pipeline',
    start_date=datetime(2025, 12, 1),
    schedule_interval=None,  # Run manually; can be changed to cron or timedelta
    catchup=False,
    tags=['ETL', 'S3', 'RDS', 'DataMart']
) as dag:

    # --------------------------
    # 3. Define ETL Task
    # --------------------------
    run_etl_task = PythonOperator(
        task_id='run_etl_main_process',
        python_callable=run_pipeline,
        provide_context=False
    )

    # Single task DAG
    run_etl_task

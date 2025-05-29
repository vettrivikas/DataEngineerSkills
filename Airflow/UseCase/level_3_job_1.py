from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datasets import level_1_job_1_dataset
from datetime import datetime

with DAG(
    "Level_3_job_1",
    start_date=datetime(2023, 1, 1),
    schedule=[level_1_job_1_dataset],
    catchup=False,
) as dag:
    run_job = EmptyOperator(task_id="run_level_3_job_1")

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from datasets import level_1_job_4_dataset, level_3_jobs_2_dataset

with DAG(
    "Level_3_jobs_4",
    start_date=datetime(2023, 1, 1),
    schedule=[level_1_job_4_dataset, level_3_jobs_2_dataset],
    catchup=False,
) as dag:
    task = EmptyOperator(task_id="run_job")

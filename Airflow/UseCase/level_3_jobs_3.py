from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from datasets import level_1_job_2_dataset, level_1_job_3_dataset, level_1_job_4_dataset

with DAG(
    "Level_3_jobs_3",
    start_date=datetime(2023, 1, 1),
    schedule=[level_1_job_2_dataset, level_1_job_3_dataset, level_1_job_4_dataset],
    catchup=False,
) as dag:
    task = EmptyOperator(task_id="run_job")

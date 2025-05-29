from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from datasets import level_1_job_4_dataset

with DAG(
    "Level_1_job_4", start_date=datetime(2023, 1, 1), schedule="@daily", catchup=False
) as dag:
    t1 = EmptyOperator(task_id="complete_job", outlets=[level_1_job_4_dataset])

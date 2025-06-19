from airflow.datasets import Dataset
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

level_1_job_1_dataset = Dataset("ds://level_1_job_1")
level_1_job_2_dataset = Dataset("ds://level_1_job_2")
level_1_job_3_dataset = Dataset("ds://level_1_job_3")
level_1_job_4_dataset = Dataset("ds://level_1_job_4")

level_3_jobs_2_dataset = Dataset("ds://level_3_jobs_2")


with DAG(
    "Level_1_job_1", start_date=datetime(2023, 1, 1), schedule="@daily", catchup=False
) as dag:
    t1 = EmptyOperator(task_id="complete_job", outlets=[level_1_job_1_dataset])

with DAG(
    "Level_1_job_2", start_date=datetime(2023, 1, 1), schedule="@daily", catchup=False
) as dag1:
    t2 = EmptyOperator(task_id="complete_job", outlets=[level_1_job_2_dataset])

with DAG(
        "Level_1_job_3", start_date=datetime(2023, 1, 1), schedule="@daily", catchup=False
) as dag3:
    t3 = EmptyOperator(task_id="complete_job", outlets=[level_1_job_3_dataset])

with DAG(
    "Level_1_job_4", start_date=datetime(2023, 1, 1), schedule="@daily", catchup=False
) as dag4:
    t4 = EmptyOperator(task_id="complete_job", outlets=[level_1_job_4_dataset])


with DAG(
    "Level_3_job_1",
    start_date=datetime(2023, 1, 1),
    schedule=[level_1_job_1_dataset],
    catchup=False,
) as dag5:
    run_job = EmptyOperator(task_id="run_level_3_job_1")

with DAG(
    "Level_3_jobs_2",
    start_date=datetime(2023, 1, 1),
    schedule=[level_1_job_1_dataset, level_1_job_2_dataset],
    catchup=False,
) as dag6:
    task = EmptyOperator(task_id="run_job", outlets=[level_3_jobs_2_dataset])




with DAG(
    "Level_3_jobs_3",
    start_date=datetime(2023, 1, 1),
    schedule=[level_1_job_2_dataset, level_1_job_3_dataset, level_1_job_4_dataset],
    catchup=False,
) as dag7:
    task7 = EmptyOperator(task_id="run_job")



with DAG(
    "Level_3_jobs_4",
    start_date=datetime(2023, 1, 1),
    schedule=[level_1_job_4_dataset, level_3_jobs_2_dataset],
    catchup=False,
) as dag8:
    task8 = EmptyOperator(task_id="run_job")

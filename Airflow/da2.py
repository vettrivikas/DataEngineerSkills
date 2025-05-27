from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, time
from shared_dataset import dag1_completion_dataset

from airflow.datasets import Dataset

dag1_completion_dataset = Dataset("gs://my-airflow-dag-bucket/dag1/complete.flag")

def check_sla_time():
    now = datetime.now().time()
    # SLA window: 08:00 - 09:00 AM
    if not time(7, 0) <= now <= time(12, 0):
        raise ValueError("SLA not met: Current time is outside the allowed window (08:00-09:00 AM)")

with DAG(
    dag_id='dag2',
    start_date=datetime(2024, 1, 1),
    schedule=[dag1_completion_dataset],  # Triggered by DAG 1 dataset
    catchup=False,
    description='DAG 2 waits for DAG 1 dataset and validates SLA',
) as dag:

    sla_check = PythonOperator(
        task_id='check_sla',
        python_callable=check_sla_time,
    )

    proceed = EmptyOperator(task_id='proceed_with_processing')

    sla_check >> proceed


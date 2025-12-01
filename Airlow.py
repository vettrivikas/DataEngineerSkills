from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False
}

# --- DAG 1 ---
with DAG(
    dag_id='glue_job1_dag',
    default_args=default_args,
    schedule_interval=None,
) as dag1:
    glue_job1 = AwsGlueJobOperator(
        task_id='run_glue_job1',
        job_name='your_glue_job1_name',  # replace with actual Glue job name
        region_name='ap-south-1',        # your AWS region
    )

# --- DAG 2 ---
with DAG(
    dag_id='glue_job2_dag',
    default_args=default_args,
    schedule_interval=None,
) as dag2:
    glue_job2 = AwsGlueJobOperator(
        task_id='run_glue_job2',
        job_name='your_glue_job2_name',  # replace with actual Glue job name
        region_name='ap-south-1',
    )


import time
import secrets
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def unique_sleep_ms():
    delay_ms = secrets.randbelow(30000) + 1000   # 1000–30000 ms (1–30s)
    logging.info(f"Random sleep time selected: {delay_ms} ms")
    time.sleep(delay_ms / 1000)
    logging.info(f"Sleep completed after {delay_ms} ms")
    return delay_ms

slept = unique_sleep_ms()
print(f"Slept for {slept} ms")


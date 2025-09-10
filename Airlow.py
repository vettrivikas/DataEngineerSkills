from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json

def print_conf_params(**context):
    dag_run_conf = context.get("dag_run").conf or {}
    print("Received Config Params:")
    print(json.dumps(dag_run_conf, indent=2))

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 0
}

with DAG(
    dag_id="s3_lambda_triggered_dag",
    default_args=default_args,
    schedule_interval=None,  # Triggered manually or via API/Lambda
    catchup=False,
    tags=["aws", "lambda", "s3"],
) as dag:

    print_conf = PythonOperator(
        task_id="print_conf",
        python_callable=print_conf_params,
        provide_context=True,
    )

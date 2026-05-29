from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="runtime_dynamic_tasks",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def glue_job(table_name):
        print(f"Running glue for {table_name}")

    @task
    def get_config(table_name):
        # Read S3 config here

        if table_name == "customer":
            return [
                {"job_name": "job1"},
                {"job_name": "job2"},
                {"job_name": "job3"},
            ]

        elif table_name == "orders":
            return [
                {"job_name": "job1"},
            ]

        return [
            {"job_name": "job1"},
            {"job_name": "job2"},
        ]

    @task(map_index_template="{{ job_name }}")
    def process_item(item):
        print(f"Processing {item}")

    glue = glue_job("{{ params.table_name }}")

    items = get_config("{{ params.table_name }}")

    mapped_tasks = process_item.expand(item=items)

    glue >> items >> mapped_tasks

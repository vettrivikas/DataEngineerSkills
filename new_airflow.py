from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator


CONFIG = {
    "cde": [
        {"file_name": "file1"},
        {"file_name": "file2"}
    ],
    "pw": [
        {"file_name": "file1"},
        {"file_name": "file2"},
        {"file_name": "file3"}
    ],
    "iw": [
        {"file_name": "file1"}
    ]
}


with DAG(
    dag_id="dynamic_mapping_test",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    params={
        "table_name": Param(
            default="cde",
            type="string"
        )
    }
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    end = EmptyOperator(
        task_id="end"
    )

    @task
    def insert_audit():
        print("Audit Inserted")

    @task
    def standalone_glue(table_name):
        print(
            f"Standalone Glue Running for {table_name}"
        )
        return table_name

    @task
    def get_files(table_name):

        print(f"Selected Table = {table_name}")

        if table_name not in CONFIG:
            raise Exception(
                f"{table_name} not found in config"
            )

        return CONFIG[table_name]

    @task_group
    def process_file(file_cfg):

        @task
        def dq(file_cfg):

            print(
                f"DQ Started : "
                f"{file_cfg['file_name']}"
            )

        @task
        def encryption(file_cfg):

            print(
                f"Encryption Started : "
                f"{file_cfg['file_name']}"
            )

        dq_task = dq(file_cfg)

        enc_task = encryption(file_cfg)

        dq_task >> enc_task

    audit = insert_audit()

    table_name = standalone_glue(
        "{{ params.table_name }}"
    )

    files = get_files(table_name)

    mapped_tasks = process_file.expand(
        file_cfg=files
    )

    start >> audit >> table_name >> files >> mapped_tasks >> end

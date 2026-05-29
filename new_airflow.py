from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.utils.task_group import task_group
from airflow.operators.empty import EmptyOperator


CONFIG = {
    "cde": [
        {
            "flagName": "file1",
            "fileName": "cde_file1.csv"
        },
        {
            "flagName": "file2",
            "fileName": "cde_file2.csv"
        }
    ],
    "pw": [
        {
            "flagName": "file1",
            "fileName": "pw_file1.csv"
        },
        {
            "flagName": "file2",
            "fileName": "pw_file2.csv"
        },
        {
            "flagName": "file3",
            "fileName": "pw_file3.csv"
        }
    ],
    "iw": [
        {
            "flagName": "file1",
            "fileName": "iw_file1.csv"
        }
    ]
}


with DAG(
    dag_id="dynamic_dq_encryption",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    params={
        "table_name": Param(
            "",
            type="string",
            description="Table Name"
        )
    }
) as dag:

    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    @task
    def insert_audit():
        print("Audit inserted")

    @task
    def standalone_glue(table_name):
        print(
            f"Running standalone glue job for {table_name}"
        )
        return table_name

    @task
    def get_outputs(table_name):

        if table_name not in CONFIG:
            raise Exception(
                f"{table_name} not found"
            )

        return CONFIG[table_name]

    @task_group
    def process_file(file_cfg):

        @task
        def dq(file_cfg):

            print(
                f"DQ started : "
                f"{file_cfg['fileName']}"
            )

        @task
        def encryption(file_cfg):

            print(
                f"Encryption started : "
                f"{file_cfg['fileName']}"
            )

        dq_task = dq(file_cfg)

        enc_task = encryption(file_cfg)

        dq_task >> enc_task

    audit_task = insert_audit()

    table_name = standalone_glue(
        "{{ params.table_name }}"
    )

    file_configs = get_outputs(table_name)

    process_file.expand(
        file_cfg=file_configs
    )

    start >> audit_task >> table_name >> file_configs

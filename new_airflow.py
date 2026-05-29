from datetime import datetime

from airflow import DAG
from airflow.decorators import task
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
    dag_id="dynamic_encryption_dq_test",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    params={
        "table_name": Param("cde", type="string")
    },
    tags=["test"],
) as dag:

    @task
    def get_file_configs(**context):

        table_name = context["params"]["table_name"]

        if table_name not in CONFIG:
            raise Exception(
                f"Table '{table_name}' not found in config"
            )

        result = []

        for file_cfg in CONFIG[table_name]:
            result.append(
                {
                    "table_name": table_name,
                    "file_name": file_cfg["file_name"]
                }
            )

        print(f"Selected table: {table_name}")
        print(f"Files: {result}")

        return result

    # Audit Task
    insert_audit = EmptyOperator(
        task_id="insert_audit"
    )

    file_configs = get_file_configs()

    # Dynamic Encryption Tasks
    encrypt_files = EmptyOperator.partial(
        task_id="encrypt_file"
    ).expand(
        op_args=file_configs
    )

    # Dynamic DQ Tasks
    dq_files = EmptyOperator.partial(
        task_id="dq_file"
    ).expand(
        op_args=file_configs
    )

    insert_audit >> file_configs >> encrypt_files >> dq_files

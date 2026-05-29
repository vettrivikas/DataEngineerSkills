from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="dynamic_dependency_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "table_name": Param(
            default="",
            type="string",
            title="Table Name"
        )
    },
) as dag:

    @task
    def audit_task():
        print("Audit started")

    @task
    def glue_job(table_name):
        print(f"Running glue job for table: {table_name}")

    @task
    def get_dependency_list(table_name):
        """
        Call your function here.
        Function should return list of dicts.
        """

        return [
            {
                "table_name": "customer",
                "job_name": "customer_job"
            },
            {
                "table_name": "orders",
                "job_name": "orders_job"
            },
            {
                "table_name": "products",
                "job_name": "products_job"
            }
        ]

    @task_group(group_id="process_item")
    def process_item(item):

        dummy_task = EmptyOperator(
            task_id="dummy_task"
        )

        dummy_task_1 = EmptyOperator(
            task_id="dummy_task_1"
        )

        dummy_task >> dummy_task_1

    audit = audit_task()

    glue = glue_job(
        "{{ params.table_name }}"
    )

    dependency_list = get_dependency_list(
        "{{ params.table_name }}"
    )

    dynamic_group = process_item.expand(
        item=dependency_list
    )

    audit >> glue >> dependency_list >> dynamic_group

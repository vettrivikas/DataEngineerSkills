from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from datetime import datetime

with DAG(
    dag_id="dynamic_glue_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "table_name": Param("", type="string")
    },
) as dag:

    @task
    def audit_task():
        print("Audit started")

    @task
    def glue_job(table_name):
        print(f"Running glue job for {table_name}")

    @task
    def get_dependency_list(table_name):
        # call your function here
        return [
            {"job_name": "job1", "schema": "s1"},
            {"job_name": "job2", "schema": "s2"},
            {"job_name": "job3", "schema": "s3"},
        ]

    @task
    def run_job(job):
        print(job)

    audit = audit_task()

    glue = glue_job("{{ params.table_name }}")

    dependency_list = get_dependency_list("{{ params.table_name }}")

    dynamic_jobs = run_job.expand(job=dependency_list)

    audit >> glue >> dependency_list >> dynamic_jobs

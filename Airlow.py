from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

with DAG(
    dag_id="recursion_suite_glue_trigger",
    schedule_interval=None,  # Trigger manually or set cron expression
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["glue", "recursion_suite"],
) as dag:

    first_glue_dev_job = GlueJobOperator(
        task_id="first_glue_dev_job",
        job_name="first_glue_dev_job",
        region_name="ap-south-1",  # Change if needed
        script_args={},  # Optional: {"--ENV": "dev"}
        wait_for_completion=True,
    )

    pre_glue_job = GlueJobOperator(
        task_id="pre_glue_job",
        job_name="pre_glue_job",
        region_name="ap-south-1",
        script_args={},
        wait_for_completion=True,
    )

    post_glue_job = GlueJobOperator(
        task_id="post_glue_job",
        job_name="post_glue_job",
        region_name="ap-south-1",
        script_args={},
        wait_for_completion=True,
    )

    # Execution order
    first_glue_dev_job >> pre_glue_job >> post_glue_job

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime
import boto3

GLUE_WORKFLOW_NAME = "my_glue_workflow"
AWS_REGION = "ap-south-1"

# ---------------------------
# Trigger Glue Workflow
# ---------------------------
def trigger_glue_workflow(**context):
    glue = boto3.client("glue", region_name=AWS_REGION)
    response = glue.start_workflow_run(
        Name=GLUE_WORKFLOW_NAME
    )
    run_id = response["RunId"]
    context["ti"].xcom_push(key="run_id", value=run_id)
    print("Triggered workflow RunId:", run_id)

# ---------------------------
# Monitor Glue Workflow
# ---------------------------
def monitor_glue_workflow(**context):
    run_id = context["ti"].xcom_pull(
        task_ids="trigger_glue_workflow",
        key="run_id"
    )

    glue = boto3.client("glue", region_name=AWS_REGION)
    response = glue.get_workflow_run(
        Name=GLUE_WORKFLOW_NAME,
        RunId=run_id,
        IncludeGraph=False
    )

    status = response["Run"]["Status"]
    print("Workflow status:", status)

    if status == "COMPLETED":
        return True
    if status in ["FAILED", "STOPPED", "ERROR"]:
        raise Exception(f"Glue workflow failed: {status}")

    return False  # still running

# ---------------------------
# DAG Definition
# ---------------------------
with DAG(
    dag_id="trigger_and_monitor_glue_workflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["aws", "glue", "workflow"],
) as dag:

    trigger_workflow = PythonOperator(
        task_id="trigger_glue_workflow",
        python_callable=trigger_glue_workflow,
        provide_context=True
    )

    monitor_workflow = PythonSensor(
        task_id="monitor_glue_workflow",
        python_callable=monitor_glue_workflow,
        poke_interval=60,           # every 60 seconds
        timeout=60 * 60 * 3,        # 3 hours
        mode="poke"
    )

    trigger_workflow >> monitor_workflow

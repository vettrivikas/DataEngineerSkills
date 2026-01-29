from airflow.sensors.python import PythonSensor
import boto3

def monitor_workflow(**context):
    run_id = context["ti"].xcom_pull(
        task_ids="trigger_glue_workflow",
        key="run_id"
    )

    glue = boto3.client("glue", region_name="ap-south-1")
    response = glue.get_workflow_run(
        Name="my_glue_workflow",
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

monitor_workflow = PythonSensor(
    task_id="monitor_glue_workflow",
    python_callable=monitor_workflow,
    poke_interval=60,   # check every 60 sec
    timeout=60 * 60 * 3,  # 3 hours
    mode="poke"
)

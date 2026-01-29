from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.glue import AwsGlueHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import time
import logging

# Define workflow name and AWS region
GLUE_WORKFLOW_NAME = "Your_Glue_Workflow_Name"
AWS_REGION = "us-east-1" # Replace with your region

def trigger_glue_workflow(**kwargs):
    """
    Triggers an AWS Glue Workflow and waits for its completion.
    """
    # Initialize the hook
    glue_hook = AwsGlueHook(aws_conn_id='aws_default', region_name=AWS_REGION)

    logging.info(f"Triggering Glue Workflow: {GLUE_WORKFLOW_NAME}")
    # Start the workflow run
    run_id = glue_hook.get_client().start_workflow_run(Name=GLUE_WORKFLOW_NAME)['RunId']
    logging.info(f"Workflow run ID: {run_id}")
    
    # Push the run_id to XCom for potential use by other tasks
    kwargs['ti'].xcom_push(key='workflow_run_id', value=run_id)

    # Poll for the workflow status (implement or use a sensor for production)
    # This is a basic polling example; a dedicated sensor is better for long-running processes
    while True:
        status = glue_hook.get_client().get_workflow_run(Name=GLUE_WORKFLOW_NAME, RunId=run_id)
        run_state = status['Run']['WorkflowRunStatus']
        logging.info(f"Current workflow state: {run_state}")

        if run_state in ['COMPLETED', 'FAILED', 'STOPPED']:
            if run_state == 'COMPLETED':
                logging.info("Workflow completed successfully.")
                return run_id
            else:
                logging.error(f"Workflow failed or stopped with state: {run_state}")
                raise Exception(f"Glue Workflow run {run_id} {run_state}")
        
        time.sleep(30) # Wait for 30 seconds before polling again

with DAG(
    dag_id='trigger_aws_glue_workflow_dag',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=120),
    max_active_runs=1,
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0 # Set retries to 0 to prevent infinite loops on failure
    }
) as dag:

    trigger_and_monitor_workflow = PythonOperator(
        task_id='trigger_and_monitor_glue_workflow',
        python_callable=trigger_glue_workflow,
        provide_context=True # Allows access to task instance (ti) and XComs
    )

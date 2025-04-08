from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import boto3

def copy_s3_file(**kwargs):
    # Get parameters from dagrun.conf
    source_key = kwargs['dag_run'].conf.get('source_key')
    dest_key = kwargs['dag_run'].conf.get('dest_key')
    bucket_name = kwargs['dag_run'].conf.get('bucket_name')

    s3 = boto3.client('s3')

    # Copy the object
    copy_source = {
        'Bucket': bucket_name,
        'Key': source_key
    }

    s3.copy_object(
        Bucket=bucket_name,
        CopySource=copy_source,
        Key=dest_key
    )
    print(f"Copied {source_key} to {dest_key} in bucket {bucket_name}")

# Define DAG
with DAG(
    dag_id='copy_s3_file_dag',
    start_date=days_ago(1),
    schedule_interval=None,  # manual trigger
    catchup=False
) as dag:

    copy_task = PythonOperator(
        task_id='copy_file_task',
        python_callable=copy_s3_file,
        provide_context=True
    )

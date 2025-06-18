from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import boto3
##pip download SQLAlchemy==1.4.49 --only-binary=:all: --platform manylinux2014_x86_64 --python-version 3.7 --implementation cp --abi cp37 --dest .

def copy_s3_folder(**kwargs):
    source_prefix = kwargs['dag_run'].conf.get('source_key')  # e.g., "fb/jn/"
    dest_prefix = kwargs['dag_run'].conf.get('dest_key')      # e.g., "db/jb/"
    bucket_name = kwargs['dag_run'].conf.get('bucket_name')   # e.g., "my-bucket"

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket_name, Prefix=source_prefix):
        for obj in page.get('Contents', []):
            source_key = obj['Key']
            dest_key = source_key.replace(source_prefix, dest_prefix, 1)

            # Skip "folders" (keys ending with /)
            if source_key.endswith('/'):
                continue

            s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': source_key},
                Key=dest_key
            )
            print(f"Copied {source_key} → {dest_key}")

# Define DAG
with DAG(
    dag_id='copy_s3_folder_dag',
    start_date=days_ago(1),
    schedule_interval=None,  # Manual trigger only
    catchup=False
) as dag:

    copy_task = PythonOperator(
        task_id='copy_all_files_in_folder',
        python_callable=copy_s3_folder,
        provide_context=True
    )

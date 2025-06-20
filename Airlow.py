from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import boto3

def generic_s3_copy(**kwargs):
    s3 = boto3.client('s3')

    bucket = kwargs['dag_run'].conf.get('bucket_name')
    src_raw = kwargs['dag_run'].conf.get('source_keys')
    dst_raw = kwargs['dag_run'].conf.get('dest_keys')

    # Normalize to lists
    source_keys = [src_raw] if isinstance(src_raw, str) else src_raw
    dest_keys = [dst_raw] if isinstance(dst_raw, str) else dst_raw

    if not (source_keys and dest_keys) or len(source_keys) != len(dest_keys):
        raise ValueError("source_keys and dest_keys must be non-empty and of equal length")

    for src, dst in zip(source_keys, dest_keys):
        if not src.endswith('/'):
            # Single file
            file_name = src.split('/')[-1]
            dest_key = f"{dst.rstrip('/')}/{file_name}"
            s3.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': src},
                Key=dest_key
            )
            print(f"[file] Copied {src} → {dest_key}")
        else:
            # Folder copy
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=src):
                for obj in page.get('Contents', []):
                    skey = obj['Key']
                    if skey.endswith('/'):
                        continue
                    dkey = skey.replace(src, dst, 1)
                    s3.copy_object(
                        Bucket=bucket,
                        CopySource={'Bucket': bucket, 'Key': skey},
                        Key=dkey
                    )
                    print(f"[folder] Copied {skey} → {dkey}")

# Define DAG
with DAG(
    dag_id='s3_generic_copy_dag',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['s3', 'copy']
) as dag:

    copy_task = PythonOperator(
        task_id='copy_files_or_folders',
        python_callable=generic_s3_copy,
        provide_context=True
    )

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def generic_s3_crossbucket_copy(**kwargs):
    s3 = boto3.client('s3')
    lambda_client = boto3.client('lambda')

    # Read inputs
    source_bucket = kwargs['dag_run'].conf.get('source_bucket')
    target_bucket = kwargs['dag_run'].conf.get('target_bucket')
    src_raw = kwargs['dag_run'].conf.get('source_keys')
    dst_raw = kwargs['dag_run'].conf.get('dest_keys')
    lambda_function_name = kwargs['dag_run'].conf.get('lambda_function_name')

    # Normalize to lists
    source_keys = [src_raw] if isinstance(src_raw, str) else src_raw
    dest_keys = [dst_raw] if isinstance(dst_raw, str) else dst_raw

    if not (source_keys and dest_keys) or len(source_keys) != len(dest_keys):
        raise ValueError("source_keys and dest_keys must be non-empty and of equal length")

    for src, dst in zip(source_keys, dest_keys):
        src = src.strip()
        dst = dst.rstrip('/') + '/'

        # Auto-detect folder
        folder_prefix = src if src.endswith('/') else src + '/'
        result = s3.list_objects_v2(Bucket=source_bucket, Prefix=folder_prefix, MaxKeys=1)
        is_folder = 'Contents' in result and result['Contents']

        if is_folder:
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=source_bucket, Prefix=folder_prefix):
                for obj in page.get('Contents', []):
                    skey = obj['Key']
                    if skey.endswith('/'):
                        continue
                    dkey = skey.replace(folder_prefix, dst, 1)
                    s3.copy_object(
                        Bucket=target_bucket,
                        CopySource={'Bucket': source_bucket, 'Key': skey},
                        Key=dkey
                    )
                    print(f"[folder] Copied {skey} → {dkey}")
                    if dkey.endswith('.pgp'):
                        trigger_lambda(lambda_client, lambda_function_name, target_bucket, dkey)
        else:
            # File copy
            file_name = src.split('/')[-1]
            dest_key = f"{dst}{file_name}"
            s3.copy_object(
                Bucket=target_bucket,
                CopySource={'Bucket': source_bucket, 'Key': src},
                Key=dest_key
            )
            print(f"[file] Copied {src} → {dest_key}")
            if dest_key.endswith('.pgp'):
                trigger_lambda(lambda_client, lambda_function_name, target_bucket, dest_key)

def trigger_lambda(lambda_client, lambda_function_name, bucket, key):
    payload = {
        "bucket": bucket,
        "key": key
    }
    print(f"Invoking Lambda {lambda_function_name} for decryption → {key}")
    response = lambda_client.invoke(
        FunctionName=lambda_function_name,
        InvocationType='Event',  # async
        Payload=str(payload).encode('utf-8')
    )
    print(f"Lambda invoke status: {response['StatusCode']}")

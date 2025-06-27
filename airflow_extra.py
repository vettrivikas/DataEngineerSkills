import boto3
import json
import ast
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException


def trigger_lambda(lambda_client, lambda_function_name, key, bucket, dest_path, errfolderpath, decrypt_secret_key, decrypt_assume_role):
    payload = {
        'key': key,
        'bucket': bucket,
        'dest_path': dest_path,
        'errfolderpath': errfolderpath,
        'pubKeyStoreName': decrypt_secret_key,
        'assumerolearn': decrypt_assume_role
    }

    print(f"Invoking Lambda: {lambda_function_name} with payload: {payload}")

    response = lambda_client.invoke(
        FunctionName=lambda_function_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )

    response_payload = response['Payload'].read().decode('utf-8')
    print(f"Lambda Response: {response_payload}")

    try:
        parsed = ast.literal_eval(response_payload)
    except Exception as e:
        raise AirflowException(f"Failed to parse Lambda response: {e}")

    if int(parsed.get("statusCode", 500)) != 200:
        raise AirflowException(f"Lambda failed decrypting {key}: {parsed.get('body')}")
    else:
        print(f"Lambda succeeded for {key}")


def s3_copy_from_config(**kwargs):
    s3 = boto3.client('s3')
    lambda_client = boto3.client('lambda')

    source_bucket = kwargs['dag_run'].conf.get('source_bucket')
    target_bucket = kwargs['dag_run'].conf.get('target_bucket')
    config_file_key = kwargs['dag_run'].conf.get('config_file_key')
    config_file_bucket = kwargs['dag_run'].conf.get('config_file_bucket', source_bucket)
    lambda_function_name = kwargs['dag_run'].conf.get('lambda_function_name')
    decrypt_secret_key = kwargs['dag_run'].conf.get('decrypt_secret_key')
    decrypt_assume_role = kwargs['dag_run'].conf.get('decrypt_assume_role')
    staging_path = kwargs['dag_run'].conf.get('staging_path')
    error_path = kwargs['dag_run'].conf.get('error_path')
    folder_date = kwargs['dag_run'].conf.get('folderdate')  # e.g. 20240530

    # Read config file from S3
    config_obj = s3.get_object(Bucket=config_file_bucket, Key=config_file_key)
    lines = config_obj['Body'].read().decode('utf-8').splitlines()

    for line in lines:
        if not line.strip() or '|' not in line:
            continue

        src_prefix, file_pattern, dst_prefix = [x.strip() for x in line.strip().split('|')]

        # Replace placeholders
        src_prefix = src_prefix.replace("<YYYYMMDD>", folder_date).replace("<S3-Copy-YYYYMMDD>", f"S3-Copy-{folder_date}")
        dst_prefix = dst_prefix.replace("<YYYYMMDD>", folder_date).replace("<S3-Copy-YYYYMMDD>", f"S3-Copy-{folder_date}")

        if file_pattern == '*':
            # Folder mode
            folder_prefix = src_prefix if src_prefix.endswith('/') else src_prefix + '/'
            result = s3.list_objects_v2(Bucket=source_bucket, Prefix=folder_prefix, MaxKeys=1)
            if 'Contents' not in result:
                print(f"No contents in: {folder_prefix}")
                continue

            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=source_bucket, Prefix=folder_prefix):
                for obj in page.get('Contents', []):
                    skey = obj['Key']
                    if skey.endswith('/'):
                        continue
                    dkey = skey.replace(folder_prefix, dst_prefix.rstrip('/') + '/', 1)

                    if dkey.endswith('.pgp'):
                        trigger_lambda(lambda_client, lambda_function_name, dkey, target_bucket, staging_path, error_path, decrypt_secret_key, decrypt_assume_role)
                    else:
                        s3.copy_object(
                            Bucket=target_bucket,
                            CopySource={'Bucket': source_bucket, 'Key': skey},
                            Key=dkey
                        )
                        print(f"[FOLDER] Copied {skey} → {dkey}")

        else:
            # Single file mode
            full_src_key = f"{src_prefix.rstrip('/')}/{file_pattern}"
            full_dst_key = f"{dst_prefix.rstrip('/')}/{file_pattern}"

            if full_dst_key.endswith('.pgp'):
                trigger_lambda(lambda_client, lambda_function_name, full_dst_key, target_bucket, staging_path, error_path, decrypt_secret_key, decrypt_assume_role)
            else:
                s3.copy_object(
                    Bucket=target_bucket,
                    CopySource={'Bucket': source_bucket, 'Key': full_src_key},
                    Key=full_dst_key
                )
                print(f"[FILE] Copied {full_src_key} → {full_dst_key}")


# Define DAG
with DAG(
    dag_id='s3_copy_with_lambda_decrypt_from_config',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['s3', 'lambda', 'decrypt', 'config']
) as dag:

    run_copy_and_decrypt = PythonOperator(
        task_id='read_config_and_copy_with_lambda',
        python_callable=s3_copy_from_config,
        provide_context=True
    )

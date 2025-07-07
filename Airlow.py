from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException

import boto3
import subprocess
from botocore.exceptions import ClientError

# CONFIG
bucket = "your-bucket-name"
S3ConfigFilePath = "your/config/path/config.txt"
aws_region = "ap-south-1"

s3 = boto3.client("s3", region_name=aws_region)

def s3_copy_files(**kwargs):
    try:
        folder_date = kwargs['dag_run'].conf.get('folderdate')
        if not folder_date:
            raise ValueError("Missing 'folderdate' in dag_run.conf")

        response = s3.get_object(Bucket=bucket, Key=S3ConfigFilePath)
        file_content_lines = response['Body'].read().decode('utf-8').strip().split('\n')

        for line in file_content_lines:
            if not line.strip() or '|' not in line:
                continue

            try:
                src_prefix, file_pattern, dst_prefix = [x.strip() for x in line.strip().split('|')]
                src_prefix = src_prefix.replace("{0}", folder_date)
                S3_Copy_Folder_name = folder_date.strip("/").split("/")[0]

                # Handle destination structure
                if "REP" in dst_prefix:
                    subfolder_name = src_prefix.rstrip('/').split('/')[-1]
                    dst_prefix = dst_prefix.rstrip("/") + f"/S3-COPY-{S3_Copy_Folder_name}/{subfolder_name}"

                src_prefix = src_prefix.lstrip('/')
                dst_prefix = dst_prefix.lstrip('/')

                print(f"\nProcessing:\nSRC: {src_prefix}\nDST: {dst_prefix}\nPATTERN: {file_pattern}")

                full_key = f"{src_prefix.rstrip('/')}/{file_pattern.lstrip('/')}"
                is_folder = False

                try:
                    s3.head_object(Bucket=bucket, Key=full_key)
                    print("Identified as a file")
                except ClientError:
                    print("Not a file, checking if folder...")
                    result = s3.list_objects_v2(Bucket=bucket, Prefix=full_key, MaxKeys=1)
                    if "Contents" in result:
                        is_folder = True
                        print("Identified as a folder")
                        src_prefix = full_key
                        dst_prefix = dst_prefix.rstrip("/") + "/" + folder_date.lstrip('/')
                    else:
                        print("Source not found, skipping.")
                        continue

                if is_folder or file_pattern in ("*", "SubFolder"):
                    # Folder copy
                    src_uri = f"s3://{bucket}/{src_prefix.rstrip('/')}/"
                    dst_uri = f"s3://{bucket}/{dst_prefix.rstrip('/')}/"
                    print(f"[Recursive Copy] {src_uri} → {dst_uri}")
                    result = subprocess.call(["aws", "s3", "cp", src_uri, dst_uri, "--recursive"])
                    if result != 0:
                        raise AirflowException(f"Recursive copy failed: {src_uri} → {dst_uri}")
                else:
                    # Single file copy
                    file_name = full_key.split('/')[-1]
                    dst_key = f"{dst_prefix.rstrip('/')}/{file_name}"
                    src_uri = f"s3://{bucket}/{full_key}"
                    dst_uri = f"s3://{bucket}/{dst_key}"
                    print(f"[File Copy] {src_uri} → {dst_uri}")
                    result = subprocess.call(["aws", "s3", "cp", src_uri, dst_uri])
                    if result != 0:
                        raise AirflowException(f"Copy failed: {src_uri} → {dst_uri}")

            except Exception as file_error:
                print(f"Error processing line: {line}\n{str(file_error)}")

    except Exception as dag_error:
        raise AirflowException(f"DAG failed due to error: {str(dag_error)}")

# DAG Definition
with DAG(
    dag_id="s3_copy_from_config_with_folderdate",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    params={"folderdate": "20250701"},
    tags=["s3", "copy", "config"],
) as dag:

    run_copy = PythonOperator(
        task_id="copy_files_from_s3_config",
        python_callable=s3_copy_files,
        provide_context=True
    )

    run_copy

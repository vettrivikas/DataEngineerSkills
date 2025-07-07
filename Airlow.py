import subprocess
import os
from botocore.exceptions import ClientError
from airflow.exceptions import AirflowException

def s3_copy_and_decrypt(**kwargs):
    folder_date = kwargs['dag_run'].conf.get('folderdate')

    response = s3.get_object(Bucket=bucket, Key=S3ConfigFilePath)
    file_content_lines = response['Body'].read().decode('utf-8').strip().split('\n')

    for line in file_content_lines:
        if not line.strip() or '|' not in line:
            continue

        src_prefix, file_pattern, dst_prefix = [x.strip() for x in line.strip().split('|')]

        src_prefix = src_prefix.replace('{0}', folder_date)
        S3_Copy_Folder_name = folder_date.strip("/").split("/")[0]

        # If REP is in destination path, append /S3-COPY-{folderdate}/<subfolder>
        if "REP" in dst_prefix:
            subfolder_name = src_prefix.rstrip('/').split('/')[-1]
            dst_prefix = dst_prefix.rstrip("/") + f"/S3-COPY-{S3_Copy_Folder_name}/{subfolder_name}"

        src_prefix = src_prefix.lstrip('/')
        dst_prefix = dst_prefix.lstrip('/')

        print(f"SRC: {src_prefix}")
        print(f"DST: {dst_prefix}")

        file_pattern_check = src_prefix.rstrip('/') + "/" + file_pattern.lstrip('/')

        try:
            s3.head_object(Bucket=bucket, Key=file_pattern_check)
            is_folder = False
            print("It's a file")
        except ClientError:
            file_pattern_check_response = s3.list_objects_v2(Bucket=bucket, Prefix=file_pattern_check)
            if "Contents" in file_pattern_check_response:
                print("It's a folder")
                file_pattern = "SubFolder"
                is_folder = True
                src_prefix = file_pattern_check
                dst_prefix = dst_prefix.rstrip("/") + "/" + folder_date.lstrip('/')
            else:
                continue

        if file_pattern in ("*", "SubFolder"):
            # Folder case
            folder_prefix = src_prefix if src_prefix.endswith('/') else src_prefix + '/'
            result = s3.list_objects_v2(Bucket=bucket, Prefix=folder_prefix, MaxKeys=1)

            if 'Contents' not in result:
                print(f"No contents in: {folder_prefix}")
                continue

            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=folder_prefix):
                for obj in page.get('Contents', []):
                    skey = obj['Key']
                    if skey.endswith('/'):
                        continue
                    dkey = skey.replace(folder_prefix, dst_prefix.rstrip('/') + '/', 1)
                    src_uri = f"s3://{bucket}/{skey}"
                    dst_uri = f"s3://{bucket}/{dkey}"
                    print(f"[Folder] Moving {src_uri} → {dst_uri}")
                    result = subprocess.call(["aws", "s3", "mv", src_uri, dst_uri])
                    if result != 0:
                        raise AirflowException(f"Move failed: {src_uri} → {dst_uri}")
        else:
            # Single file case
            src = f"{src_prefix.rstrip('/')}/{file_pattern}"
            dst = f"{dst_prefix.rstrip('/')}/"
            file_name = src.split('/')[-1]
            dest_key = f"{dst}{file_name}"
            src_uri = f"s3://{bucket}/{src}"
            dst_uri = f"s3://{bucket}/{dest_key}"
            print(f"[File] Moving {src_uri} → {dst_uri}")
            result = subprocess.call(["aws", "s3", "mv", src_uri, dst_uri])
            if result != 0:
                raise AirflowException(f"Move failed: {src_uri} → {dst_uri}")

def s3_copy_and_decrypt(**kwargs):
    import boto3
    import os

    s3 = boto3.client('s3')
    lambda_client = boto3.client('lambda')

    # Airflow-passed folderdate
    folder_date = kwargs['dag_run'].conf.get('S3-Copy-folderdate')
    if not folder_date:
        raise ValueError("Missing S3-Copy-folderdate in DAG config")

    bucket = "your-bucket-name"
    s3_config_file_path = "path/to/config.txt"
    lambda_function_name = "your-lambda-name"
    errfolderpath = "s3://your-error-folder/"
    decrypt_secret = "your-secret"
    assume_role = "your-role"

    # Read config file from S3
    response = s3.get_object(Bucket=bucket, Key=s3_config_file_path)
    file_lines = response['Body'].read().decode('utf-8').strip().split('\n')

    for line in file_lines:
        if not line.strip() or '"' not in line:
            continue

        try:
            src_prefix, file_pattern, dst_prefix = [x.strip() for x in line.strip().split('|')]

            # Inject folder_date into src path
            src_prefix = src_prefix.replace("{0}", folder_date).strip().lstrip('/')
            file_pattern = file_pattern.strip()

            # Build final destination: dst_prefix/s3-copy-<folder_date>/
            dst_prefix = dst_prefix.rstrip('/') + f"/s3-copy-{folder_date}"

            if file_pattern == '*':
                # Copy all under src_prefix/
                folder_prefix = src_prefix if src_prefix.endswith('/') else src_prefix + '/'
                paginator = s3.get_paginator('list_objects_v2')

                result = s3.list_objects_v2(Bucket=bucket, Prefix=folder_prefix, MaxKeys=1)
                if 'Contents' not in result:
                    print(f"No contents in folder: {folder_prefix}")
                    continue

                for page in paginator.paginate(Bucket=bucket, Prefix=folder_prefix):
                    for obj in page.get('Contents', []):
                        skey = obj['Key']
                        if skey.endswith('/'):
                            continue

                        dkey = skey.replace(folder_prefix, dst_prefix + '/', 1)

                        if dkey.endswith('.pgp'):
                            dest_folder = os.path.dirname(dkey.rstrip('/')) + '/'
                            trigger_lambda(lambda_client, lambda_function_name, skey, bucket, dest_folder, errfolderpath, decrypt_secret, assume_role)
                            print(f"[Decrypt] {skey} → {dest_folder}")
                        else:
                            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': skey}, Key=dkey)
                            print(f"[Folder Copy] {skey} → {dkey}")

            elif '/' not in file_pattern and '.' not in file_pattern:
                # This is a subfolder copy like "subfolder"
                subfolder_prefix = f"{src_prefix}/{file_pattern}/"
                paginator = s3.get_paginator('list_objects_v2')

                result = s3.list_objects_v2(Bucket=bucket, Prefix=subfolder_prefix, MaxKeys=1)
                if 'Contents' not in result:
                    print(f"No contents in subfolder: {subfolder_prefix}")
                    continue

                for page in paginator.paginate(Bucket=bucket, Prefix=subfolder_prefix):
                    for obj in page.get('Contents', []):
                        skey = obj['Key']
                        if skey.endswith('/'):
                            continue

                        dkey = skey.replace(src_prefix, dst_prefix, 1)

                        if dkey.endswith('.pgp'):
                            dest_folder = os.path.dirname(dkey.rstrip('/')) + '/'
                            trigger_lambda(lambda_client, lambda_function_name, skey, bucket, dest_folder, errfolderpath, decrypt_secret, assume_role)
                            print(f"[Decrypt] {skey} → {dest_folder}")
                        else:
                            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': skey}, Key=dkey)
                            print(f"[Subfolder Copy] {skey} → {dkey}")

            else:
                # Single file copy
                src = f"{src_prefix}/{file_pattern}"
                dst = f"{dst_prefix}/"
                file_name = file_pattern.split('/')[-1]
                dest_key = f"{dst}{file_name}"

                if dest_key.endswith('.pgp'):
                    dest_folder = os.path.dirname(dest_key.rstrip('/')) + '/'
                    trigger_lambda(lambda_client, lambda_function_name, src, bucket, dest_folder, errfolderpath, decrypt_secret, assume_role)
                    print(f"[Decrypt] {src} → {dest_folder}")
                else:
                    s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': src}, Key=dest_key)
                    print(f"[File Copy] {src} → {dest_key}")

        except Exception as e:
            print(f"Error processing config line: {line} → {e}")

response = s3.get_object(Bucket=bucket, Key=config_key)  
file_content_lines = response['Body'].read().decode('utf-8').strip().split('\n')  

for line in file_content_lines:  
    if not line.strip() or '|' not in line:  
        continue  

    src_base, middle_value, dst_base = [x.strip().rstrip('/') for x in line.strip().split('|')]  
    src_full_prefix = src_base.replace('{0}', folder_date).rstrip('/')  
    dst_base = dst_base.rstrip('/')  
    
    if middle_value == '*':  
        # Copy all under src_full_prefix  
        folder_prefix = f"{src_full_prefix}/"  
        dst_folder_prefix = f"{dst_base}/S3-COPY-{folder_date}/"  
        print(f"[Wildcard] Copying folder: {folder_prefix} → {dst_folder_prefix}")  

        paginator = s3.get_paginator('list_objects_v2')  
        for page in paginator.paginate(Bucket=bucket, Prefix=folder_prefix):  
            for obj in page.get('Contents', []):  
                skey = obj['Key']  
                if skey.endswith('/'):  
                    continue  
                relative_path = skey[len(folder_prefix):]  
                dkey = f"{dst_folder_prefix}{relative_path}"  
                print(f"Copying {skey} → {dkey}")  
                s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': skey}, Key=dkey)  

    else:  
        # Determine if it's file or folder  
        src_key = f"{src_full_prefix}/{middle_value}".rstrip('/')  
        folder_prefix = f"{src_key}/"  

        result = s3.list_objects_v2(Bucket=bucket, Prefix=folder_prefix, MaxKeys=1)  
        is_folder = 'Contents' in result  

        if is_folder:  
            dst_folder_prefix = f"{dst_base}/S3-COPY-{folder_date}/{middle_value}/"  
            print(f"Copying folder: {folder_prefix} → {dst_folder_prefix}")  
            paginator = s3.get_paginator('list_objects_v2')  
            for page in paginator.paginate(Bucket=bucket, Prefix=folder_prefix):  
                for obj in page.get('Contents', []):  
                    skey = obj['Key']  
                    if skey.endswith('/'):  
                        continue  
                    relative_path = skey[len(folder_prefix):]  
                    dkey = f"{dst_folder_prefix}{relative_path}"  
                    print(f"Copying {skey} → {dkey}")  
                    s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': skey}, Key=dkey)  

        else:  
            dst_key = f"{dst_base}/S3-COPY-{folder_date}/{middle_value}"  
            print(f"Copying file: {src_key} → {dst_key}")  
            s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': src_key}, Key=dst_key)  

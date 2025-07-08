import boto3

s3 = boto3.client('s3')

def archivefile():
    try:
        # Step 1: Read the config file from S3
        config_obj = s3.get_object(Bucket=bucket_name, Key=S3ConfigFilePath)
        config_data = config_obj['Body'].read().decode('utf-8')

        # Step 2: Add new line (example: hardcoded key-value pair)
        new_line = "NEW_PARAM=enabled"
        if not config_data.endswith('\n'):
            config_data += '\n'
        config_data += new_line + '\n'

        # Step 3: Upload updated config to archive path
        s3.put_object(
            Bucket=bucket_name,
            Key=ArchiveFilePath,
            Body=config_data,
            ContentType='text/plain'
        )

        print(f"File successfully updated and archived at: {ArchiveFilePath}")
        # send_sns_notification(f"File archived to: {ArchiveFilePath}", status="[Success]", jobName='Adhoc_RedshiftQueryExecution')

        return True

    except Exception as Archive_Exception:
        error_msg = Archive_Exception.get('M') if isinstance(Archive_Exception, dict) else str(Archive_Exception)

        message = (
            f"There was a problem archiving the file: {error_msg}. "
            f"Check archive path: {ArchiveFilePath} and source: {S3ConfigFilePath}"
        )

        send_sns_notification(message, status="[Failure]", jobName='Adhoc_RedshiftQueryExecution')
        raise Exception(f"{error_msg}")

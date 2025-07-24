from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import csv
import io

def read_and_print_csv_from_s3():
    s3 = boto3.client('s3')
    bucket_name = 'your-bucket-name'
    key = 'path/to/your-file.csv'

    response = s3.get_object(Bucket=bucket_name, Key=key)
    content = response['Body'].read().decode('utf-8')

    reader = csv.DictReader(io.StringIO(content), delimiter='|')
    for row in reader:
        print(row)

with DAG(
    dag_id='s3_csv_reader_test',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['test']
) as dag:

    read_csv_task = PythonOperator(
        task_id='read_and_print_csv',
        python_callable=read_and_print_csv_from_s3
    )

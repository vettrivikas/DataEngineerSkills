from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import redshift_connector
import boto3
from io import StringIO
import csv

def export_redshift_to_s3():
    # Connect to Redshift
    conn = redshift_connector.connect(
        host='your-redshift-cluster.amazonaws.com',
        database='your_db',
        user='your_user',
        password='your_password',
        port=5439
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM your_schema.your_table")
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    conn.close()

    # Write to CSV with | delimiter
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer, delimiter='|', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(headers)
    writer.writerows(rows)

    # Upload to S3
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket='your-s3-bucket',
        Key='your-folder/exported_data_pipe_delimited.csv',
        Body=csv_buffer.getvalue()
    )
    print("✅ Exported with '|' delimiter")

# Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='redshift_export_pipe_delim',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    export_task = PythonOperator(
        task_id='export_redshift_to_s3',
        python_callable=export_redshift_to_s3
    )

    export_task

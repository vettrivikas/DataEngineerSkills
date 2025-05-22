from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import redshift_connector
import pandas as pd
import boto3
from io import StringIO

# Function to export data from Redshift and upload to S3
def export_redshift_to_s3():
    # Connect to Redshift
    conn = redshift_connector.connect(
        host='your-redshift-cluster.amazonaws.com',
        database='your_db',
        user='your_user',
        password='your_password',
        port=5439
    )
    query = "SELECT * FROM your_schema.your_table"
    df = pd.read_sql(query, conn)
    conn.close()

    # Convert DataFrame to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload to S3
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket='your-s3-bucket',
        Key='your-folder/table_export.csv',
        Body=csv_buffer.getvalue()
    )
    print("✅ Export complete")

# Define DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='export_redshift_to_s3',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    export_task = PythonOperator(
        task_id='export_data',
        python_callable=export_redshift_to_s3
    )

    export_task

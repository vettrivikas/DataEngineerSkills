# dag1.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime
# from shared_dataset import dag1_completion_dataset


from airflow.datasets import Dataset

dag1_completion_dataset = Dataset("gs://my-airflow-dag-bucket/dag1/complete.flag")


def create_gcs_flag():
    hook = GCSHook(gcp_conn_id='google_cloud_default')  # Uses default VM credentials
    bucket_name = 'my-airflow-dag-bucket'  # ✅ Replace with your GCS bucket name
    object_path = 'dag1/complete.flag'
    content = 'done'

    hook.upload(bucket_name=bucket_name, object_name=object_path, data=content, mime_type='text/plain')


with DAG(
        dag_id='dag1',
        start_date=datetime(2024, 1, 1),
        schedule_interval='@daily',
        catchup=False,
        description='DAG 1 generates a flag in GCS and triggers DAG 2',
) as dag:
    start = EmptyOperator(task_id='start')

    generate_flag = PythonOperator(
        task_id='generate_gcs_flag',
        python_callable=create_gcs_flag,
        outlets=[dag1_completion_dataset]  # Triggers downstream DAG
    )

    start >> generate_flag


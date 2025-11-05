from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 1, 1),
    'catchup': False
}

# --- DAG 1 ---
with DAG(
    dag_id='glue_job1_dag',
    default_args=default_args,
    schedule_interval=None,
) as dag1:
    glue_job1 = AwsGlueJobOperator(
        task_id='run_glue_job1',
        job_name='your_glue_job1_name',  # replace with actual Glue job name
        region_name='ap-south-1',        # your AWS region
    )

# --- DAG 2 ---
with DAG(
    dag_id='glue_job2_dag',
    default_args=default_args,
    schedule_interval=None,
) as dag2:
    glue_job2 = AwsGlueJobOperator(
        task_id='run_glue_job2',
        job_name='your_glue_job2_name',  # replace with actual Glue job name
        region_name='ap-south-1',
    )



column_specs = [('BDY_REC_TYPE':(3, str), 'FI_INSTRUMENT_ID':(30, str), ...
...
'FI_INITBAL_AMT': (19, int), 'FI_INTBAL_AMT': (19, int), 'FI_INITCMT_AMT': (19, int), 'FI_INTCMT_AMT': (19, int), 
'FTP_TERM_MATURITY': (11, int), 'FI_PAYMENT_AMT': (19, int), 'FI_PAYMENT_BCE_AMT': (19, int), 'RATE_RESET_TYPE': (2, str), 
'FI_INTEREST_RATE': (13, int), 'FI_PRINDX_MARGIN': (9, int), 'COUNTRY_SECTOR_CD': (10, str), ...

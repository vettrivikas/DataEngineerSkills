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



"SELECT
    BDY_REC_TYPE,
    rpad(CAST(FI_INSTRUMENT_ID AS VARCHAR), 30, ' ') AS FI_INSTRUMENT_ID,
    ASDF_DT,
    rpad(CAST(PF_SYSTEM_CODE AS VARCHAR), 10, ' ') AS PF_SYSTEM_CODE,
    rpad(CAST(PF_SRC_SYSTEM_ID AS VARCHAR), 20, ' ') AS PF_SRC_SYSTEM_ID,
    rpad(CAST(FI_PREV_INSTR_ID AS VARCHAR), 20, ' ') AS FI_PREV_INSTR_ID,
    rpad(CAST(OPERATING_UNIT AS VARCHAR), 8, ' ') AS OPERATING_UNIT,
    rpad(CAST(DEPTID AS VARCHAR), 10, ' ') AS DEPTID,
    rpad(CAST(RMO_SRV_PRODUCT AS VARCHAR), 30, ' ') AS RMO_SRV_PRODUCT,
    rpad(CAST(CHANNEL_ID AS VARCHAR), 6, ' ') AS CHANNEL_ID,
    rpad(CAST(PRODUCT_ID AS VARCHAR), 18, ' ') AS PRODUCT_ID,
    FI_START_DATE,
    rpad(CAST(FI_END_DATE AS VARCHAR), 8, ' ') AS FI_END_DATE,

    -- 🔴 FIX APPLIED HERE: Explicitly padding all 19-char amount fields
    rpad(CAST(FI_INITBAL_AMT AS VARCHAR), 19, ' ') AS FI_INITBAL_AMT,
    rpad(CAST(FI_INITBAL_BCE_AMT AS VARCHAR), 19, ' ') AS FI_INITBAL_BCE_AMT,
    rpad(CAST(FI_INITCMT_AMT AS VARCHAR), 19, ' ') AS FI_INITCMT_AMT,
    rpad(CAST(FI_INTCMT_AMT AS VARCHAR), 19, ' ') AS FI_INTCMT_AMT,
    rpad(CAST(FTP_TERM_MATURITY AS VARCHAR), 11, ' ') AS FTP_TERM_MATURITY,
    rpad(CAST(FI_PAYMENT_AMT AS VARCHAR), 19, ' ') AS FI_PAYMENT_AMT,
    rpad(CAST(FI_PAYMENT_BCE_AMT AS VARCHAR), 19, ' ') AS FI_PAYMENT_BCE_AMT,

    -- Other fields requiring explicit padding
    rpad(CAST(RATE_RESET_TYPE AS VARCHAR), 2, ' ') AS RATE_RESET_TYPE,
    rpad(CAST(FI_INTEREST_RATE AS VARCHAR), 13, ' ') AS FI_INTEREST_RATE,
    rpad(CAST(FI_PRINDX_MARGIN AS VARCHAR), 9, ' ') AS FI_PRINDX_MARGIN,

    rpad(CAST(COUNTRY_SECTOR_CD AS VARCHAR), 10, ' ') AS COUNTRY_SECTOR_CD,
    rpad(CAST(FI_LST_XLAT_DT AS VARCHAR), 8, ' ') AS FI_LST_XLAT_DT,
    rpad(CAST(FI_EVENT_SCHEDULE AS VARCHAR), 1, ' ') AS FI_EVENT_SCHEDULE,
    FI_COMP_FREQ,
    rpad(CAST(FI_COMP_FREQ_UOM AS VARCHAR), 2, ' ') AS FI_COMP_FREQ_UOM
FROM
    EXTRACTS_XEPM_PCG_ORG_9799_INSTR
WHERE
    AUDIT_ID = (SELECT MAX(AUDIT_ID) FROM EXTRACTS_XEPM_PCG_ORG_9799_INSTR)"

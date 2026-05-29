from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# ------------------------------------------------------------------
# TEST CONFIG
# Replace with S3 Config Reader later
# ------------------------------------------------------------------

CONFIG = {
    "cde": [
        {
            "flagName": "file1",
            "fileName": "cde_file1.csv",
            "outboundFolder": "cde"
        },
        {
            "flagName": "file2",
            "fileName": "cde_file2.csv",
            "outboundFolder": "cde"
        }
    ],
    "pw": [
        {
            "flagName": "file1",
            "fileName": "pw_file1.csv",
            "outboundFolder": "pw"
        },
        {
            "flagName": "file2",
            "fileName": "pw_file2.csv",
            "outboundFolder": "pw"
        },
        {
            "flagName": "file3",
            "fileName": "pw_file3.csv",
            "outboundFolder": "pw"
        }
    ],
    "iw": [
        {
            "flagName": "file1",
            "fileName": "iw_file1.csv",
            "outboundFolder": "iw"
        }
    ]
}

# ------------------------------------------------------------------
# CONSTANTS
# ------------------------------------------------------------------

REGION_NAME = "ca-central-1"
GLUE_ROLE = "GlueRole"

STANDALONE_GLUE_JOB = "adhoc_standalone_glue_job"
DQ_GLUE_JOB = "dq_glue_job"

ENCRYPTION_DAG = "CR27EncryptionLambda"

OUTBOUND_PATH = "/tmp/outbound"


# ------------------------------------------------------------------
# AUDIT TASK
# ------------------------------------------------------------------

def insert_audit():
    print("Audit inserted")


# ------------------------------------------------------------------
# DAG
# ------------------------------------------------------------------

with DAG(
    dag_id="adhoc_standalone_dynamic",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    params={
        "table_name": Param(
            default="cde",
            type="string"
        )
    },
    tags=["adhoc"],
) as dag:

    end = EmptyOperator(
        task_id="end"
    )

    audit_task = PythonOperator(
        task_id="insert_audit",
        python_callable=insert_audit
    )

    # --------------------------------------------------------------
    # Standalone Glue Job
    # --------------------------------------------------------------

    standalone_glue = GlueJobOperator(
        task_id="standalone_glue",
        job_name=STANDALONE_GLUE_JOB,
        region_name=REGION_NAME,
        iam_role_name=GLUE_ROLE,
        wait_for_completion=True,
        script_args={
            "--srcvalidation": "{{ params.table_name }}"
        }
    )

    # --------------------------------------------------------------
    # Read Config
    # --------------------------------------------------------------

    @task
    def get_files(table_name):

        if table_name not in CONFIG:
            raise Exception(
                f"{table_name} not found"
            )

        return CONFIG[table_name]

    file_configs = get_files(
        "{{ params.table_name }}"
    )

    # --------------------------------------------------------------
    # DQ Glue Job Mapping Payload
    # --------------------------------------------------------------

    @task
    def build_dq_payload(file_cfg):

        return {
            "--DQfileName": file_cfg["flagName"],
            "--input_file_path":
                f"{OUTBOUND_PATH}/"
                f"{file_cfg['outboundFolder']}/"
                f"{file_cfg['fileName']}"
        }

    dq_payloads = build_dq_payload.expand(
        file_cfg=file_configs
    )

    dq_tasks = GlueJobOperator.partial(
        task_id="dq_glue",
        job_name=DQ_GLUE_JOB,
        region_name=REGION_NAME,
        iam_role_name=GLUE_ROLE,
        wait_for_completion=True
    ).expand(
        script_args=dq_payloads
    )

    # --------------------------------------------------------------
    # Encryption DAG Mapping Payload
    # --------------------------------------------------------------

    @task
    def build_enc_payload(file_cfg):

        return {
            "flagName": file_cfg["flagName"],
            "fileName": file_cfg["fileName"],
            "outboundFolder": file_cfg["outboundFolder"]
        }

    enc_payloads = build_enc_payload.expand(
        file_cfg=file_configs
    )

    encryption_tasks = TriggerDagRunOperator.partial(
        task_id="encryption",
        trigger_dag_id=ENCRYPTION_DAG,
        wait_for_completion=True
    ).expand(
        conf=enc_payloads
    )

    # --------------------------------------------------------------
    # Dependencies
    # --------------------------------------------------------------

    (
        audit_task
        >> standalone_glue
        >> file_configs
        >> dq_payloads
        >> dq_tasks
        >> enc_payloads
        >> encryption_tasks
        >> end
    )

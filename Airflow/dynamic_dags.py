"""
Dynamic extract DAG generator  (Airflow 2.10.3 / MWAA mw1.medium)

Design rules
------------
1. PARSE TIME DOES NO NETWORK I/O except ONE cached S3 read of the config and ONE of the
   snapshot.  No Redshift, no Secrets Manager, no Glue, no CloudWatch at parse time.
   (Old version: ~3-4 Redshift queries x every DAG x every parse -> import timeouts.)
2. Anything that needs Redshift (current period, BD calendar, "is batch COMPLETED") is
   resolved by ONE small refresher DAG that writes a snapshot JSON to S3.
3. A bad config entry / missing snapshot / S3 blip can never raise at import time:
   the entry is logged and skipped, or the DAG is created with schedule=None.
4. Long waits (Glue, TriggerDagRun) are deferrable -> they free the worker slot, which is
   what lets MWAA autoscaling stay at the minimum worker count.
5. Nothing time-sensitive is computed at parse time for use at run time.
"""
import calendar
import json
import logging
import os
import tempfile
import time
from datetime import datetime, timedelta
from functools import lru_cache, partial

import boto3
from botocore.config import Config
from dateutil.relativedelta import relativedelta
from pytz import timezone

from airflow.exceptions import AirflowSkipException
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator, get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

import DagCFG            # must be plain constants only - no I/O at import
import Dag_Util          # must not open DB connections at import
from sns_notifier import notify_task_failure, send_sns_notification

log = logging.getLogger(__name__)

TZ = timezone("America/Toronto")
DAG_FILE_PATH = os.path.basename(__file__)
ENVIRONMENT_PREFIX = "QA_"

BUCKET = DagCFG.FBPA_BUCKET_NAME
REGION = DagCFG.REGION_NAME
GLUE_ROLE = DagCFG.GLUE_EXEC_ROLE
CONFIG_KEY = DagCFG.Airflow_CONFIG_PATH
SNAPSHOT_KEY = getattr(DagCFG, "SNAPSHOT_KEY", "config/airflow_dag_snapshot.json")
LAMBDA_LOG_GROUP_MAP = getattr(DagCFG, "LAMBDA_LOG_GROUP_MAP", {})

# Set to False in DagCFG to fall back to classic (slot-holding) behaviour if needed.
USE_DEFERRABLE = getattr(DagCFG, "USE_DEFERRABLE", True)

CONFIG_TTL_SEC = 300      # config changes are picked up within 5 min
SNAPSHOT_TTL_SEC = 120
REQUIRED_KEYS = ("dag_id", "GlueJob", "PROCESS_NAME", "scheduled_day",
                 "Encryption_Required", "Ingestion_Required", "Arguments")


# ----------------------------------------------------------------------------------------
# Cheap helpers (lazy clients, cached S3 JSON)
# ----------------------------------------------------------------------------------------
@lru_cache(maxsize=None)
def _client(name):
    return boto3.client(name, region_name=REGION,
                        config=Config(connect_timeout=3, read_timeout=10,
                                      retries={"max_attempts": 2}))


def _cached_s3_json(key, ttl):
    """Read a JSON object from S3 with a local /tmp file cache.
    Returns the parsed object, the stale cached copy on S3 failure, or None. Never raises."""
    path = os.path.join(tempfile.gettempdir(), "dagcache_" + key.replace("/", "_"))
    try:
        if os.path.exists(path) and time.time() - os.path.getmtime(path) < ttl:
            with open(path) as f:
                return json.load(f)
    except Exception:
        log.warning("Local cache for %s unreadable; refetching", key)
    try:
        body = _client("s3").get_object(Bucket=BUCKET, Key=key)["Body"].read().decode("utf-8")
        data = json.loads(body)
        tmp = path + ".%d.tmp" % os.getpid()
        with open(tmp, "w") as f:
            f.write(body)
        os.replace(tmp, path)          # atomic: parallel parser processes are safe
        return data
    except Exception as e:
        log.error("Could not read s3://%s/%s: %s - using stale cache if present", BUCKET, key, e)
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            return None


def _process_name(dag_id):
    return dag_id.replace(ENVIRONMENT_PREFIX, "") if ENVIRONMENT_PREFIX in dag_id else dag_id


def _resolve_period(qe_flag, get_value):
    """Returns (curr_period, current_me). get_value(key) supplies curr_period / curr_end_date
    for non-QE dags (from snapshot at parse time, from Redshift in the refresher)."""
    if qe_flag != "TRUE":
        return get_value("curr_period"), get_value("curr_end_date")
    now = datetime.now(TZ)
    return (now.strftime("%Y%m"),
            (now.replace(day=1) - relativedelta(days=1)).strftime("%Y%m%d"))


def _build_schedule(schedule_date, qe_flag):
    """Same cron rules as before; pure function, no I/O."""
    if not schedule_date:
        return None
    now = datetime.now(TZ)
    end_day = "%02d" % calendar.monthrange(now.year, now.month)[1]
    start_day = schedule_date.split("-")[2]
    if qe_flag == "TRUE":
        return f"0 * {start_day}-{end_day} 2,5,8,11 *"
    if int(start_day) >= int(end_day):
        return f"0 * {end_day} * *"
    if int(end_day) - int(start_day) >= 5:
        end_day = str(int(start_day) + 4)
    return f"0 * {start_day}-{end_day} * *"


def _safe_xcom_run_id(context, task_id):
    try:
        v = context["task_instance"].xcom_pull(task_ids=task_id, key="glue_job_run_details")
        return v["job_run_id"] if isinstance(v, dict) else v
    except Exception:
        return "n/a"


# ----------------------------------------------------------------------------------------
# DAG factory
# ----------------------------------------------------------------------------------------
def create_dag(p, snap):
    dag_id, glue_job = p["dag_id"], p["GlueJob"]
    scheduled_day = p["scheduled_day"]
    qe_flag, dq_flag = p["QE_flag"], p["DQFlag"]
    enc_required, ing_required = p["Encryption_Required"], p["Ingestion_Required"]
    arguments = p["Arguments"]
    script_arg = dict(p["script_arg"])           # copy: never mutate the loaded config
    process_details = p["process_details"]
    process_name = _process_name(dag_id)

    # ---- everything below comes from the snapshot (no DB) -------------------------------
    curr_period, current_me = _resolve_period(qe_flag, lambda k: snap.get(k))
    if qe_flag == "TRUE" and current_me:
        script_arg["--current_me"] = current_me
    dag_batch_id = f"{scheduled_day}_{current_me}"
    schedule_date = snap.get("bd_dates", {}).get(f"{curr_period}|{scheduled_day}")
    dag_status = snap.get("dag_status", {}).get(dag_id, "UNKNOWN")
    schedule = None if dag_status == "COMPLETED" else _build_schedule(schedule_date, qe_flag)
    date_format = {"YYYYMM": (current_me or "")[0:6], "YYYYMMDD": current_me or "", "NONE": ""}

    # ---- run-time callbacks -------------------------------------------------------------
    def update_success(context):
        run_id = context["run_id"]
        end_ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
        sla = Dag_Util.getSLA(end_ts, process_name, schedule_date)
        q = (f"UPDATE AUDIT.BATCH_RUN_STATUS SET ACTUAL_END_TS = '{end_ts}', STATUS = 'SUCCESS', "
             f"SLA = '{sla}' WHERE PROCESS_NAME = '{process_name}' AND JOB_RUN_ID = '{run_id}';")
        log.info("Audit success update ok=%s", Dag_Util.writeToDb(q, "DML"))

    def update_failure(context, failure_reason, job_name, stage_value):
        run_id = context["run_id"]
        end_ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
        sla = Dag_Util.getSLA(end_ts, process_name, schedule_date)
        q = (f"UPDATE AUDIT.BATCH_RUN_STATUS SET ACTUAL_END_TS = '{end_ts}', STATUS = 'FAILED', "
             f"FAILED_REASON = '{failure_reason}', STAGE = '{stage_value}', SLA = '{sla}' "
             f"WHERE PROCESS_NAME = '{process_name}' AND JOB_RUN_ID = '{run_id}';")
        log.info("Audit failure update ok=%s", Dag_Util.writeToDb(q, "DML"))
        notify_task_failure(
            context, job_name=job_name, failure_reason=failure_reason, stage_value=stage_value,
            dag_file_path=DAG_FILE_PATH, glue_client=_client("glue"),
            logs_client=_client("logs"), region_name=REGION,
            lambda_log_group_map=LAMBDA_LOG_GROUP_MAP)

    def glue_success(context, job_name, task_id, what, extra=""):
        send_sns_notification(
            f"{job_name} Glue {what} had Succeeded{extra} with the Job Run Id "
            f"{_safe_xcom_run_id(context, task_id)}. Check this Airflow Dag Run id "
            f"{context['run_id']} in the Dag : {dag_id} for more details",
            jobName=job_name, status="[SUCCESS]")

    def insert_audit(context):
        # start time is taken NOW (run time) - the old code used the parse-time timestamp.
        now = datetime.now(TZ)
        q = ("INSERT INTO AUDIT.BATCH_RUN_STATUS (batch_id,process_name,actual_start_ts,status,"
             "failed_reason,sla,stage,job_run_id,load_dt) VALUES("
             f"'{dag_batch_id}','{process_name}','{now:%Y-%m-%d %H:%M:%S}','IN_PROGRESS','N/A',"
             f"'N/A','Redshift to L2','{context['run_id']}','{now:%Y-%m-%d}');")
        log.info("Audit insert ok=%s", Dag_Util.writeToDb(q, "DML"))

    # ---- run-time task callables ----------------------------------------------------------
    def check_dag_run(**_):
        if not current_me:
            raise ValueError("Calendar snapshot unavailable - run dag_snapshot_refresh and retry")
        context = get_current_context()
        run_type = (context["dag_run"].conf or {}).get("run")
        if run_type == "manual":
            log.info("Manual trigger - skipping completion check")
            return
        success_q = ("SELECT EXISTS (SELECT 1 FROM AUDIT.BATCH_RUN_STATUS WHERE PROCESS_NAME = "
                     f"'{dag_id}' AND BATCH_ID = '{dag_batch_id}' AND STATUS = 'SUCCESS') AS result;")
        already_ok = Dag_Util.writeToDb(success_q, "DQL")[0][0] is True
        if "BD-" in str(scheduled_day):                       # negative BD days: DB only
            if already_ok:
                raise AirflowSkipException("Already succeeded for this month-end in DB")
            return
        prev = context.get("prev_start_date_success")         # positive BD days: metadata or DB
        this_month = datetime.now(TZ).month
        if (prev and prev.astimezone(TZ).month == this_month) or already_ok:
            raise AirflowSkipException("Already succeeded this month")

    def schedule_check(**_):
        if not schedule_date:
            raise ValueError(f"No schedule_date found for BD day {scheduled_day}")
        rows = Dag_Util.writeToDb(
            f"SELECT EXPECTED_START_TS FROM AUDIT.BD_SLA_REFERENCE WHERE PROCESS_NAME = '{process_name}'",
            "DQL")
        if not rows:
            raise ValueError("Dag doesn't have entry in BD_SLA_REFERENCE table")
        expected = datetime.strptime(f"{schedule_date} {rows[0][0]}", "%Y-%m-%d %H:%M:%S")
        now_naive = datetime.now(TZ).replace(tzinfo=None)
        if now_naive < expected:
            raise ValueError("Time Dependency is not met to proceed further execution.")
        for proc, bd_day in process_details.items():
            q = ("SELECT STATUS FROM AUDIT.BATCH_RUN_STATUS WHERE PROCESS_NAME = "
                 f"'{proc}' AND BATCH_ID = '{bd_day}_{current_me}' AND LOG_ID_PK = (SELECT "
                 "MAX(LOG_ID_PK) FROM AUDIT.BATCH_RUN_STATUS WHERE PROCESS_NAME = "
                 f"'{proc}' AND BATCH_ID = '{bd_day}_{current_me}');")
            res = Dag_Util.writeToDb(q, "DQL")
            if not res or res[0][0] != "SUCCESS":
                raise ValueError(f"The schedule dependency did not return Success for the process: {proc}")

    def choose_flow():
        picks = []
        if enc_required == "TRUE":
            picks.append("Data_Encryption")
        if ing_required == "TRUE":
            picks.append("Data_Ingestion")
        return picks or ["END"]

    # ---- DAG + tasks --------------------------------------------------------------------
    with DAG(
        dag_id=dag_id,
        default_args={"owner": "airflow", "depends_on_past": False,
                      "start_date": datetime(2024, 7, 2)},
        schedule=schedule,
        max_active_runs=1,
        catchup=False,
        on_success_callback=update_success,
        tags=[scheduled_day, dag_status],
    ) as gen_dag:

        check_t = PythonOperator(
            task_id="check_dagRun", python_callable=check_dag_run,
            on_failure_callback=partial(update_failure, failure_reason="CHECK_DAGRUN_FAILED",
                                        job_name="Check_DagRun", stage_value="Pre-execution Validation"))
        sched_t = PythonOperator(
            task_id="schedule_check", python_callable=schedule_check,
            on_success_callback=insert_audit,
            on_failure_callback=partial(update_failure, failure_reason="SCHEDULE_DEPENDENCY_CHECK_FAILED",
                                        job_name="Schedule_Check", stage_value="Schedule Dependency Check"))
        extract_t = GlueJobOperator(
            task_id="ExtractCreationJob", job_name=glue_job, region_name=REGION,
            iam_role_name=GLUE_ROLE, script_args=script_arg, verbose=True,
            deferrable=USE_DEFERRABLE,
            on_success_callback=partial(glue_success, job_name=glue_job,
                                        task_id="ExtractCreationJob", what="ExtractCreationJob"),
            on_failure_callback=partial(update_failure, failure_reason="EXTRACT_GLUE_JOB_FAILED",
                                        job_name=glue_job, stage_value="Redshift to L2"))
        choose_t = BranchPythonOperator(task_id="ChooseFlow", python_callable=choose_flow)
        enc_t = EmptyOperator(task_id="Data_Encryption")
        ing_t = EmptyOperator(task_id="Data_Ingestion")
        end_t = EmptyOperator(task_id="END")

        check_t >> sched_t >> extract_t >> choose_t >> [enc_t, ing_t, end_t]

        if enc_required == "TRUE":
            for a in arguments:
                if not a.get("flagName"):
                    continue
                filename = (DagCFG.L2_OUTBOUND_STAGING_PATH + a["Outbound_folder"] + "/"
                            + a["file"].format(date_format[a["Date_Format"]]) + ".gz")
                event = {"keyname": filename, "bucketname": BUCKET,
                         "destPath": DagCFG.L2_OUTBOUND_PATH + a["Outbound_folder"] + "/",
                         "flagName": a["flagName"], "DQFlag": dq_flag}
                enc_call = TriggerDagRunOperator(
                    task_id=f"triggerEncryptionLambda_{a['task']}",
                    trigger_dag_id="CR27EncryptionLambda", conf=event,
                    wait_for_completion=True, deferrable=USE_DEFERRABLE, poke_interval=30,
                    on_failure_callback=partial(update_failure, failure_reason="EXTRACT_ENCRYPTION_FAILED",
                                                job_name="EncryptionLambda",
                                                stage_value="L2 STAGING to OUTBOUND"))
                if dq_flag == "TRUE":
                    dq_id = f"DQGeneration_{a['task']}"
                    dq_t = GlueJobOperator(
                        task_id=dq_id, job_name=DagCFG.DQ_GLUE_JOB, region_name=REGION,
                        iam_role_name=GLUE_ROLE, verbose=True, deferrable=USE_DEFERRABLE,
                        script_args={"--DQfileName": a["flagName"], "--input_file_path": filename},
                        # partial() binds the CURRENT loop values (old lambdas all saw the last one)
                        on_success_callback=partial(glue_success, job_name=DagCFG.DQ_GLUE_JOB,
                                                    task_id=dq_id, what=dq_id,
                                                    extra=f" for the extract file {filename}"),
                        on_failure_callback=partial(update_failure, failure_reason="DQ_GLUE_JOB_FAILED",
                                                    job_name=DagCFG.DQ_GLUE_JOB, stage_value="Redshift to L2"))
                    enc_t >> dq_t >> enc_call
                else:
                    enc_t >> enc_call

        if ing_required == "TRUE":
            for a in arguments:
                if not a.get("Ingestion_folder"):
                    continue
                ing_call = TriggerDagRunOperator(
                    task_id=f"triggerIngestionGlue_{a['task']}",
                    trigger_dag_id="CR27IngestionJob",
                    conf={"--source_path": "/" + DagCFG.L2_OUTBOUND_STAGING_PATH + a["Ingestion_folder"],
                          "--schema": a["Schema"], "--table_name": a["TableName"]},
                    wait_for_completion=True, deferrable=USE_DEFERRABLE, poke_interval=30,
                    on_failure_callback=partial(update_failure, failure_reason="INGESTION_GLUE_JOB_FAILED",
                                                job_name="DataIngestionJob", stage_value="L2 to Redshift"))
                ing_t >> ing_call
    return gen_dag


# ----------------------------------------------------------------------------------------
# Config validation + registration (never raises)
# ----------------------------------------------------------------------------------------
def _parse_entry(raw):
    e = raw[0] if isinstance(raw, list) else raw
    missing = [k for k in REQUIRED_KEYS if k not in e]
    if missing:
        raise KeyError(f"missing keys {missing}")
    merge_off = e.get("disbale_merge_flag", "FALSE") == "TRUE"
    return {
        "dag_id": e["dag_id"], "GlueJob": e["GlueJob"], "scheduled_day": e["scheduled_day"],
        "Encryption_Required": e["Encryption_Required"], "Ingestion_Required": e["Ingestion_Required"],
        "Arguments": e["Arguments"], "script_arg": e.get("Script_Argument", {}),
        "QE_flag": e.get("QE_Flag", "False"), "DQFlag": e.get("DQFlag", "FALSE"),
        "process_details": e["NON_MERGE_PROCESS_NAME"] if merge_off else e["PROCESS_NAME"],
    }


def Dynamic_dag():
    config = _cached_s3_json(CONFIG_KEY, CONFIG_TTL_SEC)
    if not isinstance(config, dict):
        log.error("Extract config unavailable - no extract DAGs registered this parse cycle")
        return
    snap = _cached_s3_json(SNAPSHOT_KEY, SNAPSHOT_TTL_SEC) or {}
    for name, raw in config.items():
        try:
            params = _parse_entry(raw)
            if params["dag_id"] in globals():
                raise ValueError(f"duplicate dag_id {params['dag_id']}")
            globals()[params["dag_id"]] = create_dag(params, snap)
        except Exception as ex:                                # one bad entry != 60 broken DAGs
            log.error("Skipping config entry %s: %s", name, ex)


Dynamic_dag()


# ----------------------------------------------------------------------------------------
# Snapshot refresher: the ONLY place that talks to Redshift for calendar / status data.
# One tiny task every 30 min replaces ~(60 dags x 3-4 queries) on every scheduler parse.
# ----------------------------------------------------------------------------------------
def refresh_snapshot():
    config = _cached_s3_json(CONFIG_KEY, 0)
    live = {}

    def live_get(key):
        if key not in live:
            live[key] = Dag_Util.readMRConfig(key)
        return live[key]

    snap = {"generated_at": datetime.now(TZ).isoformat(),
            "curr_period": live_get("curr_period"),
            "curr_end_date": live_get("curr_end_date"),
            "bd_dates": {}, "dag_status": {}}
    for name, raw in config.items():
        try:
            p = _parse_entry(raw)
            period, me = _resolve_period(p["QE_flag"], live_get)
            day_key = f"{period}|{p['scheduled_day']}"
            if day_key not in snap["bd_dates"]:                # ~6 distinct values, not 60
                snap["bd_dates"][day_key] = Dag_Util.readBdConfig(period, p["scheduled_day"])
            snap["dag_status"][p["dag_id"]] = Dag_Util.read_AuditBatchRunStatus(
                f"{p['scheduled_day']}_{me}", _process_name(p["dag_id"]))
        except Exception as ex:
            log.error("Snapshot skipped %s: %s", name, ex)
    _client("s3").put_object(Bucket=BUCKET, Key=SNAPSHOT_KEY,
                             Body=json.dumps(snap).encode("utf-8"), ContentType="application/json")
    log.info("Snapshot written: %d dags, %d bd dates", len(snap["dag_status"]), len(snap["bd_dates"]))


with DAG(dag_id="dag_snapshot_refresh",
         default_args={"owner": "airflow", "start_date": datetime(2024, 7, 2),
                       "retries": 2, "retry_delay": timedelta(minutes=2)},
         schedule="*/30 * * * *", catchup=False, max_active_runs=1,
         tags=["infra", "snapshot"]) as snapshot_dag:
    PythonOperator(task_id="refresh_snapshot", python_callable=refresh_snapshot)

import json
from datetime import datetime

# Path to your JSON config file
ConfigFilePath = "Airflow_ProcessCompletionNotifier.json"

def print_conf_params(**context):
    dag_run_conf = context.get("dag_run").conf or {}
    print("Received Config Params")
    print(json.dumps(dag_run_conf, indent=2))

    # Load the JSON config mapping
    with open(ConfigFilePath, "r") as f:
        config_data = json.load(f)

    # Current time & audit id
    current_time = datetime.now()
    process_com_dt = current_time.isoformat()
    audit_id = current_time.strftime("%Y%m%d%H%M%S")

    # Default values
    process_name = "UNKNOWN_PROCESS"
    event_id = "FBPA_PROCESS_COMPLETE"

    # Check for table_name
    table_name = dag_run_conf.get("table_name")
    if table_name:
        if table_name in config_data:
            # ✅ Found matching entry in JSON
            process_name = config_data[table_name]["ProcessName"]

            # KYVOS prefix → update event name
            if table_name.upper().startswith("KYVOS_"):
                event_id = "FBPA_KYVOS_PROCESS_COMPLETE"
        else:
            # ❌ No match in config → IGNORE this trigger
            print(f"⚠️ Ignoring trigger: table_name '{table_name}' not found in config file.")
            return None

    # Build message only if valid process_name found
    message = {
        "event_id": "693217fc-3ce2-4b12-b70e-148504e0f7b7",
        "event": event_id,
        "process_name": process_name,
        "data_date": dag_run_conf.get("data_date", current_time.strftime("%Y%m%d")),
        "process_com_dt": process_com_dt,
        "audit_id": audit_id
    }

    print("Generated Message:")
    print(json.dumps(message, indent=2))
    return message

import json

# Read Airflow config
with open("AirflowExtractConfig.json", "r") as f:
    airflow_data = json.load(f)

# Read Standalone config
with open("standaloneConfig.txt", "r") as f:
    standalone_data = json.load(f)

# Build mapping: dag_id -> flagName
flag_mapping = {}

for dag_list in airflow_data.values():
    for dag in dag_list:
        dag_id = dag.get("dag_id")

        for arg in dag.get("Arguments", []):
            flag_name = arg.get("flagName")

            if dag_id and flag_name:
                flag_mapping[dag_id] = flag_name

# Update standalone config
for extract in standalone_data.get("extracts", []):
    table_name = extract.get("tableName")

    if table_name in flag_mapping:
        for output in extract.get("Output", []):
            output["flagName"] = flag_mapping[table_name]

# Write back
with open("standaloneConfig.txt", "w") as f:
    json.dump(standalone_data, f, indent=4)

print("Standalone config updated successfully")

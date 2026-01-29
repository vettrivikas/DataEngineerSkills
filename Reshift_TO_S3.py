import json
import csv

INPUT_JSON = "AirflowExtractConfig.json"
OUTPUT_CSV = "AirflowExtractConfig.csv"

rows = []
headers = set()

def flatten_json(obj, parent_key="", row=None):
    if row is None:
        row = {}

    if isinstance(obj, dict):
        for k, v in obj.items():
            flatten_json(v, f"{parent_key}{k}_", row)

    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            flatten_json(item, f"{parent_key}{i}_", row)

    else:
        key = parent_key.rstrip("_")
        row[key] = obj
        headers.add(key)

    return row

# Load JSON
with open(INPUT_JSON, "r") as f:
    data = json.load(f)

# Iterate DAGs
for dag_group, dag_list in data.items():
    for dag in dag_list:
        row = {"dag_group": dag_group}
        headers.add("dag_group")
        flatten_json(dag, "", row)
        rows.append(row)

# Write CSV
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=sorted(headers))
    writer.writeheader()
    writer.writerows(rows)

print("✅ CSV file created:", OUTPUT_CSV)

import json

# Load JSON data
with open("your_file.json", "r") as f:
    data = json.load(f)

# Find DAGs without 'check' key
dags_missing_check = []

for dag_name, dag_list in data.items():
    # If the list is empty or all entries are missing 'check'
    if not dag_list or all("check" not in entry for entry in dag_list):
        dags_missing_check.append(dag_name)

print(dags_missing_check)

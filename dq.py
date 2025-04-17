import json

# Read your JSON file
with open("your_file.json", "r") as f:
    data = json.load(f)

# Store DAGs missing the 'check' key
dags_missing_check = []

for dag_name, dag_list in data.items():
    if not dag_list:  # Empty list, so 'check' key is definitely missing
        dags_missing_check.append(dag_name)
    else:
        for dag_entry in dag_list:
            if "check" not in dag_entry:
                dags_missing_check.append(dag_name)
                break  # No need to check further entries in this DAG

print("DAGs missing 'check' key:", dags_missing_check)

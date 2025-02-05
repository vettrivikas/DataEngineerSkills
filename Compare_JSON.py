import json


def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)


def compare_json(pre_prod, preprd):
    missing_keys = []
    mismatched_values = {}

    for key in pre_prod:
        if key not in preprd:
            missing_keys.append(key)
        elif pre_prod[key] != preprd[key]:
            mismatched_values[key] = {'pre_prod': pre_prod[key], 'preprd': preprd[key]}

    return missing_keys, mismatched_values


# Load JSON files
pre_prod_json = load_json("pre_prod.json")
preprd_json = load_json("preprd.json")

# Compare
missing_keys, mismatched_values = compare_json(pre_prod_json, preprd_json)

# Print results
if missing_keys:
    print("Missing keys in preprd.json:", missing_keys)
else:
    print("No missing keys.")

if mismatched_values:
    print("Mismatched values:")
    for key, values in mismatched_values.items():
        print(f"  {key}: pre_prod -> {values['pre_prod']}, preprd -> {values['preprd']}")
else:
    print("No value mismatches.")

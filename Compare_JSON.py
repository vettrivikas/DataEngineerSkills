import json

def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

def merge_json(preprd, pre_prod):
    # Start with preprd.json data
    prod_new = preprd.copy()

    # Add missing keys from pre_prod.json
    for key, value in pre_prod.items():
        if key not in prod_new:
            prod_new[key] = value
    
    return prod_new

# Load JSON files
preprd_json = load_json("preprd.json")
pre_prod_json = load_json("pre_prod.json")

# Merge JSON
prod_new_json = merge_json(preprd_json, pre_prod_json)

# Save to prod_new.json
with open("prod_new.json", "w") as f:
    json.dump(prod_new_json, f, indent=4)

print("prod_new.json has been created successfully!")



# Python script to compare two TXT files (CSV-style rows with quotes).
# Show which rows are misplaced (same row exists in both files but different line numbers).
# Do not report mismatched rows, only misplaced.


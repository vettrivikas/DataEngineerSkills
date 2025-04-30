import json

# Load dev and prod JSON files
with open("dev.json") as f:
    dev_data = json.load(f)

with open("prod.json") as f:
    prod_data = json.load(f)

def normalize(gwf_name):
    return gwf_name.replace("dev2", "").replace("BPA-ANAPLAN", "BPA-")

# Extract and normalize GWF_NAMEs
dev_gwf_names = {normalize(item["details"]["GWF_NAME"]) for item in dev_data}
prod_gwf_names = {normalize(item["details"]["GWF_NAME"]) for item in prod_data}

# Find differences
missing_in_prod = dev_gwf_names - prod_gwf_names
missing_in_dev = prod_gwf_names - dev_gwf_names

print("Missing in prod:", missing_in_prod)
print("Missing in dev:", missing_in_dev)

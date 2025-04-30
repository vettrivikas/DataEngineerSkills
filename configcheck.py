import re
import json

def fix_json(text):
    # Add double quotes around keys
    text = re.sub(r'([{,]\s*)(\w+)(\s*:)','\\1"\\2"\\3', text)
    return text

# Load and fix malformed JSON from .txt
with open("dev.txt") as f:
    dev_raw = f.read()
with open("prod.txt") as f:
    prod_raw = f.read()

dev_fixed = fix_json(dev_raw)
prod_fixed = fix_json(prod_raw)

dev_data = json.loads(dev_fixed)
prod_data = json.loads(prod_fixed)

# Normalize and compare
def normalize(name):
    return name.replace("dev2", "").replace("BPA-ANAPLAN", "BPA-")

dev_gwf = {item["tableName"]: normalize(item["details"]["GWF_NAME"]) for item in dev_data}
prod_gwf = {item["tableName"]: normalize(item["details"]["GWF_NAME"]) for item in prod_data}

missing_in_prod = {k: v for k, v in dev_gwf.items() if k not in prod_gwf}
missing_in_dev = {k: v for k, v in prod_gwf.items() if k not in dev_gwf}
value_mismatches = {k: (dev_gwf[k], prod_gwf[k]) for k in dev_gwf if k in prod_gwf and dev_gwf[k] != prod_gwf[k]}

print("Missing in prod:", missing_in_prod)
print("Missing in dev:", missing_in_dev)
print("Mismatched values:", value_mismatches)

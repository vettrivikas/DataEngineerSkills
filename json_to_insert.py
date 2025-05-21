import json

# Input and output file paths
json_file_path = 'data.json'         # Your input JSON file
sql_file_path = 'insert_data.sql'    # Output .sql file
table_name = 'your_table'            # Target table name

# Load JSON data
with open(json_file_path, 'r') as f:
    records = json.load(f)

# Check if data is valid
if not records:
    raise ValueError("No data found in JSON.")

# Prepare column names
columns = list(records[0].keys())
column_str = ', '.join(columns)

# Build SQL values
values_list = []
for record in records:
    row = []
    for col in columns:
        val = record[col]
        if val is None:
            row.append("NULL")
        elif isinstance(val, str):
            val = val.replace("'", "''")  # Escape single quotes
            row.append(f"'{val}'")
        else:
            row.append(str(val))
    values_list.append(f"({', '.join(row)})")

# Join all values into a single query
values_str = ',\n  '.join(values_list)
insert_query = f"INSERT INTO {table_name} ({column_str})\nVALUES\n  {values_str};"

# Save to SQL file
with open(sql_file_path, 'w') as f:
    f.write(insert_query)

print(f"SQL INSERT statement saved to '{sql_file_path}'")

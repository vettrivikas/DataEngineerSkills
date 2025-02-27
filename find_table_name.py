import re

# Define regex pattern to capture schema.table or just table names
pattern = re.compile(r"(?i)(?:from|into|update|join|table)\s+([a-zA-Z_][\w]*(?:\.[a-zA-Z_][\w]*)?)")

# File path containing SQL statements
file_path = "path/to/your/sql_script.sql"  # Change this to your actual file location

# Read the SQL script file and extract table names
with open(file_path, "r") as file:
    sql_script = file.read()

# Find all table names
tables = pattern.findall(sql_script)

# Print extracted table names
print("Extracted Table Names:")
for table in tables:
    print(table)

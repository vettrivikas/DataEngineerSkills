import re
import csv

data = """

"""

# Extract tuples using regex
pattern = r"\('([^']*)','([^']*)','([^']*)','([^']*)',current_date\)"
rows = re.findall(pattern, data)

# Write to CSV file
with open("output.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["DB", "Table_Name", "Start_Time", "End_Time"])
    writer.writerows(rows)

print("✅ Data successfully exported to output.csv")

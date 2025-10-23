import re
import csv

data = """

"""

# Extract tuples using regex
pattern = r"\('([^']*)','([^']*)','([^']*)','([^']*)',current_date\)"
rows = re.findall(pattern, data)

def convert_to_12hr(time_str):
    h, m, s = map(int, time_str.split(":"))
    suffix = "AM" if h < 12 else "PM"
    h = h % 12 or 12
    return f"{h}{suffix}"

# Write to CSV
with open("output.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["DB", "Table_Name", "Start_Time", "End_Time", "Summary"])
    
    for db, tbl, start, end in rows:
        start12 = convert_to_12hr(start)
        end12 = convert_to_12hr(end)
        summary = f"{db} - {start12} to {end12}"
        writer.writerow([db, tbl, start, end, summary])

print("✅ Data successfully exported to output.csv")

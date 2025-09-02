import boto3
import redshift_connector
import csv
from io import StringIO

# S3 and Redshift configs
s3_bucket = 'your-s3-bucket'
s3_key = 'your-folder/exported_data_pipe_delimited.csv'
redshift_config = {
    'host': 'your-redshift-cluster.amazonaws.com',
    'database': 'your_db',
    'user': 'your_user',
    'password': 'your_password',
    'port': 5439,
    'table': 'your_schema.your_table'
}

# Step 1: Read CSV from S3
s3 = boto3.client('s3')
response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
csv_content = response['Body'].read().decode('utf-8')

# Step 2: Parse CSV
csv_buffer = StringIO(csv_content)
reader = csv.reader(csv_buffer, delimiter='|')
header = next(reader)  # first row = column names

# Step 3: Prepare insert statements
rows = []
for row in reader:
    formatted = []
    for value in row:
        if value == '':
            formatted.append("NULL")
        else:
            value = value.replace("'", "''")  # escape single quotes
            formatted.append(f"'{value}'")
    rows.append(f"({', '.join(formatted)})")

# Chunk insert into batches (optional)
insert_query = f"""
    INSERT INTO {redshift_config['table']} ({', '.join(header)})
    VALUES {',\n'.join(rows)};
"""

# Step 4: Load into Redshift
conn = redshift_connector.connect(
    host=redshift_config['host'],
    database=redshift_config['database'],
    user=redshift_config['user'],
    password=redshift_config['password'],
    port=redshift_config['port']
)
cursor = conn.cursor()
cursor.execute(insert_query)
conn.commit()
conn.close()

print("✅ Data loaded into Redshift successfully.")
It looks like we are not using the data_date from the source table. Instead, we’re applying a static month rollover date to all dataframes fetched from the source. Because of this, the filter applied on the IFRS table is unnecessary—since, as per the current code logic and mapping, we are already treating all data as belonging to the current month’s rollover end date.
#redshift_engine = create_engine("postgresql+psycopg2://%s:%s@%s:%d/%s" % (redshift_user,redshift_password,redshift_host,redshift_port,redshift_db))

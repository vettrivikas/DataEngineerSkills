import redshift_connector
import pandas as pd
import boto3
from io import StringIO

# Step 1: Connect to Redshift
conn = redshift_connector.connect(
    host='your-redshift-cluster.amazonaws.com',
    database='your_db',
    user='your_user',
    password='your_password',
    port=5439
)

query = "SELECT * FROM your_schema.your_table"
df = pd.read_sql(query, conn)
conn.close()

# Step 2: Convert DataFrame to CSV
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

# Step 3: Upload CSV to S3
s3 = boto3.client('s3')
s3.put_object(
    Bucket='your-s3-bucket',
    Key='your-folder/table_export.csv',
    Body=csv_buffer.getvalue()
)

print("✅ Table exported from Redshift and uploaded to S3.")

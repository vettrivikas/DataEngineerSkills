"""
Enhanced Lambda handler for CDK deployment with Secrets Manager integration
"""
import os
import json
import boto3
from botocore.exceptions import ClientError

def get_secret(secret_arn):
    """Retrieve secret from AWS Secrets Manager"""
    try:
        client = boto3.client('secretsmanager')
        response = client.get_secret_value(SecretId=secret_arn)
        return json.loads(response['SecretString'])
    except ClientError as e:
        print(f"Error retrieving secret {secret_arn}: {e}")
        return None

def handler(event, context):
    """
    Lambda handler with Secrets Manager integration for CDK deployment
    """
    # Get database credentials from Secrets Manager
    db_secret_arn = os.environ.get('DB_CREDENTIALS_SECRET')
    session_secret_arn = os.environ.get('SESSION_SECRET_ARN')
    
    if db_secret_arn:
        db_credentials = get_secret(db_secret_arn)
        if db_credentials:
            os.environ['PGUSER'] = db_credentials.get('username', 'admin')
            os.environ['PGPASSWORD'] = db_credentials.get('password', '')
    
    if session_secret_arn:
        session_secret = get_secret(session_secret_arn)
        if session_secret:
            os.environ['SESSION_SECRET'] = session_secret
    
    # Import the main application after setting environment variables
    try:
        from serverless_wsgi import handle_request
        from main import app
        return handle_request(app, event, context)
    except ImportError:
        # Fallback for environments without serverless_wsgi
        from main import app
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Data Quality Dashboard Lambda function is running',
                'version': '1.0.0'
            })
        }
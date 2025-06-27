"""
WSGI handler for AWS Lambda serverless deployment
"""
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(__file__))

try:
    from serverless_wsgi import handle_request
    from main import app
    
    def handler(event, context):
        return handle_request(app, event, context)
        
except ImportError:
    # Fallback for local testing or different serverless setup
    from main import app
    
    def handler(event, context):
        # Simple handler for basic Lambda integration
        return {
            'statusCode': 200,
            'body': 'Serverless WSGI handler loaded successfully'
        }
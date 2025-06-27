# Deployment Guide - Data Quality Dashboard

This guide covers deploying the Data Quality Dashboard in different environments including local development, cloud servers, and AWS serverless infrastructure.

## Table of Contents

- [Local Development Setup](#local-development-setup)
- [Production Server Deployment](#production-server-deployment)
- [AWS Serverless Deployment](#aws-serverless-deployment)
- [Docker Deployment](#docker-deployment)
- [Environment Configuration](#environment-configuration)

## Local Development Setup

### 1. Prerequisites
```bash
# Install Python 3.11+
python --version

# Install Git
git --version
```

### 2. Project Setup
```bash
# Clone the repository
git clone <your-repo-url>
cd data-quality-dashboard

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Database Setup
```bash
# For PostgreSQL (local testing)
sudo apt-get install postgresql postgresql-contrib
sudo -u postgres createdb data_quality_db

# For Amazon Redshift (production)
# Ensure your Redshift cluster is accessible
```

### 4. Environment Configuration
```bash
# Copy environment template
cp .env.example .env

# Edit .env file with your credentials
nano .env
```

Required variables:
```bash
PGHOST=localhost                    # Your database host
PGPORT=5432                        # Database port
PGDATABASE=data_quality_db         # Database name
PGUSER=your_username               # Database user
PGPASSWORD=your_password           # Database password
SESSION_SECRET=your-random-secret-key
FLASK_DEBUG=True
LOG_LEVEL=DEBUG
```

### 5. Run Application
```bash
# Development server
python main.py

# Or with Gunicorn (production-like)
gunicorn --bind 0.0.0.0:5000 --reload main:app
```

Access at: `http://localhost:5000`

## Production Server Deployment

### Ubuntu/CentOS Server Setup

### 1. Server Preparation
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Python 3.11
sudo apt install python3.11 python3.11-venv python3.11-dev

# Install system dependencies
sudo apt install postgresql-client git nginx supervisor
```

### 2. Application Deployment
```bash
# Create application directory
sudo mkdir -p /opt/data-quality-dashboard
sudo chown $USER:$USER /opt/data-quality-dashboard

# Clone application
cd /opt/data-quality-dashboard
git clone <your-repo-url> .

# Setup Python environment
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Environment Configuration
```bash
# Create production environment file
sudo nano /opt/data-quality-dashboard/.env
```

Production environment:
```bash
PGHOST=your-redshift-cluster.region.redshift.amazonaws.com
PGPORT=5439
PGDATABASE=your_production_db
PGUSER=your_redshift_user
PGPASSWORD=your_secure_password
SESSION_SECRET=your-super-secure-session-key
FLASK_DEBUG=False
LOG_LEVEL=INFO
```

### 4. Gunicorn Configuration
```bash
# Create Gunicorn configuration
sudo nano /opt/data-quality-dashboard/gunicorn.conf.py
```

```python
# gunicorn.conf.py
bind = "0.0.0.0:5000"
workers = 4
worker_class = "sync"
worker_connections = 1000
timeout = 30
keepalive = 2
max_requests = 1000
max_requests_jitter = 100
preload_app = True
```

### 5. Supervisor Configuration
```bash
# Create supervisor configuration
sudo nano /etc/supervisor/conf.d/data-quality-dashboard.conf
```

```ini
[program:data-quality-dashboard]
command=/opt/data-quality-dashboard/venv/bin/gunicorn --config /opt/data-quality-dashboard/gunicorn.conf.py main:app
directory=/opt/data-quality-dashboard
user=www-data
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/data-quality-dashboard.log
```

### 6. Nginx Configuration
```bash
# Create Nginx site configuration
sudo nano /etc/nginx/sites-available/data-quality-dashboard
```

```nginx
server {
    listen 80;
    server_name your-domain.com;
    
    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
    
    location /static/ {
        alias /opt/data-quality-dashboard/static/;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}
```

### 7. Enable and Start Services
```bash
# Enable Nginx site
sudo ln -s /etc/nginx/sites-available/data-quality-dashboard /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx

# Start Supervisor
sudo supervisorctl reread
sudo supervisorctl update
sudo supervisorctl start data-quality-dashboard
```

## AWS Serverless Deployment

### Using AWS Lambda + API Gateway + RDS

### 1. Prerequisites
```bash
# Install AWS CLI
pip install awscli

# Install Serverless Framework
npm install -g serverless

# Install Serverless plugins
npm install serverless-wsgi serverless-python-requirements
```

### 2. Project Structure for Serverless
Create `serverless.yml`:
```yaml
service: data-quality-dashboard

provider:
  name: aws
  runtime: python3.11
  region: us-east-1
  stage: ${opt:stage, 'dev'}
  
  environment:
    PGHOST: ${env:PGHOST}
    PGPORT: ${env:PGPORT}
    PGDATABASE: ${env:PGDATABASE}
    PGUSER: ${env:PGUSER}
    PGPASSWORD: ${env:PGPASSWORD}
    SESSION_SECRET: ${env:SESSION_SECRET}
    
  iamRoleStatements:
    - Effect: Allow
      Action:
        - rds:DescribeDBInstances
        - logs:CreateLogGroup
        - logs:CreateLogStream
        - logs:PutLogEvents
      Resource: "*"

functions:
  app:
    handler: wsgi_handler.handler
    events:
      - http: ANY /
      - http: 'ANY {proxy+}'
    timeout: 30
    memorySize: 512

plugins:
  - serverless-wsgi
  - serverless-python-requirements

custom:
  wsgi:
    app: main.app
    packRequirements: false
  pythonRequirements:
    dockerizePip: non-linux
    slim: true
```

### 3. WSGI Handler
Create `wsgi_handler.py`:
```python
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(__file__))

from main import app

def handler(event, context):
    return app(event, context)
```

### 4. Deployment Commands
```bash
# Set environment variables
export PGHOST=your-rds-endpoint.region.rds.amazonaws.com
export PGPORT=5432
export PGDATABASE=your_database
export PGUSER=your_username
export PGPASSWORD=your_password
export SESSION_SECRET=your-session-secret

# Deploy to AWS
serverless deploy --stage production

# View logs
serverless logs -f app --stage production
```

### 5. RDS Setup for Serverless
```bash
# Create RDS instance
aws rds create-db-instance \
    --db-instance-identifier data-quality-db \
    --db-instance-class db.t3.micro \
    --engine postgres \
    --master-username admin \
    --master-user-password YourSecurePassword \
    --allocated-storage 20 \
    --vpc-security-group-ids sg-xxxxxxxx \
    --publicly-accessible
```

## Docker Deployment

### 1. Create Dockerfile
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

# Expose port
EXPOSE 5000

# Run application
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "main:app"]
```

### 2. Create docker-compose.yml
```yaml
version: '3.8'

services:
  app:
    build: .
    ports:
      - "5000:5000"
    environment:
      - PGHOST=db
      - PGPORT=5432
      - PGDATABASE=data_quality
      - PGUSER=postgres
      - PGPASSWORD=password
      - SESSION_SECRET=your-secret-key
    depends_on:
      - db
    volumes:
      - ./logs:/app/logs

  db:
    image: postgres:15
    environment:
      - POSTGRES_DB=data_quality
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

volumes:
  postgres_data:
```

### 3. Build and Run
```bash
# Build and start containers
docker-compose up --build

# Run in background
docker-compose up -d

# View logs
docker-compose logs -f app

# Stop containers
docker-compose down
```

## Environment Configuration

### Development Environment
```bash
PGHOST=localhost
PGPORT=5432
PGDATABASE=data_quality_dev
PGUSER=developer
PGPASSWORD=dev_password
SESSION_SECRET=dev-secret-key
FLASK_DEBUG=True
LOG_LEVEL=DEBUG
```

### Production Environment
```bash
PGHOST=your-redshift-cluster.region.redshift.amazonaws.com
PGPORT=5439
PGDATABASE=production_db
PGUSER=prod_user
PGPASSWORD=secure_production_password
SESSION_SECRET=super-secure-random-key-256-bits
FLASK_DEBUG=False
LOG_LEVEL=INFO
```

### Security Best Practices

1. **Secrets Management**
   - Use AWS Secrets Manager for production
   - Never commit secrets to version control
   - Rotate credentials regularly

2. **Database Security**
   - Use SSL connections to database
   - Implement least-privilege access
   - Enable database audit logging

3. **Application Security**
   - Use HTTPS in production
   - Implement proper session management
   - Regular security updates

### Monitoring and Logging

```bash
# CloudWatch for AWS deployments
# Set up log groups and metrics

# For server deployments
sudo apt install logrotate
# Configure log rotation for application logs
```

## Troubleshooting

### Common Issues

1. **Database Connection Timeout**
   - Check security groups and network access
   - Verify credentials and connection string
   - Test connectivity from deployment environment

2. **Memory Issues in Lambda**
   - Increase memory allocation
   - Optimize imports and dependencies
   - Consider using provisioned concurrency

3. **Static Files Not Loading**
   - Configure proper static file serving
   - Check file permissions
   - Verify URL routing for static assets

### Health Checks

Add a health check endpoint to your application:
```python
@app.route('/health')
def health_check():
    return {'status': 'healthy', 'timestamp': datetime.now().isoformat()}
```

This deployment guide provides multiple options for hosting your Data Quality Dashboard, from simple local development to scalable cloud deployments.
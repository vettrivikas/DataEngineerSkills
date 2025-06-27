# Data Quality Dashboard - Bank of Canada Regulatory Compliance

A Flask-based web application for monitoring and reporting data quality metrics for Amazon Redshift databases, specifically designed for regulatory compliance requirements.

## Features

- **Database Connection**: Connects to Amazon Redshift or PostgreSQL databases
- **Schema Explorer**: Browse and select schemas and tables dynamically
- **Data Quality Metrics**: Five key dimensions of data quality assessment:
  - Completeness: Percentage of non-null values
  - Timeliness: Presence of recent records
  - Accuracy: Values within expected ranges
  - Consistency: Cross-field validation (e.g., currency codes)
  - Uniqueness: Duplicate detection in key columns
- **Critical Data Elements (CDE)**: Special focus on regulatory-critical fields
- **Interactive Dashboard**: Charts, KPIs, and detailed metrics tables
- **Responsive Design**: Bootstrap-based UI with dark theme

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL or Amazon Redshift database
- Environment variables for database connection

### Local Setup

1. **Clone and Install Dependencies**
```bash
git clone <repository-url>
cd data-quality-dashboard
pip install -r requirements.txt
```

2. **Configure Environment Variables**
```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your database credentials
PGHOST=your-database-host
PGPORT=5432
PGDATABASE=your_database
PGUSER=your_username
PGPASSWORD=your_password
SESSION_SECRET=your-secret-key-here
```

3. **Run the Application**
```bash
# Development mode
python main.py

# Production mode with Gunicorn
gunicorn --bind 0.0.0.0:5000 --reuse-port --reload main:app
```

4. **Access the Dashboard**
Open your browser to `http://localhost:5000`

## Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `PGHOST` | Database hostname | `your-cluster.region.redshift.amazonaws.com` |
| `PGPORT` | Database port | `5439` (Redshift) or `5432` (PostgreSQL) |
| `PGDATABASE` | Database name | `your_database` |
| `PGUSER` | Database username | `your_username` |
| `PGPASSWORD` | Database password | `your_password` |
| `SESSION_SECRET` | Flask session secret | `your-secure-random-key` |
| `FLASK_DEBUG` | Debug mode | `True` or `False` |
| `LOG_LEVEL` | Logging level | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

### Database Requirements

The application requires read access to:
- `information_schema.tables` - For schema and table discovery
- `information_schema.columns` - For column metadata
- Target tables - For data quality analysis

### Critical Data Elements (CDE)

Pre-configured for Bank of Canada regulatory requirements:
- `customer_id` - Customer identifiers
- `credit_score` - Credit rating scores
- `transaction_amount` - Financial transaction values
- `regulatory_flag` - Compliance indicators
- `account_status` - Account state information

## Architecture

```
├── app.py                 # Flask application factory
├── main.py               # Application entry point
├── config.py             # Configuration management
├── models.py             # Data models and schemas
├── routes/
│   └── dashboard.py      # Dashboard routes and API endpoints
├── services/
│   ├── database.py       # Database connection and operations
│   └── data_quality.py   # Data quality metrics calculation
├── templates/
│   ├── base.html         # Base template
│   └── dashboard.html    # Main dashboard template
└── static/
    ├── css/custom.css    # Custom styles
    └── js/dashboard.js   # Frontend JavaScript
```

## Data Quality Metrics

### Completeness
- Measures percentage of non-null values in each column
- Critical for regulatory reporting accuracy

### Timeliness
- Checks for recent records (last 30 days by default)
- Identifies stale or outdated data

### Accuracy
- Validates numerical values against expected ranges
- Supports custom validation rules per field type

### Consistency
- Cross-field validation (e.g., currency codes)
- Ensures data follows business rules

### Uniqueness
- Detects duplicate values in key columns
- Essential for data integrity

## Scoring System

- **Green (≥70%)**: Good data quality
- **Yellow (40-70%)**: Moderate quality, needs attention
- **Red (<40%)**: Poor quality, immediate action required

## API Endpoints

- `GET /` - Main dashboard
- `GET /api/tables/{schema}` - Get tables for schema
- `GET /api/data-quality/{schema}/{table}` - Get quality metrics
- `GET /quality-report/{schema}/{table}` - Detailed quality report

## Security Considerations

- Use environment variables for sensitive credentials
- Enable HTTPS in production
- Implement proper access controls for database connections
- Regular security updates for dependencies

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Verify credentials in environment variables
   - Check network connectivity and firewall rules
   - Ensure database accepts connections from your IP

2. **No Schemas Found**
   - Verify user has read permissions on `information_schema`
   - Check if schemas contain accessible tables

3. **Quality Metrics Not Calculating**
   - Ensure user has SELECT permissions on target tables
   - Check for data type compatibility issues

### Logs

Application logs include:
- Database connection status
- Query execution details
- Error messages with stack traces
- Performance metrics

## AWS Deployment Guide

### Prerequisites for AWS Deployment

1. **AWS Account Setup**
   - Active AWS account with appropriate permissions
   - AWS CLI installed and configured
   - IAM user with Lambda, API Gateway, and RDS permissions

2. **Install Required Tools**
   ```bash
   # Install AWS CLI
   pip install awscli
   
   # Configure AWS credentials
   aws configure
   # Enter: Access Key ID, Secret Access Key, Region (e.g., us-east-1), Output format (json)
   
   # Install Serverless Framework
   npm install -g serverless
   
   # Install project dependencies
   npm install serverless-wsgi serverless-python-requirements
   ```

### RDS Database Setup

1. **Create RDS PostgreSQL Instance**
   ```bash
   # Using AWS CLI
   aws rds create-db-instance \
       --db-instance-identifier data-quality-dashboard-db \
       --db-instance-class db.t3.micro \
       --engine postgres \
       --engine-version 15.4 \
       --master-username admin \
       --master-user-password YourSecurePassword123 \
       --allocated-storage 20 \
       --storage-type gp2 \
       --vpc-security-group-ids sg-xxxxxxxxx \
       --db-subnet-group-name default \
       --publicly-accessible \
       --backup-retention-period 7 \
       --storage-encrypted
   ```

2. **Configure Security Groups**
   ```bash
   # Create security group for RDS
   aws ec2 create-security-group \
       --group-name data-quality-db-sg \
       --description "Security group for Data Quality Dashboard RDS"
   
   # Allow PostgreSQL access (port 5432)
   aws ec2 authorize-security-group-ingress \
       --group-name data-quality-db-sg \
       --protocol tcp \
       --port 5432 \
       --cidr 0.0.0.0/0
   ```

3. **Get RDS Endpoint**
   ```bash
   # Get the RDS endpoint URL
   aws rds describe-db-instances \
       --db-instance-identifier data-quality-dashboard-db \
       --query 'DBInstances[0].Endpoint.Address' \
       --output text
   ```

### Lambda Deployment Steps

1. **Set Environment Variables**
   ```bash
   # Replace with your actual RDS details
   export PGHOST=data-quality-dashboard-db.xxxxx.us-east-1.rds.amazonaws.com
   export PGPORT=5432
   export PGDATABASE=postgres
   export PGUSER=admin
   export PGPASSWORD=YourSecurePassword123
   export SESSION_SECRET=$(openssl rand -base64 32)
   ```

2. **Deploy to AWS Lambda**
   ```bash
   # Deploy to development environment
   serverless deploy --stage dev
   
   # Deploy to production environment
   serverless deploy --stage production
   
   # View deployment information
   serverless info --stage production
   ```

3. **Initialize Database Schema**
   ```bash
   # Connect to RDS and run initialization script
   psql -h $PGHOST -U $PGUSER -d $PGDATABASE -f init-db.sql
   ```

### AWS Cost Optimization

- **Lambda**: Pay per request (first 1M requests free monthly)
- **API Gateway**: ~$3.50 per million API calls
- **RDS t3.micro**: ~$13/month (free tier eligible for 12 months)
- **Data Transfer**: Minimal for typical dashboard usage

### Monitoring and Logging

1. **CloudWatch Integration**
   ```bash
   # View Lambda logs
   serverless logs -f app --stage production
   
   # Follow logs in real-time
   serverless logs -f app --stage production --tail
   ```

2. **Set Up CloudWatch Alarms**
   ```bash
   # Create alarm for Lambda errors
   aws cloudwatch put-metric-alarm \
       --alarm-name "DataQualityDashboard-Errors" \
       --alarm-description "Lambda function errors" \
       --metric-name Errors \
       --namespace AWS/Lambda \
       --statistic Sum \
       --period 300 \
       --threshold 5 \
       --comparison-operator GreaterThanThreshold
   ```

### Custom Domain Setup (Optional)

1. **Register Domain in Route 53**
2. **Create SSL Certificate in ACM**
3. **Configure Custom Domain in API Gateway**
   ```bash
   # Create custom domain
   aws apigateway create-domain-name \
       --domain-name api.yourdomain.com \
       --certificate-arn arn:aws:acm:us-east-1:123456789:certificate/xxxxx
   ```

### Environment-Specific Configuration

**Development Environment:**
```bash
export FLASK_DEBUG=true
export LOG_LEVEL=DEBUG
serverless deploy --stage dev
```

**Production Environment:**
```bash
export FLASK_DEBUG=false
export LOG_LEVEL=INFO
serverless deploy --stage production
```

### Troubleshooting AWS Deployment

1. **Lambda Timeout Issues**
   - Increase timeout in `serverless.yml` (max 15 minutes)
   - Optimize database queries for large datasets

2. **RDS Connection Issues**
   - Verify security groups allow Lambda subnet access
   - Check VPC configuration if using private subnets

3. **Memory Issues**
   - Increase Lambda memory allocation (128MB to 3008MB)
   - Monitor CloudWatch metrics for optimization

### Security Best Practices

1. **Use AWS Secrets Manager**
   ```bash
   # Store database credentials securely
   aws secretsmanager create-secret \
       --name data-quality-dashboard/db \
       --description "Database credentials" \
       --secret-string '{"username":"admin","password":"YourSecurePassword123"}'
   ```

2. **Enable VPC for Lambda** (for production)
3. **Use least-privilege IAM roles**
4. **Enable CloudTrail for audit logging**

### Backup and Disaster Recovery

1. **Automated RDS Backups**: Enabled by default (7-day retention)
2. **Lambda Code Backup**: Stored in S3 automatically
3. **Cross-Region Deployment**: Deploy to multiple regions for high availability

For detailed troubleshooting and advanced configurations, refer to the DEPLOYMENT.md file.

## AWS CDK Infrastructure as Code

For enterprise deployments with complete infrastructure automation, use AWS CDK:

### Quick CDK Deployment
```bash
# Navigate to CDK directory
cd cdk

# Run automated deployment
./deploy.sh
```

### CDK Features
- **Complete Infrastructure**: VPC, RDS, Lambda, API Gateway, and monitoring
- **Security by Design**: Private subnets, security groups, and encrypted secrets
- **Production Ready**: Auto-scaling, backup, and monitoring configured
- **Cost Optimized**: Uses free tier resources where possible

### CDK vs Serverless Framework

| Feature | CDK | Serverless Framework |
|---------|-----|---------------------|
| **Infrastructure** | Complete VPC, RDS, monitoring | Lambda + API Gateway only |
| **Security** | Enterprise-grade isolation | Basic security groups |
| **Secrets** | AWS Secrets Manager integration | Environment variables |
| **Monitoring** | CloudWatch dashboards included | Basic logging |
| **Database** | Automated RDS provisioning | Manual RDS setup required |
| **Networking** | Private subnets, NAT gateways | Public access |

For complete CDK documentation, see [README-CDK.md](README-CDK.md).

## Support and Contributing

For issues and feature requests, please refer to the project documentation or contact the development team.
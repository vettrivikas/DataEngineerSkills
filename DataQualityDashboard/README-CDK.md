# AWS CDK Deployment Guide - Data Quality Dashboard

This guide provides Infrastructure as Code (IaC) deployment using AWS CDK for the Data Quality Dashboard with complete infrastructure automation.

## Overview

AWS CDK deployment creates a production-ready infrastructure with:
- **VPC with public/private subnets** for security isolation
- **RDS PostgreSQL database** in private subnets
- **Lambda function** for application hosting
- **API Gateway** for web interface
- **Secrets Manager** for secure credential storage
- **CloudWatch** for monitoring and logging
- **Security Groups** with least-privilege access

## Prerequisites

### 1. AWS Setup
```bash
# Install AWS CLI
pip install awscli

# Configure AWS credentials
aws configure
# Enter: Access Key ID, Secret Access Key, Region, Output format

# Bootstrap CDK in your account (one-time setup)
cdk bootstrap aws://ACCOUNT-NUMBER/REGION
```

### 2. Install CDK
```bash
# Install Node.js (required for CDK)
# Download from: https://nodejs.org/

# Install AWS CDK CLI
npm install -g aws-cdk

# Verify installation
cdk --version
```

### 3. Python Environment
```bash
# Create CDK environment
cd cdk
python -m venv venv

# Activate environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install CDK dependencies
pip install -r requirements.txt
```

## Infrastructure Components

### Network Architecture
- **VPC**: 10.0.0.0/16 CIDR with 2 Availability Zones
- **Public Subnets**: NAT Gateway and Load Balancer placement
- **Private Subnets**: Lambda functions with internet access
- **Database Subnets**: Isolated RDS instances (no internet)

### Security Design
- **Database Security Group**: Only allows Lambda access on port 5432
- **Lambda Security Group**: Outbound internet access for dependencies
- **Secrets Manager**: Encrypted credential storage with auto-rotation
- **IAM Roles**: Least-privilege access for Lambda execution

### Database Configuration
- **Engine**: PostgreSQL 15.4
- **Instance**: db.t3.micro (free tier eligible)
- **Storage**: 20GB GP2 with 7-day backup retention
- **Encryption**: At-rest and in-transit encryption enabled

## Deployment Steps

### 1. Configure CDK Context
```bash
cd cdk

# Set your AWS account and region
cdk deploy --context account=123456789012 --context region=us-east-1
```

### 2. Deploy Infrastructure
```bash
# Preview changes
cdk diff

# Deploy stack
cdk deploy

# Confirm deployment when prompted
# Type 'y' to proceed with IAM changes
```

### 3. Get Deployment Outputs
```bash
# View stack outputs
aws cloudformation describe-stacks \
    --stack-name DataQualityDashboardStack \
    --query 'Stacks[0].Outputs'
```

Expected outputs:
- **APIGatewayURL**: Your dashboard web interface URL
- **DatabaseEndpoint**: RDS instance endpoint
- **DatabaseCredentialsSecret**: ARN for database credentials

### 4. Initialize Database
```bash
# Get database credentials
aws secretsmanager get-secret-value \
    --secret-id "DATABASE_CREDENTIALS_SECRET_ARN" \
    --query SecretString --output text

# Connect and initialize database
psql -h DATABASE_ENDPOINT -U admin -d dataquality -f ../init-db.sql
```

## Environment Management

### Development Environment
```bash
# Deploy to development
cdk deploy --context env=dev
```

### Production Environment
```bash
# Deploy to production with enhanced security
cdk deploy --context env=prod --context deletionProtection=true
```

### Multiple Regions
```bash
# Deploy to different region
cdk deploy --context region=eu-west-1
```

## Monitoring and Observability

### CloudWatch Dashboard
Access the automatically created dashboard:
1. Go to AWS CloudWatch Console
2. Navigate to Dashboards
3. Open "DataQualityDashboard-Monitoring"

### Key Metrics
- **Lambda Invocations**: Request volume
- **Lambda Duration**: Response time performance
- **Lambda Errors**: Application failures
- **API Gateway Latency**: End-to-end response time
- **RDS Connections**: Database utilization

### Log Analysis
```bash
# View Lambda logs
aws logs tail /aws/lambda/DataQualityDashboardStack-DataQualityDashboardFunction --follow

# Query specific errors
aws logs filter-log-events \
    --log-group-name /aws/lambda/DataQualityDashboardStack-DataQualityDashboardFunction \
    --filter-pattern "ERROR"
```

## Security Best Practices

### 1. Network Security
- Database isolated in private subnets
- Lambda functions in VPC with controlled egress
- Security groups with minimal required access

### 2. Data Protection
- RDS encryption at rest and in transit
- Secrets Manager for credential rotation
- IAM roles with least-privilege principle

### 3. Access Control
```bash
# Enable CloudTrail for audit logging
aws cloudtrail create-trail \
    --name DataQualityDashboard-Audit \
    --s3-bucket-name your-audit-bucket
```

## Cost Optimization

### Free Tier Benefits
- **Lambda**: 1M requests/month free
- **API Gateway**: 1M API calls/month free
- **RDS t3.micro**: 750 hours/month free (12 months)
- **Secrets Manager**: 30 days free trial

### Production Costs (Monthly)
- **Lambda**: ~$0.20 per 1M requests
- **API Gateway**: ~$3.50 per 1M requests
- **RDS t3.micro**: ~$13/month
- **NAT Gateway**: ~$45/month (can be optimized)

### Cost Reduction Strategies
```bash
# Use scheduled Lambda for cost savings
# Implement in cdk/app.py:
schedule = events.Schedule.rate(Duration.minutes(5))
events.Rule(self, "ScheduledRule", schedule=schedule, targets=[targets.LambdaFunction(lambda_function)])
```

## Scaling and Performance

### Lambda Scaling
- **Default**: 1000 concurrent executions
- **Memory**: 1024 MB (adjustable 128MB-10GB)
- **Timeout**: 30 seconds (adjustable up to 15 minutes)

### Database Scaling
```bash
# Vertical scaling (resize instance)
aws rds modify-db-instance \
    --db-instance-identifier DataQualityDatabase \
    --db-instance-class db.t3.small \
    --apply-immediately

# Read replicas for read scaling
aws rds create-db-instance-read-replica \
    --db-instance-identifier DataQualityDatabase-ReadReplica \
    --source-db-instance-identifier DataQualityDatabase
```

## Troubleshooting

### Common Issues

1. **Lambda Timeout**
```bash
# Increase timeout in app.py
timeout=Duration.seconds(60)
```

2. **Database Connection Issues**
```bash
# Check security group rules
aws ec2 describe-security-groups \
    --group-names DatabaseSecurityGroup
```

3. **CDK Bootstrap Error**
```bash
# Re-bootstrap CDK
cdk bootstrap --force
```

### Debug Commands
```bash
# View CDK synthesized CloudFormation
cdk synth

# Compare deployed vs local changes
cdk diff

# List all CDK stacks
cdk list
```

## Cleanup and Destruction

### Remove Stack
```bash
# Destroy all resources
cdk destroy

# Confirm destruction (this will delete all data)
# Type stack name to confirm
```

### Selective Cleanup
```bash
# Remove only Lambda function
aws lambda delete-function \
    --function-name DataQualityDashboardStack-DataQualityDashboardFunction
```

## Advanced Configurations

### Custom Domain
```python
# Add to app.py
domain = apigateway.DomainName(
    self, "CustomDomain",
    domain_name="dashboard.yourdomain.com",
    certificate=acm.Certificate.from_certificate_arn(
        self, "Certificate",
        certificate_arn="arn:aws:acm:region:account:certificate/cert-id"
    )
)
```

### Multi-Environment Setup
```python
# Environment-specific configurations
if env == "prod":
    deletion_protection = True
    backup_retention = Duration.days(30)
else:
    deletion_protection = False
    backup_retention = Duration.days(7)
```

### Auto Scaling
```python
# Add Application Auto Scaling for RDS
auto_scaling_group = applicationautoscaling.ScalableTarget(
    self, "DatabaseAutoScaling",
    service_namespace=applicationautoscaling.ServiceNamespace.RDS,
    scalable_dimension="rds:cluster:ReadReplicaCount",
    min_capacity=1,
    max_capacity=5
)
```

## Integration with CI/CD

### GitHub Actions
```yaml
# .github/workflows/deploy.yml
name: Deploy CDK
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Setup Node.js
        uses: actions/setup-node@v2
        with:
          node-version: '18'
      - name: Install CDK
        run: npm install -g aws-cdk
      - name: Deploy
        run: |
          cd cdk
          pip install -r requirements.txt
          cdk deploy --require-approval never
```

This CDK deployment provides enterprise-grade infrastructure with security, monitoring, and scalability built in from day one.
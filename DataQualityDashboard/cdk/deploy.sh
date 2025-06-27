#!/bin/bash

# AWS CDK Deployment Script for Data Quality Dashboard
# This script automates the complete CDK deployment process

set -e

echo "🚀 AWS CDK Deployment for Data Quality Dashboard"
echo "==============================================="

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."
    
    if ! command_exists aws; then
        print_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    
    if ! command_exists cdk; then
        print_error "AWS CDK is not installed. Please install it first: npm install -g aws-cdk"
        exit 1
    fi
    
    if ! command_exists python3; then
        print_error "Python 3 is not installed."
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity >/dev/null 2>&1; then
        print_error "AWS credentials not configured. Run 'aws configure' first."
        exit 1
    fi
    
    print_status "All prerequisites met"
}

# Setup Python environment
setup_environment() {
    print_info "Setting up Python environment..."
    
    if [ ! -d "venv" ]; then
        python3 -m venv venv
        print_status "Created virtual environment"
    fi
    
    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
    
    print_status "Python environment ready"
}

# Bootstrap CDK if needed
bootstrap_cdk() {
    print_info "Checking CDK bootstrap status..."
    
    ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
    REGION=$(aws configure get region)
    
    if [ -z "$REGION" ]; then
        REGION="us-east-1"
        print_warning "No default region set, using us-east-1"
    fi
    
    # Check if bootstrap is needed
    if ! aws cloudformation describe-stacks --stack-name CDKToolkit --region $REGION >/dev/null 2>&1; then
        print_info "Bootstrapping CDK in account $ACCOUNT, region $REGION..."
        cdk bootstrap aws://$ACCOUNT/$REGION
        print_status "CDK bootstrap completed"
    else
        print_status "CDK already bootstrapped"
    fi
}

# Deploy the stack
deploy_stack() {
    print_info "Deploying Data Quality Dashboard stack..."
    
    # Get account and region for context
    ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
    REGION=$(aws configure get region)
    
    if [ -z "$REGION" ]; then
        REGION="us-east-1"
    fi
    
    echo "Deploying to Account: $ACCOUNT, Region: $REGION"
    
    # Deploy with context
    cdk deploy --context account=$ACCOUNT --context region=$REGION --require-approval never
    
    print_status "Stack deployment completed"
}

# Get stack outputs
get_outputs() {
    print_info "Retrieving stack outputs..."
    
    OUTPUTS=$(aws cloudformation describe-stacks \
        --stack-name DataQualityDashboardStack \
        --query 'Stacks[0].Outputs' \
        --output table 2>/dev/null)
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "🌐 Deployment Outputs:"
        echo "===================="
        echo "$OUTPUTS"
        echo ""
        
        # Extract API Gateway URL
        API_URL=$(aws cloudformation describe-stacks \
            --stack-name DataQualityDashboardStack \
            --query 'Stacks[0].Outputs[?OutputKey==`APIGatewayURL`].OutputValue' \
            --output text 2>/dev/null)
        
        if [ ! -z "$API_URL" ]; then
            print_status "Dashboard URL: $API_URL"
            echo ""
            print_info "You can now access your Data Quality Dashboard at the above URL"
        fi
    else
        print_warning "Could not retrieve stack outputs"
    fi
}

# Initialize database with sample data
init_database() {
    print_info "Would you like to initialize the database with sample data? (y/n)"
    read -r response
    
    if [[ "$response" =~ ^[Yy]$ ]]; then
        print_info "Retrieving database connection information..."
        
        # Get database endpoint
        DB_ENDPOINT=$(aws cloudformation describe-stacks \
            --stack-name DataQualityDashboardStack \
            --query 'Stacks[0].Outputs[?OutputKey==`DatabaseEndpoint`].OutputValue' \
            --output text 2>/dev/null)
        
        # Get credentials secret ARN
        SECRET_ARN=$(aws cloudformation describe-stacks \
            --stack-name DataQualityDashboardStack \
            --query 'Stacks[0].Outputs[?OutputKey==`DatabaseCredentialsSecret`].OutputValue' \
            --output text 2>/dev/null)
        
        if [ ! -z "$SECRET_ARN" ] && [ ! -z "$DB_ENDPOINT" ]; then
            print_info "Database endpoint: $DB_ENDPOINT"
            print_info "Credentials secret: $SECRET_ARN"
            echo ""
            print_info "To initialize the database, run the following commands:"
            echo ""
            echo "# Get database password:"
            echo "aws secretsmanager get-secret-value --secret-id '$SECRET_ARN' --query SecretString --output text"
            echo ""
            echo "# Connect and initialize:"
            echo "psql -h $DB_ENDPOINT -U admin -d dataquality -f ../init-db.sql"
            echo ""
        else
            print_warning "Could not retrieve database information"
        fi
    fi
}

# Main deployment process
main() {
    echo "Starting CDK deployment process..."
    echo ""
    
    check_prerequisites
    setup_environment
    bootstrap_cdk
    
    # Show deployment plan
    print_info "Showing deployment preview..."
    cdk diff
    
    echo ""
    print_info "Proceed with deployment? (y/n)"
    read -r response
    
    if [[ "$response" =~ ^[Yy]$ ]]; then
        deploy_stack
        get_outputs
        init_database
        
        echo ""
        print_status "Deployment completed successfully!"
        print_info "Next steps:"
        echo "  1. Access your dashboard using the provided URL"
        echo "  2. Initialize the database with sample data if needed"
        echo "  3. Monitor the application using CloudWatch dashboard"
        echo ""
    else
        print_info "Deployment cancelled"
        exit 0
    fi
}

# Handle script arguments
case "${1:-deploy}" in
    "deploy")
        main
        ;;
    "destroy")
        print_warning "This will destroy all resources. Are you sure? (type 'yes' to confirm)"
        read -r response
        if [ "$response" = "yes" ]; then
            cdk destroy --force
            print_status "Resources destroyed"
        else
            print_info "Destruction cancelled"
        fi
        ;;
    "diff")
        check_prerequisites
        setup_environment
        cdk diff
        ;;
    "status")
        aws cloudformation describe-stacks --stack-name DataQualityDashboardStack --query 'Stacks[0].StackStatus' --output text
        ;;
    "outputs")
        get_outputs
        ;;
    "help"|"-h"|"--help")
        echo "Usage: ./deploy.sh [command]"
        echo ""
        echo "Commands:"
        echo "  deploy    - Deploy the complete infrastructure (default)"
        echo "  destroy   - Destroy all resources"
        echo "  diff      - Show deployment differences"
        echo "  status    - Check stack status"
        echo "  outputs   - Show stack outputs"
        echo "  help      - Show this help message"
        ;;
    *)
        print_error "Unknown command: $1"
        echo "Use './deploy.sh help' for usage information"
        exit 1
        ;;
esac
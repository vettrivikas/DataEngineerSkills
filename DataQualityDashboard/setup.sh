#!/bin/bash

# Data Quality Dashboard Setup Script
# This script helps you set up the application in different environments

set -e

echo "🚀 Data Quality Dashboard Setup"
echo "================================"

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to create environment file
create_env_file() {
    echo "📝 Creating environment configuration..."
    
    if [ ! -f .env ]; then
        cp .env.example .env
        echo "✅ Created .env file from template"
        echo "📋 Please edit .env with your database credentials:"
        echo "   - PGHOST: Your database hostname"
        echo "   - PGPORT: Database port (5439 for Redshift, 5432 for PostgreSQL)"
        echo "   - PGDATABASE: Database name"
        echo "   - PGUSER: Database username"
        echo "   - PGPASSWORD: Database password"
        echo "   - SESSION_SECRET: Random secret key for sessions"
    else
        echo "✅ .env file already exists"
    fi
}

# Function to install Python dependencies
install_python_deps() {
    echo "📦 Installing Python dependencies..."
    
    if command_exists python3.11; then
        PYTHON_CMD=python3.11
    elif command_exists python3; then
        PYTHON_CMD=python3
    else
        echo "❌ Python 3.11+ is required but not found"
        exit 1
    fi
    
    echo "Using Python: $($PYTHON_CMD --version)"
    
    # Create virtual environment if it doesn't exist
    if [ ! -d "venv" ]; then
        echo "🔨 Creating virtual environment..."
        $PYTHON_CMD -m venv venv
    fi
    
    # Activate virtual environment
    source venv/bin/activate
    
    # Upgrade pip
    pip install --upgrade pip
    
    # Install dependencies
    pip install -r requirements.txt
    
    echo "✅ Python dependencies installed"
}

# Function to test database connection
test_database() {
    echo "🔌 Testing database connection..."
    
    source venv/bin/activate
    
    python -c "
from services.database import RedshiftService
try:
    db = RedshiftService()
    success, message = db.test_connection()
    if success:
        print('✅ Database connection successful')
    else:
        print(f'❌ Database connection failed: {message}')
        print('📋 Please check your .env file configuration')
except Exception as e:
    print(f'❌ Database test failed: {e}')
    print('📋 Make sure to configure your .env file with valid credentials')
"
}

# Function to run application
run_application() {
    echo "🚀 Starting application..."
    
    source venv/bin/activate
    
    echo "Application will be available at: http://localhost:5000"
    echo "Press Ctrl+C to stop the application"
    echo ""
    
    if command_exists gunicorn; then
        gunicorn --bind 0.0.0.0:5000 --reload main:app
    else
        python main.py
    fi
}

# Function to show usage
show_usage() {
    echo "Usage: ./setup.sh [option]"
    echo ""
    echo "Options:"
    echo "  install    - Install dependencies and create environment file"
    echo "  test       - Test database connection"
    echo "  run        - Run the application"
    echo "  docker     - Build and run with Docker"
    echo "  aws        - Show AWS deployment instructions"
    echo ""
    echo "Examples:"
    echo "  ./setup.sh install    # First time setup"
    echo "  ./setup.sh test       # Test your configuration"
    echo "  ./setup.sh run        # Start the application"
}

# Function to setup Docker
setup_docker() {
    echo "🐳 Setting up Docker deployment..."
    
    if ! command_exists docker; then
        echo "❌ Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command_exists docker-compose; then
        echo "❌ Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    echo "🔨 Building Docker containers..."
    docker-compose build
    
    echo "🚀 Starting containers..."
    docker-compose up -d
    
    echo "✅ Application is running with Docker"
    echo "🌐 Access the dashboard at: http://localhost:5000"
    echo "🔌 Database is available at: localhost:5432"
    
    echo ""
    echo "Useful commands:"
    echo "  docker-compose logs -f app    # View application logs"
    echo "  docker-compose down           # Stop containers"
    echo "  docker-compose restart        # Restart containers"
}

# Function to show AWS instructions
show_aws_instructions() {
    echo "☁️  AWS Serverless Deployment Instructions"
    echo "========================================="
    echo ""
    echo "Prerequisites:"
    echo "  1. Install AWS CLI: pip install awscli"
    echo "  2. Configure AWS credentials: aws configure"
    echo "  3. Install Serverless Framework: npm install -g serverless"
    echo "  4. Install plugins: npm install"
    echo ""
    echo "Setup AWS RDS (PostgreSQL/Aurora):"
    echo "  1. Create RDS instance in AWS Console"
    echo "  2. Configure security groups for Lambda access"
    echo "  3. Note the endpoint, username, and password"
    echo ""
    echo "Deploy to AWS Lambda:"
    echo "  1. Set environment variables:"
    echo "     export PGHOST=your-rds-endpoint.region.rds.amazonaws.com"
    echo "     export PGPORT=5432"
    echo "     export PGDATABASE=your_database"
    echo "     export PGUSER=your_username"
    echo "     export PGPASSWORD=your_password"
    echo "     export SESSION_SECRET=your-random-secret"
    echo ""
    echo "  2. Deploy: serverless deploy --stage production"
    echo ""
    echo "  3. View logs: serverless logs -f app --stage production"
    echo ""
    echo "For detailed instructions, see DEPLOYMENT.md"
}

# Main script logic
case "${1:-install}" in
    "install")
        create_env_file
        install_python_deps
        echo ""
        echo "✅ Setup complete!"
        echo "📋 Next steps:"
        echo "   1. Edit .env file with your database credentials"
        echo "   2. Run: ./setup.sh test"
        echo "   3. Run: ./setup.sh run"
        ;;
    "test")
        if [ ! -f .env ]; then
            echo "❌ .env file not found. Run: ./setup.sh install"
            exit 1
        fi
        test_database
        ;;
    "run")
        if [ ! -d venv ]; then
            echo "❌ Virtual environment not found. Run: ./setup.sh install"
            exit 1
        fi
        run_application
        ;;
    "docker")
        setup_docker
        ;;
    "aws")
        show_aws_instructions
        ;;
    "help"|"-h"|"--help")
        show_usage
        ;;
    *)
        echo "❌ Unknown option: $1"
        show_usage
        exit 1
        ;;
esac
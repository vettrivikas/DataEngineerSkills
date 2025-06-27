# Data Quality Dashboard - Bank of Canada Regulatory Compliance

## Overview

This is a Flask-based web application designed to monitor and report data quality metrics for Amazon Redshift databases, specifically tailored for Bank of Canada regulatory compliance requirements. The application provides real-time data quality assessments across multiple dimensions including completeness, timeliness, accuracy, consistency, and uniqueness, with special focus on Critical Data Elements (CDEs).

## System Architecture

### Backend Architecture
- **Framework**: Flask 3.1.1 with Python 3.11
- **Database**: Amazon Redshift via psycopg2-binary
- **Application Structure**: Modular blueprint-based architecture
  - `app.py`: Main Flask application factory
  - `main.py`: Application entry point
  - `config.py`: Centralized configuration management
  - `routes/dashboard.py`: Dashboard route handlers
  - `services/`: Business logic layer (database and data quality services)
  - `models.py`: Data models and schemas

### Frontend Architecture
- **Template Engine**: Jinja2 with HTML5
- **CSS Framework**: Bootstrap 5 with dark theme
- **JavaScript**: Vanilla JS for dynamic interactions
- **Charts**: Chart.js for data visualization
- **Icons**: Font Awesome 6.4.0

### Data Storage Solutions
- **Primary Database**: Amazon Redshift cluster
- **Configuration**: Environment variables with .env support
- **Session Management**: Flask sessions with configurable timeouts

## Key Components

### Database Service (`services/database.py`)
- **Purpose**: Handles all Redshift database operations
- **Features**: Connection pooling, schema/table discovery, query execution
- **Error Handling**: Comprehensive connection retry logic and error reporting

### Data Quality Service (`services/data_quality.py`)
- **Purpose**: Calculates data quality metrics across five dimensions
- **Metrics Calculated**:
  - Completeness: Non-null value percentages
  - Timeliness: Recent record presence validation
  - Accuracy: Numerical range validation
  - Consistency: Cross-field validation (e.g., currency codes)
  - Uniqueness: Duplicate detection in key columns

### Configuration Management (`config.py`)
- **Database Settings**: Redshift connection parameters
- **Quality Thresholds**: Configurable scoring thresholds (High: 90%, Medium: 70%)
- **Critical Data Elements**: Pre-defined list for regulatory focus
- **Validation Rules**: Data accuracy ranges and valid value sets

### Frontend Dashboard
- **Schema Explorer**: Dynamic dropdown population for schema/table selection
- **Real-time Metrics**: Live data quality score calculation and display
- **Visual Indicators**: Color-coded quality scores (Green: ≥70%, Yellow: 40-70%, Red: <40%)
- **Responsive Design**: Mobile-friendly Bootstrap implementation

## Data Flow

1. **Connection Establishment**: Application connects to Redshift using environment variables
2. **Schema Discovery**: Database service queries system tables to list available schemas/tables
3. **User Selection**: Frontend allows schema and table selection via AJAX-powered dropdowns
4. **Quality Analysis**: Data quality service runs comprehensive analysis on selected table
5. **Results Display**: Metrics are calculated, scored, and presented in dashboard format
6. **Critical Element Focus**: Special highlighting for Bank of Canada regulatory CDEs

## External Dependencies

### Core Dependencies
- **Flask Ecosystem**: Flask, Flask-SQLAlchemy, Werkzeug
- **Database**: psycopg2-binary for PostgreSQL/Redshift connectivity
- **Data Processing**: pandas for data manipulation and analysis
- **Visualization**: plotly for advanced charting capabilities
- **AWS Integration**: boto3 for potential S3 config file support
- **Web Server**: gunicorn for production deployment

### Frontend Dependencies (CDN)
- Bootstrap 5 CSS framework
- Chart.js for data visualization
- Font Awesome for iconography

## Deployment Strategy

### Replit Configuration
- **Runtime**: Python 3.11 with Nix package manager
- **Production Server**: Gunicorn with auto-scaling deployment target
- **Development**: Flask development server with hot reload
- **Environment**: Supports both development and production configurations

### Security Considerations
- Environment variable-based credential management
- Session security with configurable secret keys
- Proxy-aware deployment with ProxyFix middleware
- Debug mode controls for production safety

### Scalability Features
- Connection pooling for database efficiency
- Modular service architecture for easy extension
- Blueprint-based routing for feature modularity
- Configurable thresholds for different regulatory requirements

## Deployment Options

The application supports multiple deployment strategies:

### Local Development
- Virtual environment setup with `setup.sh install`
- Direct Python execution with Flask development server
- Production-ready Gunicorn server for testing

### Docker Deployment
- Complete containerized setup with `docker-compose.yml`
- Includes PostgreSQL database container
- Automated builds with health checks
- Volume persistence for data and logs

### AWS Serverless
- Lambda function deployment with `serverless.yml`
- API Gateway integration for web interface
- RDS/Aurora PostgreSQL backend
- Scalable and cost-effective for production workloads

### Production Server
- Ubuntu/CentOS deployment with Nginx reverse proxy
- Supervisor process management
- SSL/HTTPS configuration ready
- Log rotation and monitoring setup

## Setup Scripts

- `setup.sh` - Interactive setup script for all environments
- `init-db.sql` - Sample database schema and test data
- `DEPLOYMENT.md` - Comprehensive deployment guide
- `README.md` - Quick start and configuration guide

## Changelog

```
Changelog:
- June 27, 2025. Initial setup with core dashboard functionality
- June 27, 2025. Added comprehensive deployment configurations
  * Docker containerization with docker-compose
  * AWS serverless deployment with Lambda + API Gateway
  * Production server setup with Nginx + Supervisor
  * Interactive setup script for easy installation
  * Health check endpoint for monitoring
  * Sample database with realistic test data
```

## User Preferences

```
Preferred communication style: Simple, everyday language.
Deployment focus: Multiple environment support (local, Docker, AWS serverless, CDK)
Configuration priority: Easy setup with automated scripts
Infrastructure preference: CDK for enterprise deployments with complete automation
```

## Critical Data Elements Configuration

The application is pre-configured with Bank of Canada specific Critical Data Elements:
- `customer_id`: Primary customer identifier
- `credit_score`: Credit rating scores (300-850 range validation)
- `transaction_amount`: Financial transaction values
- `regulatory_flag`: Compliance status indicators
- `account_status`: Account state information

These elements receive priority scoring and highlighting in the dashboard interface, supporting regulatory reporting requirements and risk monitoring objectives.

## Performance Considerations

- Lazy loading of table data to minimize initial load times
- Cached schema information to reduce database queries
- Configurable timeout settings for long-running quality assessments
- Efficient SQL query patterns optimized for Redshift's columnar storage
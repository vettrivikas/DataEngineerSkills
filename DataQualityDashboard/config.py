import os
from datetime import timedelta

class Config:
    """Application configuration"""
    
    # Flask configuration
    SECRET_KEY = os.environ.get('SESSION_SECRET', 'dev-secret-key-change-in-production')
    DEBUG = os.environ.get('FLASK_DEBUG', 'True').lower() == 'true'
    
    # Database configuration
    REDSHIFT_HOST = os.environ.get('PGHOST', 'localhost')
    REDSHIFT_PORT = int(os.environ.get('PGPORT', '5439'))
    REDSHIFT_DATABASE = os.environ.get('PGDATABASE', 'dev')
    REDSHIFT_USER = os.environ.get('PGUSER', 'admin')
    REDSHIFT_PASSWORD = os.environ.get('PGPASSWORD', 'password')
    
    # Data Quality configuration
    TIMELINESS_DAYS_THRESHOLD = 30  # Days to consider for timeliness check
    COMPLETENESS_THRESHOLD_HIGH = 90  # High completeness threshold
    COMPLETENESS_THRESHOLD_MEDIUM = 70  # Medium completeness threshold
    
    # Critical Data Elements for Bank of Canada
    CRITICAL_DATA_ELEMENTS = [
        'customer_id',
        'credit_score', 
        'transaction_amount',
        'regulatory_flag',
        'account_status'
    ]
    
    # Data accuracy ranges
    ACCURACY_RANGES = {
        'credit_score': (300, 850),
        'transaction_amount': (0, 1000000),
        'account_balance': (-10000, 10000000),
        'age': (18, 120),
        'interest_rate': (0, 50)
    }
    
    # Valid currency codes for consistency checks
    VALID_CURRENCY_CODES = ['CAD', 'USD', 'EUR', 'GBP', 'JPY', 'CHF', 'AUD']
    
    # Session configuration
    PERMANENT_SESSION_LIFETIME = timedelta(hours=24)
    
    # Logging configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')
    
    @staticmethod
    def init_app(app):
        """Initialize application with configuration"""
        pass

class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True

class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    LOG_LEVEL = 'INFO'

# Configuration mapping
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}

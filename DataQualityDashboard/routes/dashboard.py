import logging
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from services.database import RedshiftService
from services.data_quality import DataQualityService

logger = logging.getLogger(__name__)

dashboard_bp = Blueprint('dashboard', __name__)

# Initialize services
db_service = RedshiftService()
dq_service = DataQualityService(db_service)

@dashboard_bp.route('/')
def index():
    """Main dashboard page"""
    # Test database connection
    connection_status, connection_message = db_service.test_connection()
    
    if not connection_status:
        flash(f"Database connection failed: {connection_message}", 'error')
        return render_template('dashboard.html', 
                             connection_status=False, 
                             connection_message=connection_message)
    
    # Get schemas for dropdown
    schemas = db_service.get_schemas()
    
    return render_template('dashboard.html', 
                         connection_status=True,
                         schemas=schemas)

@dashboard_bp.route('/api/tables/<schema_name>')
def get_tables(schema_name):
    """API endpoint to get tables for a schema"""
    try:
        tables = db_service.get_tables(schema_name)
        return jsonify({
            'success': True,
            'tables': [{'name': t.table_name, 'schema': t.schema_name} for t in tables]
        })
    except Exception as e:
        logger.error(f"Error fetching tables: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@dashboard_bp.route('/api/data-quality/<schema_name>/<table_name>')
def get_data_quality(schema_name, table_name):
    """API endpoint to get data quality metrics for a table"""
    try:
        # Get run_date from query parameters
        run_date_str = request.args.get('run_date')
        run_date = None
        
        if run_date_str:
            try:
                run_date = datetime.strptime(run_date_str, '%Y-%m-%d')
            except ValueError:
                return jsonify({'success': False, 'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
        
        # Calculate data quality summary
        summary = dq_service.calculate_data_quality_summary(schema_name, table_name, run_date)
        
        # Format response
        response_data = {
            'success': True,
            'summary': {
                'table_name': summary.table_name,
                'schema_name': summary.schema_name,
                'run_date': summary.run_date.isoformat(),
                'overall_scores': summary.overall_scores,
                'cde_scores': summary.cde_scores,
                'metrics': []
            }
        }
        
        # Group metrics by dimension
        metrics_by_dimension = {}
        for metric in summary.metrics:
            if metric.dimension not in metrics_by_dimension:
                metrics_by_dimension[metric.dimension] = []
            
            metrics_by_dimension[metric.dimension].append({
                'column_name': metric.column_name,
                'score': metric.score,
                'details': metric.details,
                'is_cde': metric.is_cde
            })
        
        response_data['summary']['metrics'] = metrics_by_dimension
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error calculating data quality: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@dashboard_bp.route('/quality-report/<schema_name>/<table_name>')
def quality_report(schema_name, table_name):
    """Detailed quality report page for a specific table"""
    try:
        # Get run_date from query parameters
        run_date_str = request.args.get('run_date')
        run_date = None
        
        if run_date_str:
            try:
                run_date = datetime.strptime(run_date_str, '%Y-%m-%d')
            except ValueError:
                flash('Invalid date format. Using current date.', 'warning')
        
        # Calculate data quality summary
        summary = dq_service.calculate_data_quality_summary(schema_name, table_name, run_date)
        
        # Get table columns for additional context
        columns = db_service.get_table_columns(schema_name, table_name)
        row_count = db_service.get_table_row_count(schema_name, table_name)
        
        return render_template('dashboard.html',
                             connection_status=True,
                             show_report=True,
                             summary=summary,
                             columns=columns,
                             row_count=row_count,
                             schemas=db_service.get_schemas())
        
    except Exception as e:
        logger.error(f"Error generating quality report: {str(e)}")
        flash(f"Error generating quality report: {str(e)}", 'error')
        return redirect(url_for('dashboard.index'))

@dashboard_bp.errorhandler(404)
def not_found(error):
    return render_template('dashboard.html', error="Page not found"), 404

@dashboard_bp.errorhandler(500)
def internal_error(error):
    return render_template('dashboard.html', error="Internal server error"), 500

@dashboard_bp.route('/health')
def health_check():
    """Health check endpoint for monitoring and load balancers"""
    try:
        # Test database connection
        connection_status, connection_message = db_service.test_connection()
        
        health_data = {
            'status': 'healthy' if connection_status else 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'database_status': connection_status,
            'database_message': connection_message,
            'version': '1.0.0'
        }
        
        status_code = 200 if connection_status else 503
        return jsonify(health_data), status_code
        
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'timestamp': datetime.now().isoformat(),
            'error': str(e)
        }), 503

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from models import DataQualityMetric, DataQualitySummary, ColumnInfo
from services.database import RedshiftService

logger = logging.getLogger(__name__)

class DataQualityService:
    """Service for calculating data quality metrics"""
    
    def __init__(self, db_service: RedshiftService):
        self.db_service = db_service
    
    def calculate_completeness(self, schema_name: str, table_name: str, columns: List[ColumnInfo]) -> List[DataQualityMetric]:
        """Calculate completeness metrics (% non-null values)"""
        metrics = []
        
        try:
            total_rows = self.db_service.get_table_row_count(schema_name, table_name)
            if total_rows == 0:
                return metrics
            
            for column in columns:
                try:
                    query = f"""
                    SELECT 
                        COUNT(*) - COUNT({column.column_name}) as null_count
                    FROM {schema_name}.{table_name}
                    """
                    
                    result = self.db_service.execute_query(query)
                    null_count = result[0]['null_count'] if result else 0
                    
                    completeness_score = ((total_rows - null_count) / total_rows) * 100
                    
                    metrics.append(DataQualityMetric(
                        dimension='completeness',
                        column_name=column.column_name,
                        score=round(completeness_score, 2),
                        details={
                            'total_rows': total_rows,
                            'null_count': null_count,
                            'non_null_count': total_rows - null_count
                        },
                        is_cde=column.is_cde
                    ))
                    
                except Exception as e:
                    logger.error(f"Error calculating completeness for {column.column_name}: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error calculating completeness metrics: {str(e)}")
            
        return metrics
    
    def calculate_timeliness(self, schema_name: str, table_name: str, columns: List[ColumnInfo], run_date: Optional[datetime] = None) -> List[DataQualityMetric]:
        """Calculate timeliness metrics (presence of recent records)"""
        metrics = []
        
        if run_date is None:
            run_date = datetime.now()
        
        # Look for date/timestamp columns
        date_columns = [col for col in columns if 'date' in col.data_type.lower() or 'timestamp' in col.data_type.lower()]
        
        for column in date_columns:
            try:
                # Check for records within last 30 days
                cutoff_date = run_date - timedelta(days=30)
                
                query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(CASE WHEN {column.column_name} >= %s THEN 1 END) as recent_rows
                FROM {schema_name}.{table_name}
                WHERE {column.column_name} IS NOT NULL
                """
                
                result = self.db_service.execute_query(query, (cutoff_date,))
                
                if result and result[0]['total_rows'] > 0:
                    total_rows = result[0]['total_rows']
                    recent_rows = result[0]['recent_rows']
                    timeliness_score = (recent_rows / total_rows) * 100
                    
                    metrics.append(DataQualityMetric(
                        dimension='timeliness',
                        column_name=column.column_name,
                        score=round(timeliness_score, 2),
                        details={
                            'total_rows': total_rows,
                            'recent_rows': recent_rows,
                            'cutoff_date': cutoff_date.isoformat()
                        },
                        is_cde=column.is_cde
                    ))
                    
            except Exception as e:
                logger.error(f"Error calculating timeliness for {column.column_name}: {str(e)}")
                
        return metrics
    
    def calculate_accuracy(self, schema_name: str, table_name: str, columns: List[ColumnInfo]) -> List[DataQualityMetric]:
        """Calculate accuracy metrics (values within expected ranges)"""
        metrics = []
        
        # Define expected ranges for common banking fields
        range_checks = {
            'credit_score': (300, 850),
            'transaction_amount': (0, 1000000),
            'account_balance': (-10000, 10000000),
            'age': (18, 120),
            'interest_rate': (0, 50)
        }
        
        for column in columns:
            try:
                # Check if column matches any range check pattern
                range_key = None
                for key in range_checks:
                    if key in column.column_name.lower():
                        range_key = key
                        break
                
                if range_key and 'numeric' in column.data_type.lower():
                    min_val, max_val = range_checks[range_key]
                    
                    query = f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(CASE WHEN {column.column_name} BETWEEN %s AND %s THEN 1 END) as valid_rows
                    FROM {schema_name}.{table_name}
                    WHERE {column.column_name} IS NOT NULL
                    """
                    
                    result = self.db_service.execute_query(query, (min_val, max_val))
                    
                    if result and result[0]['total_rows'] > 0:
                        total_rows = result[0]['total_rows']
                        valid_rows = result[0]['valid_rows']
                        accuracy_score = (valid_rows / total_rows) * 100
                        
                        metrics.append(DataQualityMetric(
                            dimension='accuracy',
                            column_name=column.column_name,
                            score=round(accuracy_score, 2),
                            details={
                                'total_rows': total_rows,
                                'valid_rows': valid_rows,
                                'expected_range': f"{min_val} - {max_val}"
                            },
                            is_cde=column.is_cde
                        ))
                        
            except Exception as e:
                logger.error(f"Error calculating accuracy for {column.column_name}: {str(e)}")
                
        return metrics
    
    def calculate_consistency(self, schema_name: str, table_name: str, columns: List[ColumnInfo]) -> List[DataQualityMetric]:
        """Calculate consistency metrics (cross-field validation)"""
        metrics = []
        
        # Look for currency code columns
        currency_columns = [col for col in columns if 'currency' in col.column_name.lower()]
        
        # Valid ISO currency codes for Canadian banking
        valid_currencies = ['CAD', 'USD', 'EUR', 'GBP', 'JPY', 'CHF', 'AUD']
        
        for column in currency_columns:
            try:
                query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(CASE WHEN UPPER({column.column_name}) IN ({','.join(['%s'] * len(valid_currencies))}) THEN 1 END) as valid_rows
                FROM {schema_name}.{table_name}
                WHERE {column.column_name} IS NOT NULL
                """
                
                result = self.db_service.execute_query(query, valid_currencies)
                
                if result and result[0]['total_rows'] > 0:
                    total_rows = result[0]['total_rows']
                    valid_rows = result[0]['valid_rows']
                    consistency_score = (valid_rows / total_rows) * 100
                    
                    metrics.append(DataQualityMetric(
                        dimension='consistency',
                        column_name=column.column_name,
                        score=round(consistency_score, 2),
                        details={
                            'total_rows': total_rows,
                            'valid_rows': valid_rows,
                            'valid_currencies': valid_currencies
                        },
                        is_cde=column.is_cde
                    ))
                    
            except Exception as e:
                logger.error(f"Error calculating consistency for {column.column_name}: {str(e)}")
                
        return metrics
    
    def calculate_uniqueness(self, schema_name: str, table_name: str, columns: List[ColumnInfo]) -> List[DataQualityMetric]:
        """Calculate uniqueness metrics (% unique values in key columns)"""
        metrics = []
        
        # Focus on ID columns and key business fields
        key_patterns = ['id', 'key', 'number', 'code']
        
        for column in columns:
            try:
                # Check if column is likely a key field
                is_key_field = any(pattern in column.column_name.lower() for pattern in key_patterns)
                
                if is_key_field or column.is_cde:
                    query = f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(DISTINCT {column.column_name}) as unique_rows
                    FROM {schema_name}.{table_name}
                    WHERE {column.column_name} IS NOT NULL
                    """
                    
                    result = self.db_service.execute_query(query)
                    
                    if result and result[0]['total_rows'] > 0:
                        total_rows = result[0]['total_rows']
                        unique_rows = result[0]['unique_rows']
                        uniqueness_score = (unique_rows / total_rows) * 100
                        
                        metrics.append(DataQualityMetric(
                            dimension='uniqueness',
                            column_name=column.column_name,
                            score=round(uniqueness_score, 2),
                            details={
                                'total_rows': total_rows,
                                'unique_rows': unique_rows,
                                'duplicate_rows': total_rows - unique_rows
                            },
                            is_cde=column.is_cde
                        ))
                        
            except Exception as e:
                logger.error(f"Error calculating uniqueness for {column.column_name}: {str(e)}")
                
        return metrics
    
    def calculate_data_quality_summary(self, schema_name: str, table_name: str, run_date: Optional[datetime] = None) -> DataQualitySummary:
        """Calculate comprehensive data quality summary for a table"""
        if run_date is None:
            run_date = datetime.now()
        
        # Get table columns
        columns = self.db_service.get_table_columns(schema_name, table_name)
        
        # Calculate all quality dimensions
        all_metrics = []
        all_metrics.extend(self.calculate_completeness(schema_name, table_name, columns))
        all_metrics.extend(self.calculate_timeliness(schema_name, table_name, columns, run_date))
        all_metrics.extend(self.calculate_accuracy(schema_name, table_name, columns))
        all_metrics.extend(self.calculate_consistency(schema_name, table_name, columns))
        all_metrics.extend(self.calculate_uniqueness(schema_name, table_name, columns))
        
        # Calculate overall scores by dimension
        overall_scores = {}
        cde_scores = {}
        
        dimensions = ['completeness', 'timeliness', 'accuracy', 'consistency', 'uniqueness']
        
        for dimension in dimensions:
            dimension_metrics = [m for m in all_metrics if m.dimension == dimension]
            cde_dimension_metrics = [m for m in dimension_metrics if m.is_cde]
            
            if dimension_metrics:
                overall_scores[dimension] = sum(m.score for m in dimension_metrics) / len(dimension_metrics)
            else:
                overall_scores[dimension] = None
                
            if cde_dimension_metrics:
                cde_scores[dimension] = sum(m.score for m in cde_dimension_metrics) / len(cde_dimension_metrics)
            else:
                cde_scores[dimension] = None
        
        return DataQualitySummary(
            table_name=table_name,
            schema_name=schema_name,
            run_date=run_date,
            metrics=all_metrics,
            overall_scores=overall_scores,
            cde_scores=cde_scores
        )

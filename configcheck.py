"""
AWS Glue Data Quality Check Script - Pure PySpark Version
Compatible with AWS Glue 5.0 (Spark 3.4, Python 3.10)

This script performs data quality checks using pure PySpark without pandas dependency.
Optimized for cloud-native performance and scalability.

UPDATED: Modified to use AWS Glue connections instead of hardcoded JDBC parameters.
Default connection: redshift-connection-dev2 (JDBC type)

Job Parameters:
- JOB_NAME (required): Name of the Glue job
- process_id (optional): Process ID for data quality checks (default: 11)
- connection_name (optional): Glue connection name (default: redshift-connection-dev2)
"""

import sys
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F

# Redshift Connection Configuration - Using Glue Connection
REDSHIFT_CONNECTION_NAME = 'redshift-connection-dev2'
REDSHIFT_CONFIG = {
    'connection_name': REDSHIFT_CONNECTION_NAME,
    'schema': 'dataquality'
}

# AWS Glue Job Parameters (can be overridden by job parameters)
GLUE_JOB_CONFIG = {
    'job_name': 'data-quality-checks-pure-pyspark',
    'temp_dir': 's3://your-bucket/temp/',
    'output_dir': 's3://your-bucket/output/',
    'log_level': 'INFO'
}

# Data Quality Configuration
DATA_QUALITY_CONFIG = {
    'process_id': 11,  # Default process ID, can be overridden
    'target_schema': 'dataquality',
    'metadata_tables': {
        'check_config': 'data_compare_check',
        'log_table': 'data_compare_log',
        'diff_table': 'data_compare_diff'
    },
    'batch_size': 1000,  # Batch size for processing large result sets
    'max_detail_records': 10000  # Maximum detail records to store per check
}

# Spark Configuration for Glue - Optimized for pure PySpark
SPARK_CONFIG = {
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.adaptive.coalescePartitions.enabled': 'true',
    'spark.sql.adaptive.skewJoin.enabled': 'true',
    'spark.sql.adaptive.advisoryPartitionSizeInBytes': '128MB',
    'spark.sql.adaptive.localShuffleReader.enabled': 'true',
    'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
    'spark.sql.execution.arrow.pyspark.enabled': 'false',  # Disable Arrow for pure PySpark
    'spark.sql.execution.arrow.pyspark.fallback.enabled': 'false'
}

# Connection Performance Configuration
CONNECTION_CONFIG = {
    'fetchsize': '10000',
    'batchsize': '1000',
    'queryTimeout': '300'
}


class PureSparkDataQuality:
    """Pure PySpark Data Quality Check Engine for AWS Glue"""
    
    def __init__(self, glue_context: GlueContext, job_name: str, connection_name: str = None):
        self.glue_context = glue_context
        self.spark = glue_context.spark_session
        self.job_name = job_name
        self.logger = glue_context.get_logger()
        
        # Configuration
        self.redshift_config = REDSHIFT_CONFIG
        self.target_schema = DATA_QUALITY_CONFIG['target_schema']
        self.batch_size = DATA_QUALITY_CONFIG['batch_size']
        self.max_detail_records = DATA_QUALITY_CONFIG['max_detail_records']
        
        # Get connection details from parameter or default configuration
        self.connection_name = connection_name or self.redshift_config['connection_name']
        
        # Initialize Redshift connection options using Glue connection
        self.redshift_options = {
            "connectionName": self.connection_name,
            "fetchsize": CONNECTION_CONFIG['fetchsize'],
            "batchsize": CONNECTION_CONFIG['batchsize']
        }
        
        self.logger.info(f"Initialized PureSparkDataQuality for job: {job_name}")
        self.logger.info(f"Using Glue connection: {self.connection_name}")
    
    def read_from_redshift(self, query: str) -> DataFrame:
        """Read data from Redshift using Glue connection with optimized settings"""
        try:
            self.logger.info(f"Executing query: {query[:100]}...")
            
            # Use Glue's create_dynamic_frame_from_options for connection-based access
            dynamic_frame = self.glue_context.create_dynamic_frame_from_options(
                connection_type="redshift",
                connection_options={
                    "connectionName": self.connection_name,
                    "query": query,
                    "fetchsize": CONNECTION_CONFIG['fetchsize'],
                    "batchsize": CONNECTION_CONFIG['batchsize']
                }
            )
            
            # Convert to DataFrame
            df = dynamic_frame.toDF()
            
            # Cache the DataFrame if it's going to be used multiple times
            df.cache()
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading from Redshift: {str(e)}")
            raise
    
    def write_to_redshift(self, df: DataFrame, table_name: str, mode: str = "append"):
        """Write DataFrame to Redshift using Glue connection with optimized batch processing"""
        try:
            full_table_name = f"{self.target_schema}.{table_name}"
            self.logger.info(f"Writing {df.count()} records to {full_table_name}")
            
            # Convert DataFrame to DynamicFrame
            dynamic_frame = self.glue_context.create_dynamic_frame.from_dataframe(
                df, self.glue_context, "dynamic_frame"
            )
            
            # Write using Glue connection
            self.glue_context.write_dynamic_frame.from_options(
                frame=dynamic_frame,
                connection_type="redshift",
                connection_options={
                    "connectionName": self.connection_name,
                    "dbtable": full_table_name,
                    "batchsize": CONNECTION_CONFIG['batchsize']
                },
                transformation_ctx="write_to_redshift"
            )
                
        except Exception as e:
            self.logger.error(f"Error writing to Redshift table {table_name}: {str(e)}")
            raise
    
    def get_next_run_id(self) -> int:
        """Get the next run ID for this execution"""
        try:
            query = f"SELECT COALESCE(MAX(run_id), 0) + 1 as next_id FROM {self.target_schema}.data_compare_log"
            result_df = self.read_from_redshift(query)
            next_id = result_df.select("next_id").collect()[0]["next_id"]
            self.logger.info(f"Next run ID: {next_id}")
            return next_id
        except Exception as e:
            self.logger.error(f"Error getting next run ID: {str(e)}")
            return 1
    
    def get_check_configurations(self, process_id: int) -> List[Dict]:
        """Get active check configurations for the given process ID"""
        query = f"""
        SELECT process_id, check_id, active, check_type, chunking_rule, 
               key_cols, src_qry, tgt_qry
        FROM {self.target_schema}.data_compare_check 
        WHERE active = true AND process_id = {process_id}
        ORDER BY check_id
        """
        
        config_df = self.read_from_redshift(query)
        
        # Convert to list of dictionaries using pure PySpark
        configs = []
        for row in config_df.collect():
            config = {
                'process_id': row['process_id'],
                'check_id': row['check_id'],
                'active': row['active'],
                'check_type': row['check_type'],
                'chunking_rule': row['chunking_rule'],
                'key_cols': row['key_cols'],
                'src_qry': row['src_qry'],
                'tgt_qry': row['tgt_qry']
            }
            configs.append(config)
        
        return configs
    
    def build_ootb_queries(self, check_type: str, key_cols: str, tgt_qry: str) -> Tuple[str, str]:
        """Build OOTB (Out of the Box) queries for count and details"""
        
        if check_type == 'OOTB_DUPCHECK':
            # Duplicate check queries
            count_query = f"""
            SELECT COUNT(*) as issue_count 
            FROM (
                SELECT {key_cols}, COUNT(*) as dup_count
                FROM {tgt_qry} 
                GROUP BY {key_cols} 
                HAVING COUNT(*) > 1
            ) duplicates
            """
            
            detail_query = f"""
            SELECT {key_cols} as duplicate_value, COUNT(*) as dup_count
            FROM {tgt_qry} 
            GROUP BY {key_cols} 
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
            LIMIT {self.max_detail_records}
            """
            
        elif check_type == 'OOTB_NOTNULL':
            # Parse table.column format
            parts = tgt_qry.split('.')
            if len(parts) >= 3:
                table_name = ".".join(parts[:-1])
                column_name = parts[-1]
            else:
                table_name = parts[0] if len(parts) == 2 else tgt_qry
                column_name = parts[1] if len(parts) == 2 else key_cols
            
            count_query = f"SELECT COUNT(*) as issue_count FROM {table_name} WHERE {column_name} IS NULL"
            detail_query = f"""
            SELECT '{column_name}' as column_name, 'NULL' as failing_value, COUNT(*) as null_count
            FROM {table_name} 
            WHERE {column_name} IS NULL
            GROUP BY '{column_name}', 'NULL'
            LIMIT {self.max_detail_records}
            """
            
        elif check_type == 'OOTB_MAXVAL':
            # Parse table.column format and threshold
            space_idx = tgt_qry.find(' ')
            table_col = tgt_qry[:space_idx]
            threshold = tgt_qry[space_idx:].strip()
            
            parts = table_col.split('.')
            if len(parts) >= 2:
                table_name = ".".join(parts[:-1])
                column_name = parts[-1]
            else:
                table_name = parts[0]
                column_name = key_cols
            
            count_query = f"SELECT COUNT(*) as issue_count FROM {table_name} WHERE {column_name} > {threshold}"
            detail_query = f"""
            SELECT {column_name} as failing_value, COUNT(*) as violation_count
            FROM {table_name} 
            WHERE {column_name} > {threshold}
            GROUP BY {column_name}
            ORDER BY {column_name} DESC
            LIMIT {self.max_detail_records}
            """
            
        elif check_type == 'OOTB_NOTIN':
            # Parse table.column format and allowed values
            space_idx = tgt_qry.find(' ')
            table_col = tgt_qry[:space_idx]
            allowed_values = tgt_qry[space_idx:].strip()
            
            parts = table_col.split('.')
            if len(parts) >= 2:
                table_name = ".".join(parts[:-1])
                column_name = parts[-1]
            else:
                table_name = parts[0]
                column_name = key_cols
            
            count_query = f"SELECT COUNT(*) as issue_count FROM {table_name} WHERE {column_name} NOT IN ({allowed_values})"
            detail_query = f"""
            SELECT {column_name} as failing_value, COUNT(*) as violation_count
            FROM {table_name} 
            WHERE {column_name} NOT IN ({allowed_values})
            GROUP BY {column_name}
            ORDER BY COUNT(*) DESC
            LIMIT {self.max_detail_records}
            """
            
        elif check_type == 'OOTB_FKEY_NOTFOUND':
            # Parse foreign key check format: table1.col1 table2.col2
            parts = tgt_qry.split(' ')
            table1_col = parts[0].split('.')
            table2_col = parts[1].split('.')
            
            table1_name = ".".join(table1_col[:-1])
            col1_name = table1_col[-1]
            table2_name = ".".join(table2_col[:-1])
            col2_name = table2_col[-1]
            
            count_query = f"""
            SELECT COUNT(*) as issue_count 
            FROM {table1_name} t1
            WHERE t1.{col1_name} NOT IN (SELECT {col2_name} FROM {table2_name} WHERE {col2_name} IS NOT NULL)
            """
            
            detail_query = f"""
            SELECT t1.{col1_name} as failing_value, COUNT(*) as violation_count
            FROM {table1_name} t1
            WHERE t1.{col1_name} NOT IN (SELECT {col2_name} FROM {table2_name} WHERE {col2_name} IS NOT NULL)
            GROUP BY t1.{col1_name}
            ORDER BY COUNT(*) DESC
            LIMIT {self.max_detail_records}
            """
        else:
            raise ValueError(f"Unsupported OOTB check type: {check_type}")
        
        return count_query, detail_query
    
    def execute_check(self, check_config: Dict) -> Tuple[int, List[Row], float]:
        """Execute a single data quality check"""
        start_time = time.time()
        
        check_id = check_config['check_id']
        check_type = check_config['check_type']
        key_cols = check_config['key_cols']
        src_qry = check_config['src_qry']
        tgt_qry = check_config['tgt_qry']
        
        self.logger.info(f"Processing Check ID {check_id} - Type: {check_type}")
        
        try:
            if check_type.startswith('OOTB_'):
                # Handle OOTB checks
                count_query, detail_query = self.build_ootb_queries(check_type, key_cols, tgt_qry)
                
                # Get count of issues
                count_df = self.read_from_redshift(count_query)
                issue_count = count_df.select("issue_count").collect()[0]["issue_count"]
                
                # Get detailed results if there are issues
                detail_results = []
                if issue_count > 0:
                    detail_df = self.read_from_redshift(detail_query)
                    detail_results = detail_df.collect()
                
            elif check_type == 'COMPARE':
                # Handle comparison checks between source and target
                if src_qry and tgt_qry:
                    src_df = self.read_from_redshift(src_qry)
                    tgt_df = self.read_from_redshift(tgt_qry)
                    
                    # Perform comparison logic
                    src_count = src_df.count()
                    tgt_count = tgt_df.count()
                    issue_count = abs(src_count - tgt_count)
                    
                    if issue_count > 0:
                        # Create detail result using pure PySpark
                        detail_data = [Row(src_count=src_count, tgt_count=tgt_count, difference=issue_count)]
                        detail_results = detail_data
                    else:
                        detail_results = []
                else:
                    issue_count = 0
                    detail_results = []
                    
            else:
                # Handle custom queries
                if tgt_qry:
                    result_df = self.read_from_redshift(tgt_qry)
                    issue_count = result_df.count()
                    
                    if issue_count > 0:
                        # Limit results for performance
                        limited_df = result_df.limit(self.max_detail_records)
                        detail_results = limited_df.collect()
                    else:
                        detail_results = []
                else:
                    issue_count = 0
                    detail_results = []
            
            duration = time.time() - start_time
            self.logger.info(f"Check {check_id} completed: {issue_count} issues found in {duration:.2f}s")
            
            return issue_count, detail_results, duration
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.error(f"Check {check_id} failed: {str(e)}")
            return -1, [], duration
    
    def create_log_dataframe(self, log_records: List[Tuple]) -> DataFrame:
        """Create log DataFrame using pure PySpark"""
        
        log_schema = StructType([
            StructField("process_id", IntegerType(), True),
            StructField("run_id", IntegerType(), True),
            StructField("check_id", IntegerType(), True),
            StructField("run_status", StringType(), True),
            StructField("numdiff", IntegerType(), True),
            StructField("run_duration", DoubleType(), True),
            StructField("run_date", StringType(), True),
            StructField("err_reason", StringType(), True)
        ])
        
        # Convert tuples to Rows
        log_rows = [Row(*record) for record in log_records]
        
        return self.spark.createDataFrame(log_rows, log_schema)
    
    def create_diff_dataframe(self, diff_records: List[Tuple]) -> DataFrame:
        """Create diff DataFrame using pure PySpark"""
        
        diff_schema = StructType([
            StructField("process_id", IntegerType(), True),
            StructField("run_id", IntegerType(), True),
            StructField("check_id", IntegerType(), True),
            StructField("colname", StringType(), True),
            StructField("pkeyval", StringType(), True),
            StructField("srcval", StringType(), True),
            StructField("tgtval", StringType(), True)
        ])
        
        # Convert tuples to Rows
        diff_rows = [Row(*record) for record in diff_records]
        
        return self.spark.createDataFrame(diff_rows, diff_schema)
    
    def store_log_results(self, log_records: List[Tuple]):
        """Store check results in log table using pure PySpark"""
        
        if not log_records:
            return
        
        log_df = self.create_log_dataframe(log_records)
        self.write_to_redshift(log_df, "data_compare_log")
    
    def store_detail_results(self, diff_records: List[Tuple]):
        """Store detailed check results in diff table using pure PySpark"""
        
        if not diff_records:
            return
        
        # Process in batches for large datasets
        batch_size = self.batch_size
        for i in range(0, len(diff_records), batch_size):
            batch = diff_records[i:i + batch_size]
            diff_df = self.create_diff_dataframe(batch)
            self.write_to_redshift(diff_df, "data_compare_diff")
            
            self.logger.info(f"Stored batch {i//batch_size + 1}: {len(batch)} detail records")
    
    def process_detail_results(self, process_id: int, run_id: int, check_id: int,
                             check_type: str, detail_results: List[Row]) -> List[Tuple]:
        """Process detailed results into diff records using pure PySpark operations"""
        
        diff_records = []
        
        if check_type == 'OOTB_DUPCHECK':
            # Process duplicate details
            for row in detail_results:
                # Extract duplicate value and count
                duplicate_value = str(row['duplicate_value'])
                count = int(row['dup_count'])
                
                diff_records.append((
                    process_id, run_id, check_id, 'duplicate_check',
                    duplicate_value, str(count), 'DUPLICATE'
                ))
        
        elif check_type == 'OOTB_NOTNULL':
            # Process null value details
            for row in detail_results:
                column_name = str(row['column_name'])
                null_count = int(row['null_count'])
                
                diff_records.append((
                    process_id, run_id, check_id, 'null_check',
                    column_name, str(null_count), 'NULL'
                ))
        
        elif check_type in ['OOTB_MAXVAL', 'OOTB_NOTIN']:
            # Process threshold/value violation details
            for row in detail_results:
                failing_value = str(row['failing_value'])
                violation_count = int(row['violation_count'])
                
                diff_records.append((
                    process_id, run_id, check_id, check_type.lower(),
                    failing_value, str(violation_count), 'VIOLATION'
                ))
        
        elif check_type == 'OOTB_FKEY_NOTFOUND':
            # Process foreign key violation details
            for row in detail_results:
                failing_value = str(row['failing_value'])
                violation_count = int(row['violation_count'])
                
                diff_records.append((
                    process_id, run_id, check_id, 'fkey_violation',
                    failing_value, str(violation_count), 'NOT_FOUND'
                ))
        
        elif check_type == 'COMPARE':
            # Process comparison details
            for row in detail_results:
                src_count = int(row['src_count'])
                tgt_count = int(row['tgt_count'])
                difference = int(row['difference'])
                
                diff_records.append((
                    process_id, run_id, check_id, 'count_comparison',
                    'row_count', str(src_count), str(tgt_count)
                ))
        
        return diff_records
    
    def run_data_quality_checks(self, process_id: int) -> Tuple[int, float]:
        """Run all data quality checks for the given process ID"""
        
        start_time = time.time()
        run_id = self.get_next_run_id()
        
        self.logger.info(f"Starting data quality checks for Process ID: {process_id}, Run ID: {run_id}")
        
        # Get check configurations
        check_configs = self.get_check_configurations(process_id)
        
        if not check_configs:
            self.logger.warning(f"No active checks found for process ID: {process_id}")
            return 0, 0.0
        
        total_checks = len(check_configs)
        self.logger.info(f"Found {total_checks} active checks to execute")
        
        # Collect all results for batch processing
        log_records = []
        all_diff_records = []
        
        # Execute each check
        for check_config in check_configs:
            check_id = check_config['check_id']
            check_type = check_config['check_type']
            
            try:
                # Execute the check
                issue_count, detail_results, duration = self.execute_check(check_config)
                
                # Determine status
                if issue_count == -1:
                    status = f"ERROR-{check_type}"
                    error_reason = "Check execution failed"
                elif issue_count > 0:
                    status = f"FAILED-{check_type}"
                    error_reason = ""
                else:
                    status = "OK"
                    error_reason = ""
                
                # Prepare log record
                log_record = (
                    process_id, run_id, check_id, status, issue_count,
                    duration, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), error_reason
                )
                log_records.append(log_record)
                
                # Process detailed results if any issues found
                if issue_count > 0 and detail_results:
                    diff_records = self.process_detail_results(
                        process_id, run_id, check_id, check_type, detail_results
                    )
                    all_diff_records.extend(diff_records)
                
            except Exception as e:
                error_msg = str(e)
                self.logger.error(f"Failed to execute check {check_id}: {error_msg}")
                
                # Add error log record
                error_record = (
                    process_id, run_id, check_id, f"ERROR-{check_type}", -1,
                    0.0, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), error_msg
                )
                log_records.append(error_record)
        
        # Store all results in batches
        self.logger.info("Storing check results...")
        
        # Store log results
        self.store_log_results(log_records)
        
        # Store detailed results
        if all_diff_records:
            self.store_detail_results(all_diff_records)
            self.logger.info(f"Stored {len(all_diff_records)} detailed result records")
        
        total_duration = time.time() - start_time
        self.logger.info(f"Completed {total_checks} checks in {total_duration:.2f} seconds")
        
        return total_checks, total_duration
    
    def generate_summary_report(self, run_id: int):
        """Generate summary report for the run using pure PySpark"""
        
        # Get summary statistics
        summary_query = f"""
        SELECT run_status, COUNT(*) as check_count, SUM(numdiff) as total_differences 
        FROM {self.target_schema}.data_compare_log 
        WHERE run_id = {run_id} 
        GROUP BY run_status
        ORDER BY run_status
        """
        
        summary_df = self.read_from_redshift(summary_query)
        summary_results = summary_df.collect()
        
        self.logger.info("=== DATA QUALITY CHECK SUMMARY ===")
        self.logger.info(f"Run ID: {run_id}")
        
        for row in summary_results:
            status = row['run_status']
            count = row['check_count']
            differences = row['total_differences'] if row['total_differences'] else 0
            self.logger.info(f"{status}: {count} checks, {differences} total issues")
        
        # Get failed checks details
        failed_query = f"""
        SELECT check_id, run_status, numdiff, err_reason 
        FROM {self.target_schema}.data_compare_log 
        WHERE run_id = {run_id} AND run_status != 'OK'
        ORDER BY check_id
        """
        
        failed_df = self.read_from_redshift(failed_query)
        failed_results = failed_df.collect()
        
        if failed_results:
            self.logger.info("\n=== FAILED CHECKS ===")
            for row in failed_results:
                check_id = row['check_id']
                status = row['run_status']
                issues = row['numdiff']
                error = row['err_reason'] if row['err_reason'] else 'N/A'
                self.logger.info(f"Check {check_id}: {status} - {issues} issues - {error}")
        
        # Write summary to S3 for monitoring
        self.write_summary_to_s3(run_id, summary_results, failed_results)
    
    def write_summary_to_s3(self, run_id: int, summary_results: List[Row], failed_results: List[Row]):
        """Write summary results to S3 using pure PySpark"""
        
        try:
            output_dir = GLUE_JOB_CONFIG['output_dir']
            
            # Create summary data
            summary_data = []
            for row in summary_results:
                summary_data.append(Row(
                    run_id=run_id,
                    job_name=self.job_name,
                    run_status=row['run_status'],
                    check_count=row['check_count'],
                    total_differences=row['total_differences'] if row['total_differences'] else 0,
                    timestamp=datetime.now().isoformat()
                ))
            
            if summary_data:
                summary_schema = StructType([
                    StructField("run_id", IntegerType(), True),
                    StructField("job_name", StringType(), True),
                    StructField("run_status", StringType(), True),
                    StructField("check_count", IntegerType(), True),
                    StructField("total_differences", IntegerType(), True),
                    StructField("timestamp", StringType(), True)
                ])
                
                summary_df = self.spark.createDataFrame(summary_data, summary_schema)
                
                # Write to S3 partitioned by run_id
                summary_df.write \
                    .mode("append") \
                    .partitionBy("run_id") \
                    .parquet(f"{output_dir}/summary_reports/")
                
                self.logger.info(f"Summary report written to S3: {output_dir}/summary_reports/")
            
        except Exception as e:
            self.logger.warning(f"Failed to write summary to S3: {str(e)}")


def main():
    """Main function for AWS Glue job"""
    
    # Get job arguments - only JOB_NAME is required
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    # Get optional parameters with defaults
    try:
        process_id = int(getResolvedOptions(sys.argv, ['process_id'])['process_id'])
    except:
        process_id = DATA_QUALITY_CONFIG['process_id']
    
    try:
        connection_name = getResolvedOptions(sys.argv, ['connection_name'])['connection_name']
    except:
        connection_name = REDSHIFT_CONNECTION_NAME
    
    # Initialize Glue context and job
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    
    # Log job configuration
    logger = glue_context.get_logger()
    logger.info(f"Starting Glue Data Quality Job: {args['JOB_NAME']}")
    logger.info(f"Process ID: {process_id}")
    logger.info(f"Redshift Connection: {connection_name}")
    
    # Apply Spark configurations
    for key, value in SPARK_CONFIG.items():
        sc.setLocalProperty(key, value)
    
    try:
        # Initialize data quality engine
        dq_engine = PureSparkDataQuality(glue_context, args['JOB_NAME'], connection_name)
        
        # Run data quality checks
        total_checks, total_duration = dq_engine.run_data_quality_checks(process_id)
        
        # Generate summary report
        if total_checks > 0:
            run_id = dq_engine.get_next_run_id() - 1  # Get the run ID we just used
            dq_engine.generate_summary_report(run_id)
        
        dq_engine.logger.info(f"Job completed successfully: {total_checks} checks in {total_duration:.2f}s")
        
    except Exception as e:
        glue_context.get_logger().error(f"Job failed: {str(e)}")
        raise
    
    finally:
        job.commit()


if __name__ == "__main__":
    main()

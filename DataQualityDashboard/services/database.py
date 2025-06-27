import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Optional, Tuple
from models import DatabaseConnection, SchemaInfo, TableInfo, ColumnInfo

logger = logging.getLogger(__name__)

class RedshiftService:
    """Service for Redshift database operations"""
    
    def __init__(self):
        self.connection_config = self._get_connection_config()
        self._connection = None
    
    def _get_connection_config(self) -> DatabaseConnection:
        """Get database connection configuration from environment variables"""
        return DatabaseConnection(
            host=os.getenv('PGHOST', 'localhost'),
            port=int(os.getenv('PGPORT', '5439')),
            database=os.getenv('PGDATABASE', 'dev'),
            user=os.getenv('PGUSER', 'admin'),
            password=os.getenv('PGPASSWORD', 'password')
        )
    
    def get_connection(self):
        """Get database connection with retry logic"""
        if self._connection is None or self._connection.closed:
            try:
                self._connection = psycopg2.connect(
                    host=self.connection_config.host,
                    port=self.connection_config.port,
                    database=self.connection_config.database,
                    user=self.connection_config.user,
                    password=self.connection_config.password,
                    cursor_factory=RealDictCursor
                )
                logger.info("Connected to Redshift successfully")
            except Exception as e:
                logger.error(f"Failed to connect to Redshift: {str(e)}")
                raise
        return self._connection
    
    def test_connection(self) -> Tuple[bool, str]:
        """Test database connection"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
    
    def get_schemas(self) -> List[SchemaInfo]:
        """Get all available schemas"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT 
                table_schema as schema_name,
                COUNT(table_name) as table_count
            FROM information_schema.tables 
            WHERE table_type = 'BASE TABLE'
            AND table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_internal')
            GROUP BY table_schema
            ORDER BY table_schema
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            
            return [SchemaInfo(
                schema_name=row['schema_name'],
                table_count=row['table_count']
            ) for row in results]
            
        except Exception as e:
            logger.error(f"Error fetching schemas: {str(e)}")
            return []
    
    def get_tables(self, schema_name: str) -> List[TableInfo]:
        """Get all tables in a schema"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT 
                table_schema,
                table_name
            FROM information_schema.tables 
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
            
            cursor.execute(query, (schema_name,))
            results = cursor.fetchall()
            cursor.close()
            
            return [TableInfo(
                schema_name=row['table_schema'],
                table_name=row['table_name']
            ) for row in results]
            
        except Exception as e:
            logger.error(f"Error fetching tables for schema {schema_name}: {str(e)}")
            return []
    
    def get_table_columns(self, schema_name: str, table_name: str) -> List[ColumnInfo]:
        """Get column information for a table"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = """
            SELECT 
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns 
            WHERE table_schema = %s 
            AND table_name = %s
            ORDER BY ordinal_position
            """
            
            cursor.execute(query, (schema_name, table_name))
            results = cursor.fetchall()
            cursor.close()
            
            # Define CDE columns for Bank of Canada use case
            cde_columns = {
                'customer_id', 'credit_score', 'transaction_amount', 
                'regulatory_flag', 'account_status'
            }
            
            return [ColumnInfo(
                column_name=row['column_name'],
                data_type=row['data_type'],
                is_nullable=row['is_nullable'] == 'YES',
                is_cde=row['column_name'].lower() in cde_columns
            ) for row in results]
            
        except Exception as e:
            logger.error(f"Error fetching columns for {schema_name}.{table_name}: {str(e)}")
            return []
    
    def get_table_row_count(self, schema_name: str, table_name: str) -> int:
        """Get row count for a table"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            query = f"SELECT COUNT(*) as row_count FROM {schema_name}.{table_name}"
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            
            return result['row_count'] if result else 0
            
        except Exception as e:
            logger.error(f"Error getting row count for {schema_name}.{table_name}: {str(e)}")
            return 0
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict]:
        """Execute a custom query and return results"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            results = cursor.fetchall()
            cursor.close()
            
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def close_connection(self):
        """Close database connection"""
        if self._connection and not self._connection.closed:
            self._connection.close()
            logger.info("Database connection closed")

from dataclasses import dataclass
from typing import Optional, Dict, List
from datetime import datetime

@dataclass
class DatabaseConnection:
    """Model for database connection configuration"""
    host: str
    port: int
    database: str
    user: str
    password: str

@dataclass
class SchemaInfo:
    """Model for schema information"""
    schema_name: str
    table_count: int

@dataclass
class TableInfo:
    """Model for table information"""
    schema_name: str
    table_name: str
    row_count: Optional[int] = None
    column_count: Optional[int] = None

@dataclass
class ColumnInfo:
    """Model for column information"""
    column_name: str
    data_type: str
    is_nullable: bool
    is_cde: bool = False  # Critical Data Element flag

@dataclass
class DataQualityMetric:
    """Model for data quality metrics"""
    dimension: str  # completeness, timeliness, accuracy, consistency, uniqueness
    column_name: str
    score: float  # 0-100 percentage
    details: Dict
    is_cde: bool = False

@dataclass
class DataQualitySummary:
    """Model for overall data quality summary"""
    table_name: str
    schema_name: str
    run_date: datetime
    metrics: List[DataQualityMetric]
    overall_scores: Dict[str, float]  # dimension -> average score
    cde_scores: Dict[str, float]  # CDE-specific scores

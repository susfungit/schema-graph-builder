"""
Schema extractor module - Main interface for extracting database schemas
"""

from ..connectors.postgres_connector import get_postgres_schema
from ..connectors.mysql_connector import get_mysql_schema
from ..connectors.mssql_connector import get_mssql_schema


def extract_schema(db_type: str, config_path: str):
    """
    Extract database schema based on database type.
    
    Args:
        db_type: Database type ('postgres', 'postgresql', 'mysql', 'mssql', 'sqlserver')
        config_path: Path to database configuration YAML file
        
    Returns:
        Dictionary containing database schema information
        
    Raises:
        ValueError: If database type is not supported
    """
    db_type_lower = db_type.lower()
    
    if db_type_lower in ['postgres', 'postgresql']:
        return get_postgres_schema(config_path)
    elif db_type_lower == 'mysql':
        return get_mysql_schema(config_path)
    elif db_type_lower in ['mssql', 'sqlserver']:
        return get_mssql_schema(config_path)
    else:
        raise ValueError(f"Unsupported database type: '{db_type}'") 
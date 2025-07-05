"""
Schema extraction module for extracting database schemas from different database types
"""

from typing import Dict, Any, Optional
import yaml
from pathlib import Path


def extract_schema(db_type: str, config_file: str) -> Dict[str, Any]:
    """
    Extract schema from database based on database type
    
    Args:
        db_type: Type of database (postgres, mysql, mssql, etc.)
        config_file: Path to configuration file
        
    Returns:
        Dictionary containing schema information
        
    Raises:
        ValueError: If database type is unsupported
    """
    if not db_type:
        raise ValueError("Unsupported database type: ''")
    
    # Normalize database type to lowercase
    db_type_lower = db_type.lower()
    
    # Map database types to connector functions
    if db_type_lower in ['postgres', 'postgresql']:
        return get_postgres_schema(config_file)
    elif db_type_lower == 'mysql':
        return get_mysql_schema(config_file)
    elif db_type_lower in ['mssql', 'sqlserver']:
        return get_mssql_schema(config_file)
    else:
        raise ValueError(f"Unsupported database type: '{db_type}'")


def get_postgres_schema(config_file: str) -> Dict[str, Any]:
    """
    Extract schema from PostgreSQL database
    
    Args:
        config_file: Path to configuration file
        
    Returns:
        Dictionary containing PostgreSQL schema information
    """
    # Import here to avoid circular imports
    from schema_graph_builder.connectors.postgres_connector import get_postgres_schema as _get_postgres_schema
    
    return _get_postgres_schema(config_file)


def get_mysql_schema(config_file: str) -> Dict[str, Any]:
    """
    Extract schema from MySQL database
    
    Args:
        config_file: Path to configuration file
        
    Returns:
        Dictionary containing MySQL schema information
    """
    # Import here to avoid circular imports
    from schema_graph_builder.connectors.mysql_connector import get_mysql_schema as _get_mysql_schema
    
    return _get_mysql_schema(config_file)


def get_mssql_schema(config_file: str) -> Dict[str, Any]:
    """
    Extract schema from MS SQL Server database
    
    Args:
        config_file: Path to configuration file
        
    Returns:
        Dictionary containing MS SQL Server schema information
    """
    # Import here to avoid circular imports
    from schema_graph_builder.connectors.mssql_connector import get_mssql_schema as _get_mssql_schema
    
    return _get_mssql_schema(config_file)


def _load_config(config_file: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file
    
    Args:
        config_file: Path to configuration file
        
    Returns:
        Dictionary containing configuration data
        
    Raises:
        FileNotFoundError: If configuration file doesn't exist
        yaml.YAMLError: If configuration file is invalid YAML
    """
    config_path = Path(config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_file}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) 
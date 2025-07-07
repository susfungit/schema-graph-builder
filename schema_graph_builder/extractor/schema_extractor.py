"""
Schema extractor module - Main interface for extracting database schemas
"""

from typing import Dict, Any
from ..connectors.base_connector import DatabaseConnector
from ..connectors.postgres_connector import PostgreSQLConnector
from ..connectors.mysql_connector import MySQLConnector
from ..connectors.mssql_connector import MSSQLConnector


# Registry of database types to connector classes
DATABASE_CONNECTORS = {
    'postgres': PostgreSQLConnector,
    'postgresql': PostgreSQLConnector,
    'mysql': MySQLConnector,
    'mssql': MSSQLConnector,
    'sqlserver': MSSQLConnector,
}


def extract_schema(db_type: str, config_path: str) -> Dict[str, Any]:
    """
    Extract database schema based on database type using the connector abstraction.
    
    Args:
        db_type: Database type ('postgres', 'postgresql', 'mysql', 'mssql', 'sqlserver')
        config_path: Path to database configuration YAML file
        
    Returns:
        Dictionary containing database schema information
        
    Raises:
        ValueError: If database type is not supported
    """
    db_type_lower = db_type.lower()
    
    if db_type_lower not in DATABASE_CONNECTORS:
        supported_types = ', '.join(DATABASE_CONNECTORS.keys())
        raise ValueError(f"Unsupported database type: '{db_type}'. Supported types: {supported_types}")
    
    # Get the connector class and create an instance
    connector_class = DATABASE_CONNECTORS[db_type_lower]
    connector = connector_class()
    
    # Extract the schema using the connector
    return connector.extract_schema(config_path)


def get_supported_database_types() -> list:
    """
    Get list of supported database types.
    
    Returns:
        List of supported database type strings
    """
    return list(DATABASE_CONNECTORS.keys())


def register_database_connector(db_type: str, connector_class: type) -> None:
    """
    Register a new database connector type.
    
    Args:
        db_type: Database type identifier
        connector_class: DatabaseConnector subclass
        
    Raises:
        ValueError: If connector_class is not a DatabaseConnector subclass
    """
    if not issubclass(connector_class, DatabaseConnector):
        raise ValueError("connector_class must be a subclass of DatabaseConnector")
    
    DATABASE_CONNECTORS[db_type.lower()] = connector_class 
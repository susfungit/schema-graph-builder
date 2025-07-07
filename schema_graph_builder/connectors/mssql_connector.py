"""
MS SQL Server database connector
"""

from typing import Dict, Any
from .base_connector import DatabaseConnector


class MSSQLConnector(DatabaseConnector):
    """MS SQL Server database connector implementation."""
    
    def __init__(self):
        """Initialize MSSQL connector with default port."""
        super().__init__('mssql', 1433)
    
    def _get_db_specific_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get MSSQL-specific connection parameters.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Dictionary with MSSQL-specific parameters
        """
        driver = config.get('driver', 'ODBC Driver 18 for SQL Server')
        trust_cert = config.get('trust_server_certificate', True)
        
        return {
            'driver': driver.replace(' ', '+'),
            'trust_server_certificate': 'yes' if trust_cert else 'no'
        }
    
    def _get_connect_args(self) -> Dict[str, Any]:
        """
        Get MSSQL-specific connection arguments.
        
        Returns:
            Dictionary with connection arguments
        """
        return {
            "timeout": 30,       # 30 second connection timeout
            "autocommit": True,  # Prevent transaction locks
        }


def get_mssql_schema(config_path: str) -> Dict[str, Any]:
    """
    Extract schema from MS SQL Server database.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Dictionary containing schema information
        
    Raises:
        ValueError: If configuration is invalid
        ConnectionError: If database connection fails
    """
    connector = MSSQLConnector()
    return connector.extract_schema(config_path) 
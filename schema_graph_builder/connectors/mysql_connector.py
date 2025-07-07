"""
MySQL database connector
"""

from typing import Dict, Any
from .base_connector import DatabaseConnector


class MySQLConnector(DatabaseConnector):
    """MySQL database connector implementation."""
    
    def __init__(self):
        """Initialize MySQL connector with default port."""
        super().__init__('mysql', 3306)
    
    def _get_db_specific_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get MySQL-specific connection parameters.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Dictionary with MySQL-specific parameters
        """
        # MySQL doesn't need additional parameters beyond the standard ones
        return {}
    
    def _get_connect_args(self) -> Dict[str, Any]:
        """
        Get MySQL-specific connection arguments.
        
        Returns:
            Dictionary with connection arguments
        """
        return {
            "connect_timeout": 30,  # 30 second connection timeout
            "autocommit": True,     # Prevent transaction locks
        }


def get_mysql_schema(config_path: str) -> Dict[str, Any]:
    """
    Extract schema from MySQL database.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Dictionary containing schema information
        
    Raises:
        ValueError: If configuration is invalid
        ConnectionError: If database connection fails
    """
    connector = MySQLConnector()
    return connector.extract_schema(config_path) 
"""
PostgreSQL database connector
"""

from typing import Dict, Any
from .base_connector import DatabaseConnector


class PostgreSQLConnector(DatabaseConnector):
    """PostgreSQL database connector implementation."""
    
    def __init__(self):
        """Initialize PostgreSQL connector with default port."""
        super().__init__('postgres', 5432)
    
    def _get_db_specific_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get PostgreSQL-specific connection parameters.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Dictionary with PostgreSQL-specific parameters
        """
        # PostgreSQL doesn't need additional parameters beyond the standard ones
        return {}
    
    def _get_connect_args(self) -> Dict[str, Any]:
        """
        Get PostgreSQL-specific connection arguments.
        
        Returns:
            Dictionary with connection arguments
        """
        return {
            "connect_timeout": 30,  # 30 second connection timeout
            "options": "-c default_transaction_isolation=read_committed"
        }


def get_postgres_schema(config_path: str) -> Dict[str, Any]:
    """
    Extract schema from PostgreSQL database.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Dictionary containing schema information
    """
    connector = PostgreSQLConnector()
    return connector.extract_schema(config_path) 
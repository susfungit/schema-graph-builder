"""
Oracle database connector
"""

from typing import Dict, Any
from .base_connector import DatabaseConnector


class OracleConnector(DatabaseConnector):
    """Oracle database connector implementation."""
    
    def __init__(self):
        """Initialize Oracle connector with default port."""
        super().__init__('oracle', 1521)
    
    def _get_connection_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Override to handle Oracle-specific database identifier logic.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Dictionary with Oracle connection parameters
        """
        service_name = config.get('service_name')
        sid = config.get('sid')
        
        # Oracle requires either service_name or sid
        if not service_name and not sid:
            raise ValueError("Oracle configuration requires either 'service_name' or 'sid'")
        
        if service_name and sid:
            raise ValueError("Oracle configuration cannot have both 'service_name' and 'sid'. Choose one.")
        
        database_identifier = service_name or sid
        
        params = {
            'host': config.get('host', 'localhost'),
            'port': config.get('port', self.default_port),
            'database': database_identifier
        }
        
        # Add Oracle-specific parameters
        params.update(self._get_db_specific_params(config))
        
        return params
    
    def _get_db_specific_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get Oracle-specific connection parameters.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Dictionary with Oracle-specific parameters
        """
        service_name = config.get('service_name')
        sid = config.get('sid')
        
        params = {}
        if service_name:
            params['service_name'] = service_name
        if sid:
            params['sid'] = sid
            
        return params
    
    def _get_connect_args(self) -> Dict[str, Any]:
        """
        Get Oracle-specific connection arguments.
        
        Returns:
            Dictionary with connection arguments
        """
        return {
            "autocommit": True,     # Prevent transaction locks
        }


def get_oracle_schema(config_path: str) -> Dict[str, Any]:
    """
    Extract schema from Oracle database.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Dictionary containing schema information
        
    Raises:
        ValueError: If configuration is invalid
        ConnectionError: If database connection fails
    """
    connector = OracleConnector()
    return connector.extract_schema(config_path) 
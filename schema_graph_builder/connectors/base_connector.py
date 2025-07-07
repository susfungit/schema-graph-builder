"""
Abstract base class for database connectors

This module provides the base interface and common functionality for all database connectors,
eliminating code duplication and providing a consistent interface.
"""

import os
import yaml
import logging
from abc import ABC, abstractmethod
from sqlalchemy import create_engine, inspect
from sqlalchemy.pool import StaticPool
from typing import Dict, Any, Optional

from ..utils.security import CredentialManager, ConnectionSecurity, AuditLogger
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


class DatabaseConnector(ABC):
    """
    Abstract base class for database connectors.
    
    This class provides common functionality for all database types while allowing
    each connector to customize database-specific behavior.
    """
    
    def __init__(self, db_type: str, default_port: int):
        """
        Initialize the database connector.
        
        Args:
            db_type: Database type identifier (e.g., 'postgres', 'mysql', 'mssql')
            default_port: Default port number for this database type
        """
        self.db_type = db_type
        self.default_port = default_port
        self.audit_logger = AuditLogger()
        self.engine = None
    
    def extract_schema(self, config_path: str) -> Dict[str, Any]:
        """
        Extract database schema with security enhancements.
        
        Args:
            config_path: Path to YAML configuration file
            
        Returns:
            Dictionary containing schema information
            
        Raises:
            ValueError: If configuration is invalid
            ConnectionError: If database connection fails
        """
        try:
            # Load and validate configuration
            config = self._load_config(config_path)
            
            # Get secure credentials
            username, password = CredentialManager.get_credentials(config)
            
            # Get connection parameters
            connection_params = self._get_connection_params(config)
            
            # Log connection attempt
            self.audit_logger.log_connection_attempt(
                self.db_type, connection_params['host'], 
                connection_params['database'], username
            )
            
            # Create secure connection
            self._create_connection(connection_params, username, password)
            
            # Extract schema
            schema = self._extract_schema_data(connection_params['database'])
            
            # Log successful extraction
            self.audit_logger.log_connection_success(
                self.db_type, connection_params['host'], connection_params['database']
            )
            self.audit_logger.log_schema_extraction(
                self.db_type, connection_params['database'], len(schema['tables'])
            )
            
            logger.info(f"Successfully extracted {self.db_type} schema: {len(schema['tables'])} tables")
            return schema
            
        except Exception as e:
            if hasattr(self, 'audit_logger') and 'connection_params' in locals():
                self.audit_logger.log_connection_failure(
                    self.db_type, connection_params.get('host', 'unknown'),
                    connection_params.get('database', 'unknown'), str(e)
                )
            raise
        finally:
            self._cleanup_connection()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load and validate configuration from YAML file."""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        if not isinstance(config, dict):
            raise ValueError("Configuration must be a dictionary")
        
        return config
    
    def _get_connection_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get connection parameters from configuration.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Dictionary with connection parameters
        """
        database = config.get('database')
        if not database:
            raise ValueError("Database name is required in configuration")
        
        params = {
            'host': config.get('host', 'localhost'),
            'port': config.get('port', self.default_port),
            'database': database
        }
        
        # Add database-specific parameters
        params.update(self._get_db_specific_params(config))
        
        return params
    
    def _create_connection(self, connection_params: Dict[str, Any], username: str, password: str) -> None:
        """
        Create database connection with security settings.
        
        Args:
            connection_params: Connection parameters
            username: Database username
            password: Database password
        """
        # Create secure connection string
        db_url = ConnectionSecurity.create_secure_connection_string(
            self.db_type, connection_params['host'], connection_params['port'],
            connection_params['database'], username, password,
            **{k: v for k, v in connection_params.items() if k not in ['host', 'port', 'database']}
        )
        
        # Log connection attempt (with masked credentials)
        masked_url = ConnectionSecurity.mask_connection_string(db_url)
        logger.info(f"Connecting to {self.db_type}: {masked_url}")
        
        # Create engine with security settings
        connect_args = self._get_connect_args()
        
        self.engine = create_engine(
            db_url,
            poolclass=StaticPool,
            pool_pre_ping=True,  # Verify connections before use
            pool_recycle=3600,   # Recycle connections every hour
            connect_args=connect_args
        )
    
    def _extract_schema_data(self, database: str) -> Dict[str, Any]:
        """
        Extract schema data using SQLAlchemy inspector.
        
        Args:
            database: Database name
            
        Returns:
            Dictionary containing schema information
        """
        inspector = inspect(self.engine)
        
        schema = {
            'database': database,
            'tables': []
        }
        
        try:
            table_names = inspector.get_table_names()
            
            for table_name in table_names:
                # Validate table name for security
                if not table_name or not isinstance(table_name, str):
                    logger.warning(f"Skipping invalid table name: {table_name}")
                    continue
                
                table_info = {
                    'name': table_name,
                    'columns': []
                }
                
                # Get column information
                try:
                    columns = inspector.get_columns(table_name)
                    pk_constraint = inspector.get_pk_constraint(table_name)
                    pk_columns = pk_constraint.get('constrained_columns', [])
                    
                    for column in columns:
                        if not isinstance(column, dict) or 'name' not in column:
                            logger.warning(f"Skipping invalid column in table {table_name}")
                            continue
                        
                        column_info = {
                            'name': str(column['name']),
                            'type': str(column.get('type', 'UNKNOWN')),
                            'nullable': bool(column.get('nullable', True)),
                            'primary_key': column['name'] in pk_columns
                        }
                        table_info['columns'].append(column_info)
                
                except Exception as e:
                    logger.warning(f"Error extracting columns for table {table_name}: {e}")
                    continue
                
                schema['tables'].append(table_info)
            
            return schema
            
        except Exception as e:
            error_msg = f"Failed to extract schema: {e}"
            raise ConnectionError(error_msg) from e
    
    def _cleanup_connection(self) -> None:
        """Clean up database connection."""
        if self.engine:
            try:
                self.engine.dispose()
                logger.debug(f"{self.db_type} connection disposed")
            except Exception as e:
                logger.warning(f"Error disposing {self.db_type} connection: {e}")
            finally:
                self.engine = None
    
    @abstractmethod
    def _get_db_specific_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get database-specific connection parameters.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Dictionary with database-specific parameters
        """
        pass
    
    @abstractmethod
    def _get_connect_args(self) -> Dict[str, Any]:
        """
        Get database-specific connection arguments.
        
        Returns:
            Dictionary with connection arguments
        """
        pass


# Convenience functions for backward compatibility
def create_postgres_connector() -> DatabaseConnector:
    """Create a PostgreSQL connector instance."""
    from .postgres_connector import PostgreSQLConnector
    return PostgreSQLConnector()


def create_mysql_connector() -> DatabaseConnector:
    """Create a MySQL connector instance."""
    from .mysql_connector import MySQLConnector
    return MySQLConnector()


def create_mssql_connector() -> DatabaseConnector:
    """Create a MSSQL connector instance."""
    from .mssql_connector import MSSQLConnector
    return MSSQLConnector() 
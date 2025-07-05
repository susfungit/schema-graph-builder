"""
MySQL database connector with security enhancements
"""

import os
import yaml
import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.pool import StaticPool
from typing import Dict, Any

from ..utils.security import CredentialManager, ConnectionSecurity, AuditLogger
from ..utils.logging_config import get_logger

logger = get_logger(__name__)


def get_mysql_schema(config_path: str) -> Dict[str, Any]:
    """
    Extract schema from MySQL database with security enhancements.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Dictionary containing schema information
        
    Raises:
        ValueError: If configuration is invalid
        ConnectionError: If database connection fails
    """
    audit_logger = AuditLogger()
    
    try:
        # Load and validate configuration
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        if not isinstance(config, dict):
            raise ValueError("Configuration must be a dictionary")
        
        # Get secure credentials
        username, password = CredentialManager.get_credentials(config)
        
        # Get connection parameters with defaults
        host = config.get('host', 'localhost')
        port = config.get('port', 3306)
        database = config.get('database')
        
        if not database:
            raise ValueError("Database name is required in configuration")
        
        # Log connection attempt
        audit_logger.log_connection_attempt('mysql', host, database, username)
        
        # Create secure connection string
        db_url = ConnectionSecurity.create_secure_connection_string(
            'mysql', host, port, database, username, password
        )
        
        # Log connection attempt (with masked credentials)
        masked_url = ConnectionSecurity.mask_connection_string(db_url)
        logger.info(f"Connecting to MySQL: {masked_url}")
        
        # Create engine with security settings
        engine = create_engine(
            db_url,
            poolclass=StaticPool,
            pool_pre_ping=True,  # Verify connections before use
            pool_recycle=3600,   # Recycle connections every hour
            connect_args={
                "connect_timeout": 30,  # 30 second connection timeout
                "autocommit": True,     # Prevent transaction locks
            }
        )
        
        inspector = inspect(engine)
        
        # Extract schema
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
            
            # Log successful extraction
            audit_logger.log_connection_success('mysql', host, database)
            audit_logger.log_schema_extraction('mysql', database, len(schema['tables']))
            logger.info(f"Successfully extracted MySQL schema: {len(schema['tables'])} tables")
            
            return schema
            
        except Exception as e:
            error_msg = f"Failed to extract schema: {e}"
            audit_logger.log_connection_failure('mysql', host, database, error_msg)
            raise ConnectionError(error_msg) from e
            
    except Exception as e:
        if 'audit_logger' in locals():
            audit_logger.log_connection_failure('mysql', host, database, str(e))
        raise
        
    finally:
        # Ensure connection is properly closed
        if 'engine' in locals():
            try:
                engine.dispose()
                logger.debug("MySQL connection disposed")
            except Exception as e:
                logger.warning(f"Error disposing MySQL connection: {e}") 
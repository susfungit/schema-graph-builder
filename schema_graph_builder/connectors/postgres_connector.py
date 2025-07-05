"""
PostgreSQL database connector
"""

import os
import yaml
import logging
from sqlalchemy import create_engine, inspect
from typing import Dict, Any

logger = logging.getLogger(__name__)


def get_postgres_schema(config_path: str) -> Dict[str, Any]:
    """
    Extract schema from PostgreSQL database.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Dictionary containing schema information
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Support environment variables for credentials
    username = config.get('username') or os.getenv('DB_USERNAME')
    password = config.get('password') or os.getenv('DB_PASSWORD')
    host = config.get('host', 'localhost')
    port = config.get('port', 5432)
    database = config.get('database')
    
    if not username or not password:
        raise ValueError("Database credentials not found in config or environment variables")
    
    # Create database URL
    db_url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    
    # Log connection (with masked credentials)
    masked_url = f"postgresql://{username}:***@{host}:{port}/{database}"
    logger.info(f"Connecting to PostgreSQL: {masked_url}")
    
    # Create engine and inspector
    engine = create_engine(db_url)
    inspector = inspect(engine)
    
    # Extract schema
    schema = {
        'database': database,
        'tables': []
    }
    
    try:
        for table_name in inspector.get_table_names():
            table_info = {
                'name': table_name,
                'columns': []
            }
            
            # Get column information
            columns = inspector.get_columns(table_name)
            pk_constraint = inspector.get_pk_constraint(table_name)
            pk_columns = pk_constraint.get('constrained_columns', [])
            
            for column in columns:
                column_info = {
                    'name': column['name'],
                    'type': str(column['type']),
                    'nullable': column['nullable'],
                    'primary_key': column['name'] in pk_columns
                }
                table_info['columns'].append(column_info)
            
            schema['tables'].append(table_info)
            
        logger.info(f"Successfully extracted schema: {len(schema['tables'])} tables")
        return schema
        
    finally:
        # Ensure connection is closed
        engine.dispose() 
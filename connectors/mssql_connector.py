"""
MS SQL Server database connector
"""

import yaml
from sqlalchemy import create_engine, inspect
from typing import Dict, Any


def get_mssql_schema(config_path: str) -> Dict[str, Any]:
    """
    Extract schema from MS SQL Server database.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Dictionary containing schema information
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Create database URL with ODBC driver
    db_url = f"mssql+pyodbc://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    
    # Create engine and inspector
    engine = create_engine(db_url)
    inspector = inspect(engine)
    
    # Extract schema
    schema = {
        'database': config['database'],
        'tables': []
    }
    
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
    
    return schema 
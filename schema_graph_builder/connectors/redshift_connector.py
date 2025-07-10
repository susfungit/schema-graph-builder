"""
Amazon Redshift database connector

This module provides connectivity to Amazon Redshift data warehouses.
Redshift is based on PostgreSQL but has specific requirements for SSL connections
and supports additional metadata like distribution keys and sort keys.
"""

from typing import Dict, Any
from .base_connector import DatabaseConnector


class RedshiftConnector(DatabaseConnector):
    """Amazon Redshift database connector implementation."""
    
    def __init__(self):
        """Initialize Redshift connector with default port 5439."""
        super().__init__('redshift', 5439)
    
    def _get_db_specific_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get Redshift-specific connection parameters.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Dictionary with Redshift-specific parameters
        """
        params = {}
        
        # SSL is required for Redshift connections
        params['sslmode'] = config.get('ssl_mode', 'require')
        
        # Optional AWS-specific parameters
        if 'region' in config:
            params['region'] = config['region']
            
        if 'cluster_type' in config:
            params['cluster_type'] = config['cluster_type']
            
        return params
    
    def _get_connect_args(self) -> Dict[str, Any]:
        """
        Get Redshift-specific connection arguments.
        
        Returns:
            Dictionary with connection arguments optimized for Redshift
        """
        return {
            "connect_timeout": 60,  # Redshift may need more time for cluster wake-up
            "sslmode": "require",   # Always require SSL for Redshift
            "options": "-c default_transaction_isolation=read_committed -c search_path=public",
            # Redshift-specific optimizations
            "keepalives_idle": 600,
            "keepalives_interval": 30,
            "keepalives_count": 3
        }
    
    def _extract_schema_data(self, database: str) -> Dict[str, Any]:
        """
        Extract schema data with Redshift-specific enhancements.
        
        This method extends the base schema extraction to include
        Redshift-specific metadata like distribution keys and sort keys.
        
        Args:
            database: Database name
            
        Returns:
            Dictionary containing schema information with Redshift metadata
        """
        # Get the base schema data first
        schema = super()._extract_schema_data(database)
        
        # Add Redshift-specific metadata extraction
        try:
            self._add_redshift_metadata(schema)
        except Exception as e:
            # Log warning but don't fail the entire extraction
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to extract Redshift-specific metadata: {e}")
        
        return schema
    
    def _add_redshift_metadata(self, schema: Dict[str, Any]) -> None:
        """
        Add Redshift-specific metadata to schema information.
        
        This includes distribution keys, sort keys, and column encodings.
        
        Args:
            schema: Schema dictionary to enhance
        """
        if not self.engine:
            return
            
        # Query for Redshift-specific table information
        redshift_metadata_query = """
        SELECT 
            schemaname,
            tablename,
            column,
            distkey,
            sortkey,
            encoding
        FROM pg_catalog.svv_table_info
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schemaname, tablename, sortkey NULLS LAST
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(redshift_metadata_query)
                
                # Build lookup dictionaries for efficient access
                table_metadata = {}
                
                for row in result:
                    schema_name = row[0] if row[0] else 'public'
                    table_name = row[1]
                    column_name = row[2]
                    
                    table_key = f"{schema_name}.{table_name}"
                    
                    if table_key not in table_metadata:
                        table_metadata[table_key] = {
                            'distribution_key': None,
                            'sort_keys': [],
                            'column_encodings': {}
                        }
                    
                    # Distribution key (only one per table)
                    if row[3]:  # distkey
                        table_metadata[table_key]['distribution_key'] = column_name
                    
                    # Sort keys (can be multiple, ordered by sortkey number)
                    if row[4]:  # sortkey
                        table_metadata[table_key]['sort_keys'].append({
                            'column': column_name,
                            'sort_order': row[4]
                        })
                    
                    # Column encodings
                    if row[5]:  # encoding
                        table_metadata[table_key]['column_encodings'][column_name] = row[5]
                
                # Add metadata to schema tables
                for table in schema.get('tables', []):
                    table_name = table['name']
                    # Assume public schema if not specified
                    table_key = f"public.{table_name}"
                    
                    if table_key in table_metadata:
                        metadata = table_metadata[table_key]
                        
                        # Add Redshift-specific metadata
                        table['redshift_metadata'] = {
                            'distribution_key': metadata['distribution_key'],
                            'sort_keys': sorted(metadata['sort_keys'], key=lambda x: x['sort_order']),
                            'column_encodings': metadata['column_encodings']
                        }
        
        except Exception as e:
            # Re-raise with more context
            raise Exception(f"Failed to query Redshift metadata: {e}")


def get_redshift_schema(config_path: str) -> Dict[str, Any]:
    """
    Extract schema from Amazon Redshift database.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        Dictionary containing schema information with Redshift-specific metadata
        
    Example:
        >>> schema = get_redshift_schema('config/redshift_db_connections.yaml')
        >>> print(f"Found {len(schema['tables'])} tables")
        >>> 
        >>> # Access Redshift-specific metadata
        >>> for table in schema['tables']:
        ...     redshift_meta = table.get('redshift_metadata', {})
        ...     if redshift_meta.get('distribution_key'):
        ...         print(f"Table {table['name']} uses distribution key: {redshift_meta['distribution_key']}")
    """
    connector = RedshiftConnector()
    return connector.extract_schema(config_path) 
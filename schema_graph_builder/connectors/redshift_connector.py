"""
Amazon Redshift database connector

This module provides connectivity to Amazon Redshift data warehouses.
Redshift is based on PostgreSQL but has specific requirements for SSL connections
and supports additional metadata like distribution keys and sort keys.
"""

from typing import Dict, Any, List
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
            
        # Serverless-specific parameters
        if 'workgroup' in config:
            params['workgroup'] = config['workgroup']
            
        if 'database_user' in config:
            params['database_user'] = config['database_user']
            
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
            # Detect cluster type first
            cluster_type = self._detect_cluster_type()
            schema['cluster_type'] = cluster_type
            
            # Add comprehensive Redshift metadata
            self._add_redshift_metadata(schema)
            
            # Add external table metadata (Redshift Spectrum)
            self._add_external_table_metadata(schema)
            
        except Exception as e:
            # Log warning but don't fail the entire extraction
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to extract Redshift-specific metadata: {e}")
        
        return schema
    
    def _add_redshift_metadata(self, schema: Dict[str, Any]) -> None:
        """
        Add comprehensive Redshift-specific metadata to schema information.
        
        This includes distribution keys, sort keys, column encodings, table statistics,
        external tables, and advanced Redshift features.
        
        Args:
            schema: Schema dictionary to enhance
        """
        if not self.engine:
            return
            
        # Query for comprehensive Redshift table metadata
        redshift_metadata_query = """
        SELECT 
            sti.schemaname,
            sti.tablename,
            sti.column,
            sti.distkey,
            sti.sortkey,
            sti.encoding,
            sti.type,
            sti.notnull,
            sti.diststyle,
            sti.sortkey1,
            sti.sortkey_num,
            sti.size,
            sti.pct_used,
            sti.empty,
            sti.unsorted,
            sti.stats_off,
            sti.tbl_rows,
            sti.skew_sortkey1,
            sti.skew_rows,
            -- Additional table-level information
            c.relkind,
            c.relhasoids,
            c.reltablespace,
            CASE 
                WHEN c.relkind = 'r' THEN 'TABLE'
                WHEN c.relkind = 'v' THEN 'VIEW'
                WHEN c.relkind = 'S' THEN 'SEQUENCE'
                WHEN c.relkind = 'f' THEN 'EXTERNAL_TABLE'
                ELSE 'OTHER'
            END as table_type
        FROM pg_catalog.svv_table_info sti
        LEFT JOIN pg_catalog.pg_class c ON c.relname = sti.tablename
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname = sti.schemaname
        WHERE sti.schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1')
        ORDER BY sti.schemaname, sti.tablename, sti.sortkey NULLS LAST
        """
        
        try:
            # Get sort key types and dependencies
            sort_key_types = self._detect_sort_key_types()
            table_dependencies = self._get_table_dependencies()
            
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
                            'distribution_style': None,
                            'sort_keys': [],
                            'sort_key_type': None,
                            'column_encodings': {},
                            'table_statistics': {},
                            'table_type': 'TABLE',
                            'is_external': False,
                            'columns_metadata': {},
                            'dependencies': []
                        }
                    
                    # Distribution key (only one per table)
                    if row[3]:  # distkey
                        table_metadata[table_key]['distribution_key'] = column_name
                    
                    # Distribution style
                    if row[8]:  # diststyle
                        table_metadata[table_key]['distribution_style'] = row[8]
                    
                    # Sort keys (can be multiple, ordered by sortkey number)
                    if row[4]:  # sortkey
                        sort_key_info = {
                            'column': column_name,
                            'sort_order': row[4],
                            'sort_key_num': row[10] if row[10] else None
                        }
                        table_metadata[table_key]['sort_keys'].append(sort_key_info)
                    
                    # Sort key type (compound vs interleaved)
                    if row[9]:  # sortkey1 - indicates primary sort key
                        # Use detected sort key type
                        table_metadata[table_key]['sort_key_type'] = sort_key_types.get(table_key, 'compound')
                    
                    # Column encodings
                    if row[5]:  # encoding
                        table_metadata[table_key]['column_encodings'][column_name] = row[5]
                    
                    # Column-level metadata
                    table_metadata[table_key]['columns_metadata'][column_name] = {
                        'type': row[6] if row[6] else None,
                        'not_null': row[7] if row[7] else False,
                        'encoding': row[5] if row[5] else None
                    }
                    
                    # Table-level statistics (only set once per table)
                    if not table_metadata[table_key]['table_statistics']:
                        table_metadata[table_key]['table_statistics'] = {
                            'size_mb': row[11] if row[11] else 0,
                            'pct_used': row[12] if row[12] else 0,
                            'is_empty': row[13] if row[13] else False,
                            'unsorted_pct': row[14] if row[14] else 0,
                            'stats_off_pct': row[15] if row[15] else 0,
                            'estimated_rows': row[16] if row[16] else 0,
                            'sortkey1_skew': row[17] if row[17] else 0,
                            'row_skew': row[18] if row[18] else 0
                        }
                    
                    # Table type and external table detection
                    if row[22]:  # table_type
                        table_metadata[table_key]['table_type'] = row[22]
                        table_metadata[table_key]['is_external'] = (row[22] == 'EXTERNAL_TABLE')
                    
                    # Add dependencies for this table
                    if table_key in table_dependencies:
                        table_metadata[table_key]['dependencies'] = table_dependencies[table_key]
                
                # Add metadata to schema tables
                for table in schema.get('tables', []):
                    table_name = table['name']
                    # Assume public schema if not specified
                    table_key = f"public.{table_name}"
                    
                    if table_key in table_metadata:
                        metadata = table_metadata[table_key]
                        
                        # Add comprehensive Redshift-specific metadata
                        table['redshift_metadata'] = {
                            # Distribution information
                            'distribution_key': metadata['distribution_key'],
                            'distribution_style': metadata['distribution_style'],
                            
                            # Sort key information
                            'sort_keys': sorted(metadata['sort_keys'], key=lambda x: x['sort_order']),
                            'sort_key_type': metadata['sort_key_type'],
                            
                            # Column encodings
                            'column_encodings': metadata['column_encodings'],
                            
                            # Table type and external table info
                            'table_type': metadata['table_type'],
                            'is_external': metadata['is_external'],
                            
                            # Table statistics and performance metrics
                            'statistics': metadata['table_statistics'],
                            
                            # Column-level metadata
                            'columns_metadata': metadata['columns_metadata'],
                            
                            # Table dependencies
                            'dependencies': metadata['dependencies']
                        }
                        
                        # Add performance recommendations based on statistics
                        recommendations = self._generate_performance_recommendations(metadata)
                        if recommendations:
                            table['redshift_metadata']['recommendations'] = recommendations
        
        except Exception as e:
            # Re-raise with more context
            raise Exception(f"Failed to query Redshift metadata: {e}")
    
    def _generate_performance_recommendations(self, metadata: Dict[str, Any]) -> List[str]:
        """
        Generate performance recommendations based on table statistics.
        
        Args:
            metadata: Table metadata dictionary
            
        Returns:
            List of performance recommendations
        """
        recommendations = []
        stats = metadata.get('table_statistics', {})
        
        # Check for high unsorted percentage
        unsorted_pct = stats.get('unsorted_pct', 0)
        if unsorted_pct > 20:
            recommendations.append(f"Consider running VACUUM: {unsorted_pct}% of table is unsorted")
        
        # Check for outdated statistics
        stats_off_pct = stats.get('stats_off_pct', 0)
        if stats_off_pct > 10:
            recommendations.append(f"Consider running ANALYZE: {stats_off_pct}% of statistics are outdated")
        
        # Check for high sort key skew
        sortkey1_skew = stats.get('sortkey1_skew', 0)
        if sortkey1_skew > 4:
            recommendations.append(f"High sort key skew detected ({sortkey1_skew}): consider choosing a different sort key")
        
        # Check for high row skew
        row_skew = stats.get('row_skew', 0)
        if row_skew > 4:
            recommendations.append(f"High row skew detected ({row_skew}): consider choosing a different distribution key")
        
        # Check for small tables that might benefit from DISTSTYLE ALL
        size_mb = stats.get('size_mb', 0)
        if size_mb < 100 and metadata.get('distribution_style') not in ['ALL', 'EVEN']:
            recommendations.append("Small table: consider using DISTSTYLE ALL for better performance")
        
        # Check for empty tables
        if stats.get('is_empty', False):
            recommendations.append("Table is empty: consider loading data or dropping if unused")
        
        # Check for low disk usage
        pct_used = stats.get('pct_used', 0)
        if pct_used < 50 and size_mb > 1000:
            recommendations.append(f"Low disk utilization ({pct_used}%): consider compression or archiving old data")
        
        return recommendations
    
    def _add_external_table_metadata(self, schema: Dict[str, Any]) -> None:
        """
        Add metadata for Redshift Spectrum external tables.
        
        Args:
            schema: Schema dictionary to enhance
        """
        if not self.engine:
            return
            
        # Query for external table information
        external_tables_query = """
        SELECT 
            schemaname,
            tablename,
            location,
            input_format,
            output_format,
            serialization_lib,
            serde_parameters,
            compressed,
            parameters
        FROM svv_external_tables
        WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY schemaname, tablename
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(external_tables_query)
                
                # Build lookup dictionary for external tables
                external_metadata = {}
                
                for row in result:
                    schema_name = row[0] if row[0] else 'public'
                    table_name = row[1]
                    table_key = f"{schema_name}.{table_name}"
                    
                    external_metadata[table_key] = {
                        'location': row[2],
                        'input_format': row[3],
                        'output_format': row[4],
                        'serialization_lib': row[5],
                        'serde_parameters': row[6],
                        'compressed': row[7],
                        'parameters': row[8]
                    }
                
                # Add external table metadata to schema tables
                for table in schema.get('tables', []):
                    table_name = table['name']
                    table_key = f"public.{table_name}"
                    
                    if table_key in external_metadata:
                        if 'redshift_metadata' not in table:
                            table['redshift_metadata'] = {}
                        
                        table['redshift_metadata']['external_table'] = external_metadata[table_key]
                        table['redshift_metadata']['is_external'] = True
                        
        except Exception as e:
            # Log warning but don't fail the entire extraction
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to extract external table metadata: {e}")
    
    def _detect_cluster_type(self) -> str:
        """
        Detect if this is a Redshift Serverless or Provisioned cluster.
        
        Returns:
            'serverless' or 'provisioned'
        """
        try:
            with self.engine.connect() as conn:
                # Query to detect cluster type
                result = conn.execute("SELECT version()")
                version_info = result.fetchone()[0]
                
                # Redshift Serverless has different version string patterns
                if 'serverless' in version_info.lower():
                    return 'serverless'
                else:
                    return 'provisioned'
        except Exception:
            # Default to provisioned if we can't determine
            return 'provisioned'
    
    def _detect_sort_key_types(self) -> Dict[str, str]:
        """
        Detect sort key types (compound vs interleaved) for all tables.
        
        Returns:
            Dictionary mapping table names to sort key types
        """
        sort_key_types = {}
        
        try:
            with self.engine.connect() as conn:
                # Query to get sort key information from pg_class
                sort_key_query = """
                SELECT 
                    n.nspname as schema_name,
                    c.relname as table_name,
                    CASE 
                        WHEN c.relinterleaved = 't' THEN 'interleaved'
                        WHEN c.relisreplicated = 't' THEN 'compound'
                        ELSE 'compound'
                    END as sort_key_type
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'r'
                AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                AND c.relhassubclass = 'f'
                ORDER BY n.nspname, c.relname
                """
                
                result = conn.execute(sort_key_query)
                
                for row in result:
                    schema_name = row[0]
                    table_name = row[1]
                    sort_key_type = row[2]
                    
                    table_key = f"{schema_name}.{table_name}"
                    sort_key_types[table_key] = sort_key_type
                    
        except Exception as e:
            # Log warning but continue
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to detect sort key types: {e}")
        
        return sort_key_types
    
    def _get_table_dependencies(self) -> Dict[str, List[str]]:
        """
        Get table dependencies for Redshift Spectrum external tables.
        
        Returns:
            Dictionary mapping table names to their dependencies
        """
        dependencies = {}
        
        try:
            with self.engine.connect() as conn:
                # Query for table dependencies (views, external tables)
                dependencies_query = """
                SELECT DISTINCT
                    dependent_ns.nspname as dependent_schema,
                    dependent_view.relname as dependent_table,
                    source_ns.nspname as source_schema,
                    source_table.relname as source_table
                FROM pg_depend
                JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
                JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
                JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
                JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
                JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
                WHERE source_table.relkind in ('r', 'v', 'f')
                AND dependent_ns.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                ORDER BY dependent_schema, dependent_table
                """
                
                result = conn.execute(dependencies_query)
                
                for row in result:
                    dependent_table = f"{row[0]}.{row[1]}"
                    source_table = f"{row[2]}.{row[3]}"
                    
                    if dependent_table not in dependencies:
                        dependencies[dependent_table] = []
                    
                    dependencies[dependent_table].append(source_table)
                    
        except Exception as e:
            # Log warning but continue
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to get table dependencies: {e}")
        
        return dependencies


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
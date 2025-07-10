"""
Sybase/SAP ASE (Adaptive Server Enterprise) database connector

This module provides connectivity to Sybase and SAP ASE databases using the TDS protocol.
Sybase is commonly used in enterprise environments, especially legacy systems and
financial services organizations.
"""

from typing import Dict, Any
from .base_connector import DatabaseConnector


class SybaseConnector(DatabaseConnector):
    """Sybase/SAP ASE database connector implementation."""
    
    def __init__(self):
        """Initialize Sybase connector with default port 5000."""
        super().__init__('sybase', 5000)
    
    def _get_db_specific_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get Sybase-specific connection parameters.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Dictionary with Sybase-specific parameters
        """
        params = {}
        
        # Character set encoding (important for Sybase)
        params['charset'] = config.get('charset', 'utf8')
        
        # TDS version (important for compatibility)
        params['tds_version'] = config.get('tds_version', '7.0')
        
        # Application name for connection tracking
        params['appname'] = config.get('appname', 'schema-graph-builder')
        
        # Login timeout
        params['login_timeout'] = config.get('login_timeout', 30)
        
        # Network packet size
        if 'packet_size' in config:
            params['packet_size'] = config['packet_size']
            
        # Auto-commit mode
        params['autocommit'] = config.get('autocommit', True)
        
        return params
    
    def _get_connect_args(self) -> Dict[str, Any]:
        """
        Get Sybase-specific connection arguments.
        
        Returns:
            Dictionary with connection arguments optimized for Sybase
        """
        return {
            "timeout": 30,
            "login_timeout": 30,
            "charset": "utf8",
            "tds_version": "7.0",
            "appname": "schema-graph-builder",
            "autocommit": True
        }
    
    def _build_connection_string(self, config: Dict[str, Any]) -> str:
        """
        Build Sybase connection string using pymssql format.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Formatted connection string for Sybase
        """
        # Get database-specific parameters
        db_params = self._get_db_specific_params(config)
        
        # Build the connection string for pymssql
        # Format: mssql+pymssql://username:password@host:port/database
        connection_string = (
            f"mssql+pymssql://{config['username']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['database']}"
        )
        
        # Add query parameters
        query_params = []
        
        # Character set
        if 'charset' in db_params:
            query_params.append(f"charset={db_params['charset']}")
            
        # TDS version
        if 'tds_version' in db_params:
            query_params.append(f"tds_version={db_params['tds_version']}")
            
        # Application name
        if 'appname' in db_params:
            query_params.append(f"appname={db_params['appname']}")
            
        # Login timeout
        if 'login_timeout' in db_params:
            query_params.append(f"login_timeout={db_params['login_timeout']}")
            
        # Network packet size
        if 'packet_size' in db_params:
            query_params.append(f"packet_size={db_params['packet_size']}")
        
        # Append query parameters if any
        if query_params:
            connection_string += "?" + "&".join(query_params)
            
        return connection_string
    
    def _extract_schema_data(self, database: str) -> Dict[str, Any]:
        """
        Extract schema data with Sybase-specific enhancements.
        
        Args:
            database: Database name
            
        Returns:
            Dictionary containing schema information with Sybase metadata
        """
        # Get the base schema data first
        schema = super()._extract_schema_data(database)
        
        # Add Sybase-specific metadata extraction
        try:
            self._add_sybase_metadata(schema)
        except Exception as e:
            # Log warning but don't fail the entire extraction
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to extract Sybase-specific metadata: {e}")
        
        # Add Sybase server information
        try:
            schema['server_version'] = self._get_sybase_version()
            schema['available_databases'] = self._get_sybase_databases()
        except Exception as e:
            # Log warning but don't fail the entire extraction
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to extract Sybase server information: {e}")
            schema['server_version'] = "Unknown"
            schema['available_databases'] = []
        
        return schema
    
    def _add_sybase_metadata(self, schema: Dict[str, Any]) -> None:
        """
        Add Sybase-specific metadata to schema information.
        
        This includes indexes, constraints, user-defined data types,
        and other Sybase-specific features.
        
        Args:
            schema: Schema dictionary to enhance
        """
        if not self.engine:
            return
            
        # Query for Sybase-specific metadata
        sybase_metadata_query = """
        SELECT 
            u.name as table_name,
            c.name as column_name,
            t.name as data_type,
            c.length as column_length,
            c.prec as precision_val,
            c.scale as scale_val,
            c.status as column_status,
            CASE WHEN c.status & 8 = 8 THEN 1 ELSE 0 END as is_identity,
            CASE WHEN c.status & 128 = 128 THEN 1 ELSE 0 END as is_nullable,
            c.cdefault as default_id,
            d.text as default_value
        FROM sysobjects u
        JOIN syscolumns c ON u.id = c.id
        JOIN systypes t ON c.usertype = t.usertype
        LEFT JOIN syscomments d ON c.cdefault = d.id
        WHERE u.type = 'U'  -- User tables only
        ORDER BY u.name, c.colid
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(sybase_metadata_query)
                
                # Build metadata lookup
                table_metadata = {}
                
                for row in result:
                    table_name = row[0]
                    column_name = row[1]
                    
                    if table_name not in table_metadata:
                        table_metadata[table_name] = {
                            'columns': {},
                            'indexes': [],
                            'constraints': []
                        }
                    
                    # Store column metadata
                    table_metadata[table_name]['columns'][column_name] = {
                        'data_type': row[2],
                        'length': row[3],
                        'precision': row[4],
                        'scale': row[5],
                        'is_identity': bool(row[7]),
                        'is_nullable': bool(row[8]),
                        'default_value': row[10] if row[10] else None
                    }
                
                # Add metadata to schema tables
                for table in schema.get('tables', []):
                    table_name = table['name']
                    
                    if table_name in table_metadata:
                        metadata = table_metadata[table_name]
                        
                        # Add Sybase-specific metadata
                        table['sybase_metadata'] = {
                            'columns': metadata['columns'],
                            'database_engine': 'sybase',
                            'has_identity_columns': any(
                                col_meta['is_identity'] 
                                for col_meta in metadata['columns'].values()
                            )
                        }
                        
                        # Add identity column information to the table
                        identity_columns = [
                            col_name for col_name, col_meta in metadata['columns'].items()
                            if col_meta['is_identity']
                        ]
                        
                        if identity_columns:
                            table['sybase_metadata']['identity_columns'] = identity_columns
                            
        except Exception as e:
            # Log the error but continue
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error extracting Sybase metadata: {e}")
    
    def _get_sybase_version(self) -> str:
        """
        Get Sybase server version information.
        
        Returns:
            Sybase server version string
        """
        if not self.engine:
            return "Unknown"
            
        try:
            with self.engine.connect() as conn:
                result = conn.execute("SELECT @@version")
                version_info = result.fetchone()
                return version_info[0] if version_info else "Unknown"
        except Exception:
            return "Unknown"
    
    def _get_sybase_databases(self) -> list:
        """
        Get list of databases on the Sybase server.
        
        Returns:
            List of database names
        """
        if not self.engine:
            return []
            
        try:
            with self.engine.connect() as conn:
                result = conn.execute("SELECT name FROM master..sysdatabases WHERE name NOT IN ('master', 'model', 'tempdb', 'sybsystemprocs')")
                return [row[0] for row in result]
        except Exception:
            return []


def get_sybase_schema(config_path: str) -> Dict[str, Any]:
    """
    Extract schema information from a Sybase database.
    
    Args:
        config_path: Path to the YAML configuration file
        
    Returns:
        Dictionary containing schema information
        
    Raises:
        Exception: If connection fails or schema extraction fails
    """
    connector = SybaseConnector()
    return connector.extract_schema(config_path) 
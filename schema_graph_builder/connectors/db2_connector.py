"""
IBM DB2 database connector

This module provides connectivity to IBM DB2 databases (LUW and z/OS) using the ibm_db driver.
DB2 is widely used in enterprise environments, especially financial services, government,
and large corporations with mainframe systems.
"""

from typing import Dict, Any
from .base_connector import DatabaseConnector


class DB2Connector(DatabaseConnector):
    """IBM DB2 database connector implementation."""
    
    def __init__(self):
        """Initialize DB2 connector with default port 50000."""
        super().__init__('db2', 50000)
    
    def _get_db_specific_params(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get DB2-specific connection parameters.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Dictionary with DB2-specific parameters
        """
        params = {}
        
        # Protocol (TCPIP is most common)
        params['protocol'] = config.get('protocol', 'TCPIP')
        
        # Security method
        params['security'] = config.get('security', 'SSL')
        
        # Connection timeout
        params['connect_timeout'] = config.get('connect_timeout', 30)
        
        # Current schema (important for DB2)
        if 'current_schema' in config:
            params['current_schema'] = config['current_schema']
            
        # Authentication type
        params['authentication'] = config.get('authentication', 'SERVER')
        
        # Character set/codepage
        if 'codepage' in config:
            params['codepage'] = config['codepage']
            
        # Application name for connection tracking
        params['application_name'] = config.get('application_name', 'schema-graph-builder')
        
        # Workstation name
        if 'workstation_name' in config:
            params['workstation_name'] = config['workstation_name']
            
        # For z/OS environments
        if config.get('environment') == 'zos':
            params['environment'] = 'zos'
            
        # Location name for z/OS (or general location parameter)
        if 'location' in config:
            params['location'] = config['location']
        
        return params
    
    def _get_connect_args(self) -> Dict[str, Any]:
        """
        Get DB2-specific connection arguments.
        
        Returns:
            Dictionary with connection arguments optimized for DB2
        """
        return {
            "autocommit": True,
            "connecttimeout": 30,
            "querytimeout": 300  # 5 minutes for complex queries
        }
    
    def _build_connection_string(self, config: Dict[str, Any]) -> str:
        """
        Build DB2 connection string using ibm_db format.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Formatted connection string for DB2
        """
        # Get database-specific parameters
        db_params = self._get_db_specific_params(config)
        
        # Build the connection string for ibm_db
        # Format: ibm_db_sa://username:password@hostname:port/database
        connection_string = (
            f"ibm_db_sa://{config['username']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['database']}"
        )
        
        # Add query parameters
        query_params = []
        
        # Protocol
        if 'protocol' in db_params:
            query_params.append(f"protocol={db_params['protocol']}")
            
        # Security
        if 'security' in db_params:
            query_params.append(f"security={db_params['security']}")
            
        # Current schema
        if 'current_schema' in db_params:
            query_params.append(f"currentschema={db_params['current_schema']}")
            
        # Authentication
        if 'authentication' in db_params:
            query_params.append(f"authentication={db_params['authentication']}")
            
        # Application name
        if 'application_name' in db_params:
            query_params.append(f"applicationname={db_params['application_name']}")
            
        # Connection timeout
        if 'connect_timeout' in db_params:
            query_params.append(f"connecttimeout={db_params['connect_timeout']}")
            
        # Codepage
        if 'codepage' in db_params:
            query_params.append(f"codepage={db_params['codepage']}")
            
        # Location parameter (z/OS or general)
        if 'location' in db_params:
            query_params.append(f"location={db_params['location']}")
        
        # Append query parameters if any
        if query_params:
            connection_string += "?" + "&".join(query_params)
            
        return connection_string
    
    def _extract_schema_data(self, database: str) -> Dict[str, Any]:
        """
        Extract schema data with DB2-specific enhancements.
        
        Args:
            database: Database name
            
        Returns:
            Dictionary containing schema information with DB2 metadata
        """
        # Get the base schema data first
        schema = super()._extract_schema_data(database)
        
        # Add DB2-specific metadata extraction
        try:
            self._add_db2_metadata(schema)
        except Exception as e:
            # Log warning but don't fail the entire extraction
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to extract DB2-specific metadata: {e}")
        
        # Add DB2 server information
        try:
            schema['server_version'] = self._get_db2_version()
            schema['server_info'] = self._get_db2_server_info()
            schema['tablespaces'] = self._get_db2_tablespaces()
            schema['schemas'] = self._get_db2_schemas()
        except Exception as e:
            # Log warning but don't fail the entire extraction
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to extract DB2 server information: {e}")
            schema['server_version'] = "Unknown"
            schema['server_info'] = {}
            schema['tablespaces'] = []
            schema['schemas'] = []
        
        return schema
    
    def _add_db2_metadata(self, schema: Dict[str, Any]) -> None:
        """
        Add DB2-specific metadata to schema information.
        
        This includes tablespaces, buffer pools, indexes, constraints,
        and other DB2-specific features.
        
        Args:
            schema: Schema dictionary to enhance
        """
        if not self.engine:
            return
            
        # Query for DB2-specific metadata using system catalog tables
        db2_metadata_query = """
        SELECT 
            t.tabschema,
            t.tabname,
            c.colname,
            c.typename,
            c.length,
            c.scale,
            c.nulls,
            c.default,
            c.identity,
            c.generated,
            t.tbspace,
            t.card as row_count,
            t.npages,
            t.fpages
        FROM syscat.tables t
        JOIN syscat.columns c ON t.tabschema = c.tabschema AND t.tabname = c.tabname
        WHERE t.type = 'T'  -- Regular tables only
        AND t.tabschema NOT LIKE 'SYS%'  -- Exclude system schemas
        AND t.tabschema NOT IN ('INFORMATION_SCHEMA', 'SYSIBM', 'SYSIBMADM', 'SYSTOOLS')
        ORDER BY t.tabschema, t.tabname, c.colno
        """
        
        try:
            with self.engine.connect() as conn:
                from sqlalchemy import text
                result = conn.execute(text(db2_metadata_query))
                
                # Build metadata lookup
                table_metadata = {}
                
                for row in result:
                    schema_name = row[0]
                    table_name = row[1]
                    column_name = row[2]
                    full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
                    
                    if full_table_name not in table_metadata:
                        table_metadata[full_table_name] = {
                            'schema': schema_name,
                            'tablespace': row[10],
                            'row_count': row[11],
                            'data_pages': row[12],
                            'used_pages': row[13],
                            'columns': {},
                            'indexes': [],
                            'constraints': []
                        }
                    
                    # Store column metadata
                    table_metadata[full_table_name]['columns'][column_name] = {
                        'data_type': row[3],
                        'length': row[4],
                        'scale': row[5],
                        'nullable': row[6] == 'Y',
                        'default_value': row[7],
                        'is_identity': row[8] == 'Y',
                        'is_generated': row[9] in ('A', 'D')  # Always or Default generated
                    }
                
                # Add metadata to schema tables
                for table in schema.get('tables', []):
                    table_name = table['name']
                    
                    # Try both simple name and schema-qualified name  
                    # Also try case-insensitive matches
                    metadata = None
                    for key in table_metadata:
                        key_lower = key.lower()
                        table_name_lower = table_name.lower()
                        
                        if (key_lower.endswith(f".{table_name_lower}") or 
                            key_lower == table_name_lower or
                            key_lower.endswith(f".{table_name_lower}s") or  # Try plural
                            key_lower == f"{table_name_lower}s"):
                            metadata = table_metadata[key]
                            break
                    
                    if metadata:
                        # Add DB2-specific metadata
                        table['db2_metadata'] = {
                            'schema': metadata['schema'],
                            'tablespace': metadata['tablespace'],
                            'statistics': {
                                'row_count': metadata['row_count'],
                                'data_pages': metadata['data_pages'],
                                'used_pages': metadata['used_pages']
                            },
                            'columns': metadata['columns'],
                            'database_engine': 'db2',
                            'has_identity_columns': any(
                                col_meta['is_identity'] 
                                for col_meta in metadata['columns'].values()
                            ),
                            'has_generated_columns': any(
                                col_meta['is_generated'] 
                                for col_meta in metadata['columns'].values()
                            )
                        }
                        
                        # Add identity column information
                        identity_columns = [
                            col_name for col_name, col_meta in metadata['columns'].items()
                            if col_meta['is_identity']
                        ]
                        
                        if identity_columns:
                            table['db2_metadata']['identity_columns'] = identity_columns
                            
                        # Add generated column information
                        generated_columns = [
                            col_name for col_name, col_meta in metadata['columns'].items()
                            if col_meta['is_generated']
                        ]
                        
                        if generated_columns:
                            table['db2_metadata']['generated_columns'] = generated_columns
                            
        except Exception as e:
            # Log the error but continue
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error extracting DB2 metadata: {e}")
    
    def _get_db2_version(self) -> str:
        """
        Get DB2 server version information.
        
        Returns:
            DB2 server version string
        """
        if not self.engine:
            return "Unknown"
            
        try:
            with self.engine.connect() as conn:
                from sqlalchemy import text
                result = conn.execute(text("SELECT service_level, fixpack_num FROM TABLE(sysproc.env_get_inst_info())"))
                version_info = result.fetchone()
                if version_info:
                    return f"DB2 {version_info[0]} FixPack {version_info[1]}"
                return "Unknown"
        except Exception:
            # Fallback to simpler version query
            try:
                with self.engine.connect() as conn:
                    from sqlalchemy import text
                    result = conn.execute(text("VALUES (current server)"))
                    server_info = result.fetchone()
                    return f"DB2 Server: {server_info[0]}" if server_info else "Unknown"
            except Exception:
                return "Unknown"
    
    def _get_db2_server_info(self) -> Dict[str, Any]:
        """
        Get DB2 server configuration information.
        
        Returns:
            Dictionary with server configuration details
        """
        if not self.engine:
            return {}
            
        try:
            with self.engine.connect() as conn:
                from sqlalchemy import text
                # Get basic server information
                result = conn.execute(text("""
                    SELECT 
                        current server as server_name,
                        current timezone as timezone,
                        current date as current_date,
                        current time as current_time
                """))
                
                info = result.fetchone()
                if info:
                    return {
                        'server_name': info[0],
                        'timezone': info[1],
                        'current_date': str(info[2]),
                        'current_time': str(info[3])
                    }
                    
        except Exception:
            pass
            
        return {}
    
    def _get_db2_tablespaces(self) -> list:
        """
        Get list of tablespaces in the DB2 database.
        
        Returns:
            List of tablespace information
        """
        if not self.engine:
            return []
            
        try:
            with self.engine.connect() as conn:
                from sqlalchemy import text
                result = conn.execute(text("""
                    SELECT 
                        tbspace,
                        tbspacetype,
                        datatype,
                        pagesize,
                        totalPages,
                        usablePages,
                        usedPages
                    FROM syscat.tablespaces
                    WHERE tbspacetype IN ('DMS', 'SMS')
                    ORDER BY tbspace
                """))
                
                tablespaces = []
                for row in result:
                    tablespaces.append({
                        'name': row[0],
                        'type': row[1],
                        'data_type': row[2],
                        'page_size': row[3],
                        'total_pages': row[4],
                        'usable_pages': row[5],
                        'used_pages': row[6],
                        'utilization_percent': round((row[6] / row[5]) * 100, 2) if row[5] > 0 else 0
                    })
                
                return tablespaces
                
        except Exception:
            return []
    
    def _get_db2_schemas(self) -> list:
        """
        Get list of schemas in the DB2 database.
        
        Returns:
            List of schema names
        """
        if not self.engine:
            return []
            
        try:
            with self.engine.connect() as conn:
                from sqlalchemy import text
                result = conn.execute(text("""
                    SELECT schemaname, owner, definer, create_time
                    FROM syscat.schemata
                    WHERE schemaname NOT LIKE 'SYS%'
                    AND schemaname NOT IN ('INFORMATION_SCHEMA', 'SYSIBM', 'SYSIBMADM', 'SYSTOOLS')
                    ORDER BY schemaname
                """))
                
                schemas = []
                for row in result:
                    schemas.append({
                        'name': row[0],
                        'owner': row[1],
                        'definer': row[2],
                        'create_time': str(row[3]) if row[3] else None
                    })
                
                return schemas
                
        except Exception:
            return []


def get_db2_schema(config_path: str) -> Dict[str, Any]:
    """
    Extract schema information from an IBM DB2 database.
    
    Args:
        config_path: Path to the YAML configuration file
        
    Returns:
        Dictionary containing schema information
        
    Raises:
        Exception: If connection fails or schema extraction fails
    """
    connector = DB2Connector()
    return connector.extract_schema(config_path) 
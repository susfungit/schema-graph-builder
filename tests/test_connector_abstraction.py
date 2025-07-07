"""
Tests for the database connector abstraction layer
"""

import pytest
import tempfile
import os
from unittest.mock import patch, Mock, MagicMock
from schema_graph_builder.connectors.base_connector import DatabaseConnector
from schema_graph_builder.connectors.postgres_connector import PostgreSQLConnector
from schema_graph_builder.connectors.mysql_connector import MySQLConnector
from schema_graph_builder.connectors.mssql_connector import MSSQLConnector
from schema_graph_builder.extractor.schema_extractor import (
    extract_schema,
    get_supported_database_types,
    register_database_connector,
    DATABASE_CONNECTORS
)


class TestDatabaseConnectorAbstraction:
    """Test the database connector abstraction layer"""
    
    def test_connector_initialization(self):
        """Test that connectors are properly initialized"""
        postgres_connector = PostgreSQLConnector()
        assert postgres_connector.db_type == 'postgres'
        assert postgres_connector.default_port == 5432
        
        mysql_connector = MySQLConnector()
        assert mysql_connector.db_type == 'mysql'
        assert mysql_connector.default_port == 3306
        
        mssql_connector = MSSQLConnector()
        assert mssql_connector.db_type == 'mssql'
        assert mssql_connector.default_port == 1433
    
    def test_database_specific_parameters(self):
        """Test that database-specific parameters are correctly set"""
        postgres_connector = PostgreSQLConnector()
        postgres_params = postgres_connector._get_db_specific_params({})
        assert postgres_params == {}
        
        mysql_connector = MySQLConnector()
        mysql_params = mysql_connector._get_db_specific_params({})
        assert mysql_params == {}
        
        mssql_connector = MSSQLConnector()
        mssql_params = mssql_connector._get_db_specific_params({})
        assert 'driver' in mssql_params
        assert 'trust_server_certificate' in mssql_params
    
    def test_connection_arguments(self):
        """Test that connection arguments are correctly set"""
        postgres_connector = PostgreSQLConnector()
        postgres_args = postgres_connector._get_connect_args()
        assert 'connect_timeout' in postgres_args
        assert 'options' in postgres_args
        
        mysql_connector = MySQLConnector()
        mysql_args = mysql_connector._get_connect_args()
        assert 'connect_timeout' in mysql_args
        assert 'autocommit' in mysql_args
        
        mssql_connector = MSSQLConnector()
        mssql_args = mssql_connector._get_connect_args()
        assert 'timeout' in mssql_args
        assert 'autocommit' in mssql_args
    
    @patch('schema_graph_builder.connectors.base_connector.yaml.safe_load')
    @patch('builtins.open')
    def test_config_loading(self, mock_open, mock_yaml_load):
        """Test configuration loading"""
        mock_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'testdb',
            'username': 'user',
            'password': 'pass'
        }
        mock_yaml_load.return_value = mock_config
        
        connector = PostgreSQLConnector()
        config = connector._load_config('test_config.yaml')
        
        assert config == mock_config
        mock_open.assert_called_once_with('test_config.yaml', 'r')
    
    @patch('schema_graph_builder.connectors.base_connector.yaml.safe_load')
    @patch('builtins.open')
    def test_config_validation(self, mock_open, mock_yaml_load):
        """Test configuration validation"""
        connector = PostgreSQLConnector()
        
        # Test invalid config (not a dict)
        mock_yaml_load.return_value = "not a dict"
        
        with pytest.raises(ValueError, match="Configuration must be a dictionary"):
            connector._load_config('test_config.yaml')
    
    @patch('schema_graph_builder.connectors.base_connector.CredentialManager.get_credentials')
    def test_connection_parameters(self, mock_get_credentials):
        """Test connection parameter extraction"""
        mock_get_credentials.return_value = ('testuser', 'testpass')
        
        connector = PostgreSQLConnector()
        config = {
            'host': 'testhost',
            'port': 5433,
            'database': 'testdb'
        }
        
        params = connector._get_connection_params(config)
        
        assert params['host'] == 'testhost'
        assert params['port'] == 5433
        assert params['database'] == 'testdb'
    
    def test_connection_parameters_defaults(self):
        """Test default connection parameters"""
        connector = PostgreSQLConnector()
        config = {'database': 'testdb'}
        
        params = connector._get_connection_params(config)
        
        assert params['host'] == 'localhost'
        assert params['port'] == 5432  # PostgreSQL default
        assert params['database'] == 'testdb'
    
    def test_missing_database_error(self):
        """Test error when database name is missing"""
        connector = PostgreSQLConnector()
        config = {'host': 'localhost'}
        
        with pytest.raises(ValueError, match="Database name is required in configuration"):
            connector._get_connection_params(config)
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.ConnectionSecurity.create_secure_connection_string')
    @patch('schema_graph_builder.connectors.base_connector.ConnectionSecurity.mask_connection_string')
    def test_connection_creation(self, mock_mask, mock_create_string, mock_create_engine):
        """Test database connection creation"""
        mock_create_string.return_value = 'postgres://user:pass@localhost:5432/db'
        mock_mask.return_value = 'postgres://user:***@localhost:5432/db'
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        
        connector = PostgreSQLConnector()
        connection_params = {
            'host': 'localhost',
            'port': 5432,
            'database': 'testdb'
        }
        
        connector._create_connection(connection_params, 'testuser', 'testpass')
        
        assert connector.engine == mock_engine
        mock_create_engine.assert_called_once()
    
    def test_cleanup_connection(self):
        """Test connection cleanup"""
        connector = PostgreSQLConnector()
        mock_engine = Mock()
        connector.engine = mock_engine
        
        connector._cleanup_connection()
        
        mock_engine.dispose.assert_called_once()
        assert connector.engine is None
    
    def test_cleanup_connection_with_error(self):
        """Test connection cleanup with error"""
        connector = PostgreSQLConnector()
        mock_engine = Mock()
        mock_engine.dispose.side_effect = Exception("Cleanup error")
        connector.engine = mock_engine
        
        # Should not raise exception
        connector._cleanup_connection()
        
        assert connector.engine is None


class TestSchemaExtractorRegistry:
    """Test the schema extractor registry functionality"""
    
    def test_supported_database_types(self):
        """Test getting supported database types"""
        supported = get_supported_database_types()
        
        assert 'postgres' in supported
        assert 'postgresql' in supported
        assert 'mysql' in supported
        assert 'mssql' in supported
        assert 'sqlserver' in supported
    
    def test_registry_mapping(self):
        """Test that registry maps correctly to connector classes"""
        assert DATABASE_CONNECTORS['postgres'] == PostgreSQLConnector
        assert DATABASE_CONNECTORS['postgresql'] == PostgreSQLConnector
        assert DATABASE_CONNECTORS['mysql'] == MySQLConnector
        assert DATABASE_CONNECTORS['mssql'] == MSSQLConnector
        assert DATABASE_CONNECTORS['sqlserver'] == MSSQLConnector
    
    def test_register_new_connector(self):
        """Test registering a new connector type"""
        class TestConnector(DatabaseConnector):
            def __init__(self):
                super().__init__('test', 9999)
            
            def _get_db_specific_params(self, config):
                return {}
            
            def _get_connect_args(self):
                return {}
        
        # Store original state
        original_connectors = DATABASE_CONNECTORS.copy()
        
        try:
            register_database_connector('test', TestConnector)
            
            assert 'test' in DATABASE_CONNECTORS
            assert DATABASE_CONNECTORS['test'] == TestConnector
            
            supported = get_supported_database_types()
            assert 'test' in supported
            
        finally:
            # Restore original state
            DATABASE_CONNECTORS.clear()
            DATABASE_CONNECTORS.update(original_connectors)
    
    def test_register_invalid_connector(self):
        """Test registering an invalid connector raises error"""
        class InvalidConnector:
            pass
        
        with pytest.raises(ValueError, match="connector_class must be a subclass of DatabaseConnector"):
            register_database_connector('invalid', InvalidConnector)
    
    @patch('schema_graph_builder.connectors.base_connector.yaml.safe_load')
    @patch('builtins.open')
    @patch('schema_graph_builder.connectors.base_connector.CredentialManager.get_credentials')
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_extract_schema_uses_registry(self, mock_inspect, mock_create_engine, mock_get_credentials, mock_open, mock_yaml_load):
        """Test that extract_schema uses the registry"""
        # Mock configuration
        mock_config = {'database': 'test', 'host': 'localhost', 'username': 'user', 'password': 'pass'}
        mock_yaml_load.return_value = mock_config
        mock_get_credentials.return_value = ('user', 'pass')
        
        # Mock database connection
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        mock_inspector = Mock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.get_table_names.return_value = ['table1', 'table2']
        mock_inspector.get_columns.return_value = [{'name': 'col1', 'type': 'VARCHAR', 'nullable': True}]
        mock_inspector.get_pk_constraint.return_value = {'constrained_columns': ['col1']}
        
        result = extract_schema('postgres', 'test_config.yaml')
        
        # Verify the result has the expected structure
        assert 'database' in result
        assert 'tables' in result
        assert result['database'] == 'test'
    
    def test_extract_schema_unsupported_type(self):
        """Test extract_schema with unsupported database type"""
        with pytest.raises(ValueError, match="Unsupported database type: 'unsupported'"):
            extract_schema('unsupported', 'test_config.yaml')
    
    @patch('schema_graph_builder.connectors.base_connector.yaml.safe_load')
    @patch('builtins.open')
    @patch('schema_graph_builder.connectors.base_connector.CredentialManager.get_credentials')
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_extract_schema_case_insensitive(self, mock_inspect, mock_create_engine, mock_get_credentials, mock_open, mock_yaml_load):
        """Test that extract_schema is case insensitive"""
        # Mock configuration
        mock_config = {'database': 'test', 'host': 'localhost', 'username': 'user', 'password': 'pass'}
        mock_yaml_load.return_value = mock_config
        mock_get_credentials.return_value = ('user', 'pass')
        
        # Mock database connection
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine
        mock_inspector = Mock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.get_table_names.return_value = ['table1']
        mock_inspector.get_columns.return_value = [{'name': 'col1', 'type': 'VARCHAR', 'nullable': True}]
        mock_inspector.get_pk_constraint.return_value = {'constrained_columns': ['col1']}
        
        # Test different cases - all should work
        result1 = extract_schema('POSTGRES', 'test_config.yaml')
        result2 = extract_schema('PostgreSQL', 'test_config.yaml')
        result3 = extract_schema('postgres', 'test_config.yaml')
        
        # All should return the same result
        assert result1['database'] == 'test'
        assert result2['database'] == 'test'
        assert result3['database'] == 'test'


class TestIntegrationWithBackwardCompatibility:
    """Test that the new abstraction maintains backward compatibility"""
    
    @patch('schema_graph_builder.connectors.postgres_connector.PostgreSQLConnector')
    def test_backward_compatibility_postgres(self, mock_connector_class):
        """Test that old function interface still works for PostgreSQL"""
        from schema_graph_builder.connectors.postgres_connector import get_postgres_schema
        
        mock_connector = Mock()
        mock_connector_class.return_value = mock_connector
        mock_schema = {'database': 'test', 'tables': []}
        mock_connector.extract_schema.return_value = mock_schema
        
        result = get_postgres_schema('test_config.yaml')
        
        assert result == mock_schema
        mock_connector_class.assert_called_once()
        mock_connector.extract_schema.assert_called_once_with('test_config.yaml')
    
    @patch('schema_graph_builder.connectors.mysql_connector.MySQLConnector')
    def test_backward_compatibility_mysql(self, mock_connector_class):
        """Test that old function interface still works for MySQL"""
        from schema_graph_builder.connectors.mysql_connector import get_mysql_schema
        
        mock_connector = Mock()
        mock_connector_class.return_value = mock_connector
        mock_schema = {'database': 'test', 'tables': []}
        mock_connector.extract_schema.return_value = mock_schema
        
        result = get_mysql_schema('test_config.yaml')
        
        assert result == mock_schema
        mock_connector_class.assert_called_once()
        mock_connector.extract_schema.assert_called_once_with('test_config.yaml')
    
    @patch('schema_graph_builder.connectors.mssql_connector.MSSQLConnector')
    def test_backward_compatibility_mssql(self, mock_connector_class):
        """Test that old function interface still works for MSSQL"""
        from schema_graph_builder.connectors.mssql_connector import get_mssql_schema
        
        mock_connector = Mock()
        mock_connector_class.return_value = mock_connector
        mock_schema = {'database': 'test', 'tables': []}
        mock_connector.extract_schema.return_value = mock_schema
        
        result = get_mssql_schema('test_config.yaml')
        
        assert result == mock_schema
        mock_connector_class.assert_called_once()
        mock_connector.extract_schema.assert_called_once_with('test_config.yaml') 
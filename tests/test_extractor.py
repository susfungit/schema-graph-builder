"""
Tests for schema extractor module
"""

import pytest
from unittest.mock import patch, Mock
from schema_graph_builder.extractor.schema_extractor import extract_schema, get_supported_database_types


class TestSchemaExtractor:
    """Tests for the schema extractor"""
    
    def test_extract_schema_postgres(self, sample_schema, temp_config_file):
        """Test schema extraction for PostgreSQL"""
        with patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._create_connection'), \
             patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._extract_schema_data', return_value=sample_schema):
            
            result = extract_schema('postgres', temp_config_file)
            
            assert result == sample_schema
            assert result['database'] == 'testdb'
            assert len(result['tables']) == 3
    
    def test_extract_schema_postgresql_alias(self, sample_schema, temp_config_file):
        """Test schema extraction using 'postgresql' alias"""
        with patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._create_connection'), \
             patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._extract_schema_data', return_value=sample_schema):
            
            result = extract_schema('postgresql', temp_config_file)
            
            assert result == sample_schema
    
    def test_extract_schema_mysql(self, sample_schema, temp_config_file):
        """Test schema extraction for MySQL"""
        with patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._create_connection'), \
             patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._extract_schema_data', return_value=sample_schema):
            
            result = extract_schema('mysql', temp_config_file)
            
            assert result == sample_schema
    
    def test_extract_schema_mssql(self, sample_schema, temp_config_file):
        """Test schema extraction for MS SQL Server"""
        with patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._create_connection'), \
             patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._extract_schema_data', return_value=sample_schema):
            
            result = extract_schema('mssql', temp_config_file)
            
            assert result == sample_schema
    
    def test_extract_schema_sqlserver_alias(self, sample_schema, temp_config_file):
        """Test schema extraction using 'sqlserver' alias"""
        with patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._create_connection'), \
             patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._extract_schema_data', return_value=sample_schema):
            
            result = extract_schema('sqlserver', temp_config_file)
            
            assert result == sample_schema
    
    def test_extract_schema_oracle(self, sample_schema, temp_config_file):
        """Test schema extraction for Oracle"""
        # Create a proper Oracle config for testing
        oracle_config = {
            'host': 'localhost',
            'port': 1521,
            'service_name': 'XEPDB1',
            'username': 'testuser',
            'password': 'testpass'
        }
        
        import tempfile
        import yaml
        import os
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(oracle_config, f)
            oracle_config_file = f.name
        
        try:
            with patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._create_connection'), \
                 patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._extract_schema_data', return_value=sample_schema):
                
                result = extract_schema('oracle', oracle_config_file)
                
                assert result == sample_schema
        finally:
            os.unlink(oracle_config_file)
    
    def test_extract_schema_unsupported_db_type(self, temp_config_file):
        """Test schema extraction with unsupported database type"""
        with pytest.raises(ValueError, match="Unsupported database type: 'nosql'"):
            extract_schema('nosql', temp_config_file)
    
    def test_extract_schema_case_insensitive(self, temp_config_file):
        """Test that database type matching is case insensitive"""
        with patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._create_connection'), \
             patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._extract_schema_data', return_value={'database': 'test', 'tables': []}):
            
            # Test various case combinations
            for db_type in ['POSTGRES', 'Postgres', 'PostgreSQL', 'POSTGRESQL']:
                result = extract_schema(db_type, temp_config_file)
                assert result['database'] == 'test'
    
    def test_extract_schema_connector_exception(self, temp_config_file):
        """Test handling of exceptions from connectors"""
        with patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._create_connection'), \
             patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._extract_schema_data', side_effect=Exception("Database connection failed")):
            
            with pytest.raises(Exception, match="Database connection failed"):
                extract_schema('postgres', temp_config_file)
    
    def test_extract_schema_supported_databases_list(self):
        """Test that the function supports all expected database types"""
        # Use the new get_supported_database_types function
        supported_types = get_supported_database_types()
        
        # Ensure we have the expected types including Oracle
        expected_types = ['postgres', 'postgresql', 'mysql', 'mssql', 'sqlserver', 'oracle']
        for db_type in expected_types:
            assert db_type in supported_types, f"Database type '{db_type}' should be supported"
    
    def test_extract_schema_empty_string_db_type(self, temp_config_file):
        """Test schema extraction with empty database type"""
        with pytest.raises(ValueError, match="Unsupported database type: ''. Supported types:"):
            extract_schema('', temp_config_file)
    
    def test_extract_schema_none_db_type(self, temp_config_file):
        """Test schema extraction with None database type"""
        with pytest.raises(AttributeError):
            extract_schema(None, temp_config_file) 
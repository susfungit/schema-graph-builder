"""
Tests for schema extractor module
"""

import pytest
from unittest.mock import patch, Mock
from schema_graph_builder.extractor.schema_extractor import extract_schema, get_supported_database_types


class TestSchemaExtractor:
    """Tests for the schema extractor"""
    
    @patch('schema_graph_builder.extractor.schema_extractor.PostgreSQLConnector')
    def test_extract_schema_postgres(self, mock_postgres_class, sample_schema, temp_config_file):
        """Test schema extraction for PostgreSQL"""
        mock_connector = Mock()
        mock_postgres_class.return_value = mock_connector
        mock_connector.extract_schema.return_value = sample_schema
        
        result = extract_schema('postgres', temp_config_file)
        
        mock_postgres_class.assert_called_once()
        mock_connector.extract_schema.assert_called_once_with(temp_config_file)
        assert result == sample_schema
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 3
    
    @patch('schema_graph_builder.extractor.schema_extractor.PostgreSQLConnector')
    def test_extract_schema_postgresql_alias(self, mock_postgres_class, sample_schema, temp_config_file):
        """Test schema extraction using 'postgresql' alias"""
        mock_connector = Mock()
        mock_postgres_class.return_value = mock_connector
        mock_connector.extract_schema.return_value = sample_schema
        
        result = extract_schema('postgresql', temp_config_file)
        
        mock_postgres_class.assert_called_once()
        mock_connector.extract_schema.assert_called_once_with(temp_config_file)
        assert result == sample_schema
    
    @patch('schema_graph_builder.extractor.schema_extractor.MySQLConnector')
    def test_extract_schema_mysql(self, mock_mysql_class, sample_schema, temp_config_file):
        """Test schema extraction for MySQL"""
        mock_connector = Mock()
        mock_mysql_class.return_value = mock_connector
        mock_connector.extract_schema.return_value = sample_schema
        
        result = extract_schema('mysql', temp_config_file)
        
        mock_mysql_class.assert_called_once()
        mock_connector.extract_schema.assert_called_once_with(temp_config_file)
        assert result == sample_schema
    
    @patch('schema_graph_builder.extractor.schema_extractor.MSSQLConnector')
    def test_extract_schema_mssql(self, mock_mssql_class, sample_schema, temp_config_file):
        """Test schema extraction for MS SQL Server"""
        mock_connector = Mock()
        mock_mssql_class.return_value = mock_connector
        mock_connector.extract_schema.return_value = sample_schema
        
        result = extract_schema('mssql', temp_config_file)
        
        mock_mssql_class.assert_called_once()
        mock_connector.extract_schema.assert_called_once_with(temp_config_file)
        assert result == sample_schema
    
    @patch('schema_graph_builder.extractor.schema_extractor.MSSQLConnector')
    def test_extract_schema_sqlserver_alias(self, mock_mssql_class, sample_schema, temp_config_file):
        """Test schema extraction using 'sqlserver' alias"""
        mock_connector = Mock()
        mock_mssql_class.return_value = mock_connector
        mock_connector.extract_schema.return_value = sample_schema
        
        result = extract_schema('sqlserver', temp_config_file)
        
        mock_mssql_class.assert_called_once()
        mock_connector.extract_schema.assert_called_once_with(temp_config_file)
        assert result == sample_schema
    
    def test_extract_schema_unsupported_db_type(self, temp_config_file):
        """Test schema extraction with unsupported database type"""
        with pytest.raises(ValueError, match="Unsupported database type: 'oracle'. Supported types:"):
            extract_schema('oracle', temp_config_file)
    
    @patch('schema_graph_builder.extractor.schema_extractor.PostgreSQLConnector')
    def test_extract_schema_case_insensitive(self, mock_postgres_class, temp_config_file):
        """Test that database type matching is case insensitive"""
        mock_connector = Mock()
        mock_postgres_class.return_value = mock_connector
        mock_connector.extract_schema.return_value = {'database': 'test', 'tables': []}
        
        # Test various case combinations
        for db_type in ['POSTGRES', 'Postgres', 'PostgreSQL', 'POSTGRESQL']:
            extract_schema(db_type, temp_config_file)
            mock_connector.extract_schema.assert_called_with(temp_config_file)
    
    @patch('schema_graph_builder.extractor.schema_extractor.PostgreSQLConnector')
    def test_extract_schema_connector_exception(self, mock_postgres_class, temp_config_file):
        """Test handling of exceptions from connectors"""
        mock_connector = Mock()
        mock_postgres_class.return_value = mock_connector
        mock_connector.extract_schema.side_effect = Exception("Database connection failed")
        
        with pytest.raises(Exception, match="Database connection failed"):
            extract_schema('postgres', temp_config_file)
    
    def test_extract_schema_supported_databases_list(self):
        """Test that the function supports all expected database types"""
        # Use the new get_supported_database_types function
        supported_types = get_supported_database_types()
        
        # Ensure we have the expected types
        expected_types = ['postgres', 'postgresql', 'mysql', 'mssql', 'sqlserver']
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
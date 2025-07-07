"""
Tests for schema extractor module
"""

import pytest
from unittest.mock import patch, Mock
from schema_graph_builder.extractor.schema_extractor import extract_schema


class TestSchemaExtractor:
    """Tests for the schema extractor"""
    
    @patch('schema_graph_builder.extractor.schema_extractor.get_postgres_schema')
    def test_extract_schema_postgres(self, mock_get_postgres, sample_schema, temp_config_file):
        """Test schema extraction for PostgreSQL"""
        mock_get_postgres.return_value = sample_schema
        
        result = extract_schema('postgres', temp_config_file)
        
        mock_get_postgres.assert_called_once_with(temp_config_file)
        assert result == sample_schema
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 3
    
    @patch('schema_graph_builder.extractor.schema_extractor.get_postgres_schema')
    def test_extract_schema_postgresql_alias(self, mock_get_postgres, sample_schema, temp_config_file):
        """Test schema extraction using 'postgresql' alias"""
        mock_get_postgres.return_value = sample_schema
        
        result = extract_schema('postgresql', temp_config_file)
        
        mock_get_postgres.assert_called_once_with(temp_config_file)
        assert result == sample_schema
    
    @patch('schema_graph_builder.extractor.schema_extractor.get_mysql_schema')
    def test_extract_schema_mysql(self, mock_get_mysql, sample_schema, temp_config_file):
        """Test schema extraction for MySQL"""
        mock_get_mysql.return_value = sample_schema
        
        result = extract_schema('mysql', temp_config_file)
        
        mock_get_mysql.assert_called_once_with(temp_config_file)
        assert result == sample_schema
    
    @patch('schema_graph_builder.extractor.schema_extractor.get_mssql_schema')
    def test_extract_schema_mssql(self, mock_get_mssql, sample_schema, temp_config_file):
        """Test schema extraction for MS SQL Server"""
        mock_get_mssql.return_value = sample_schema
        
        result = extract_schema('mssql', temp_config_file)
        
        mock_get_mssql.assert_called_once_with(temp_config_file)
        assert result == sample_schema
    
    @patch('schema_graph_builder.extractor.schema_extractor.get_mssql_schema')
    def test_extract_schema_sqlserver_alias(self, mock_get_mssql, sample_schema, temp_config_file):
        """Test schema extraction using 'sqlserver' alias"""
        mock_get_mssql.return_value = sample_schema
        
        result = extract_schema('sqlserver', temp_config_file)
        
        mock_get_mssql.assert_called_once_with(temp_config_file)
        assert result == sample_schema
    
    @patch('schema_graph_builder.extractor.schema_extractor.get_oracle_schema')
    def test_extract_schema_oracle(self, mock_get_oracle, sample_schema, temp_config_file):
        """Test schema extraction for Oracle"""
        mock_get_oracle.return_value = sample_schema
        
        result = extract_schema('oracle', temp_config_file)
        
        mock_get_oracle.assert_called_once_with(temp_config_file)
        assert result == sample_schema
    
    def test_extract_schema_unsupported_db_type(self, temp_config_file):
        """Test schema extraction with unsupported database type"""
        with pytest.raises(ValueError, match="Unsupported database type: 'nosql'"):
            extract_schema('nosql', temp_config_file)
    
    def test_extract_schema_case_insensitive(self, temp_config_file):
        """Test that database type matching is case insensitive"""
        with patch('schema_graph_builder.extractor.schema_extractor.get_postgres_schema') as mock_get_postgres:
            mock_get_postgres.return_value = {'database': 'test', 'tables': []}
            
            # Test various case combinations
            for db_type in ['POSTGRES', 'Postgres', 'PostgreSQL', 'POSTGRESQL']:
                extract_schema(db_type, temp_config_file)
                mock_get_postgres.assert_called_with(temp_config_file)
    
    @patch('schema_graph_builder.extractor.schema_extractor.get_postgres_schema')
    def test_extract_schema_connector_exception(self, mock_get_postgres, temp_config_file):
        """Test handling of exceptions from connectors"""
        mock_get_postgres.side_effect = Exception("Database connection failed")
        
        with pytest.raises(Exception, match="Database connection failed"):
            extract_schema('postgres', temp_config_file)
    
    def test_extract_schema_supported_databases_list(self):
        """Test that the function supports all expected database types"""
        # This test ensures we don't accidentally remove support for any database
        supported_types = ['postgres', 'postgresql', 'mysql', 'mssql', 'sqlserver', 'oracle']
        
        for db_type in supported_types:
            # Map database type to correct function name
            if db_type.lower() in ['postgres', 'postgresql']:
                func_name = 'get_postgres_schema'
            elif db_type.lower() == 'mysql':
                func_name = 'get_mysql_schema'
            elif db_type.lower() in ['mssql', 'sqlserver']:
                func_name = 'get_mssql_schema'
            elif db_type.lower() == 'oracle':
                func_name = 'get_oracle_schema'
            else:
                func_name = f'get_{db_type}_schema'
                
            with patch(f'schema_graph_builder.extractor.schema_extractor.{func_name}') as mock_connector:
                mock_connector.return_value = {'database': 'test', 'tables': []}
                try:
                    extract_schema(db_type, 'dummy_config.yaml')
                except ValueError:
                    pytest.fail(f"Database type '{db_type}' should be supported")
                except FileNotFoundError:
                    # Expected for dummy config file, but the db type was recognized
                    pass
    
    def test_extract_schema_empty_string_db_type(self, temp_config_file):
        """Test schema extraction with empty database type"""
        with pytest.raises(ValueError, match="Unsupported database type: ''"):
            extract_schema('', temp_config_file)
    
    def test_extract_schema_none_db_type(self, temp_config_file):
        """Test schema extraction with None database type"""
        with pytest.raises(AttributeError):
            extract_schema(None, temp_config_file) 
"""
Tests for database connectors
"""

import pytest
import yaml
import tempfile
import os
from unittest.mock import patch, Mock, MagicMock
from schema_graph_builder.connectors.postgres_connector import get_postgres_schema
from schema_graph_builder.connectors.mysql_connector import get_mysql_schema
from schema_graph_builder.connectors.mssql_connector import get_mssql_schema


class TestPostgresConnector:
    """Tests for PostgreSQL connector"""
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_postgres_schema_success(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test successful PostgreSQL schema extraction"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock column types
        for table in ['customers', 'orders', 'products']:
            columns = mock_inspector.get_columns(table)
            for col in columns:
                col['type'] = Mock()
                col['type'].__str__ = Mock(return_value='INTEGER' if 'id' in col['name'] else 'VARCHAR(100)')
        
        result = get_postgres_schema(temp_config_file)
        
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 3
        assert any(table['name'] == 'customers' for table in result['tables'])
        assert any(table['name'] == 'orders' for table in result['tables'])
        assert any(table['name'] == 'products' for table in result['tables'])
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    def test_get_postgres_schema_connection_error(self, mock_create_engine, temp_config_file):
        """Test PostgreSQL connection error handling"""
        mock_create_engine.side_effect = Exception("Connection failed")
        
        with pytest.raises(Exception, match="Connection failed"):
            get_postgres_schema(temp_config_file)
    
    def test_get_postgres_schema_invalid_config(self):
        """Test PostgreSQL with invalid config file"""
        with pytest.raises(FileNotFoundError):
            get_postgres_schema("nonexistent_config.yaml")
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_postgres_schema_empty_database(self, mock_inspect, mock_create_engine, temp_config_file):
        """Test PostgreSQL with empty database"""
        mock_engine = Mock()
        mock_inspector = Mock()
        mock_inspector.get_table_names.return_value = []
        
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        result = get_postgres_schema(temp_config_file)
        
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 0


class TestMySQLConnector:
    """Tests for MySQL connector"""
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_mysql_schema_success(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test successful MySQL schema extraction"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock column types
        for table in ['customers', 'orders', 'products']:
            columns = mock_inspector.get_columns(table)
            for col in columns:
                col['type'] = Mock()
                col['type'].__str__ = Mock(return_value='INT' if 'id' in col['name'] else 'VARCHAR(100)')
        
        result = get_mysql_schema(temp_config_file)
        
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 3
        assert any(table['name'] == 'customers' for table in result['tables'])
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    def test_get_mysql_schema_connection_error(self, mock_create_engine, temp_config_file):
        """Test MySQL connection error handling"""
        mock_create_engine.side_effect = Exception("MySQL connection failed")
        
        with pytest.raises(Exception, match="MySQL connection failed"):
            get_mysql_schema(temp_config_file)


class TestMSSQLConnector:
    """Tests for MS SQL Server connector"""
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_mssql_schema_success(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test successful MS SQL Server schema extraction"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock column types
        for table in ['customers', 'orders', 'products']:
            columns = mock_inspector.get_columns(table)
            for col in columns:
                col['type'] = Mock()
                col['type'].__str__ = Mock(return_value='INT' if 'id' in col['name'] else 'NVARCHAR(100)')
        
        result = get_mssql_schema(temp_config_file)
        
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 3
        assert any(table['name'] == 'customers' for table in result['tables'])
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    def test_get_mssql_schema_connection_error(self, mock_create_engine, temp_config_file):
        """Test MS SQL Server connection error handling"""
        mock_create_engine.side_effect = Exception("MSSQL connection failed")
        
        with pytest.raises(Exception, match="MSSQL connection failed"):
            get_mssql_schema(temp_config_file)


class TestConnectorIntegration:
    """Integration tests for all connectors"""
    
    def test_all_connectors_return_consistent_format(self, temp_config_file, mock_sqlalchemy_engine):
        """Test that all connectors return consistent schema format"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        
        with patch('schema_graph_builder.connectors.base_connector.create_engine', return_value=mock_engine), \
             patch('schema_graph_builder.connectors.base_connector.inspect', return_value=mock_inspector):
            
            # Mock column types for different databases
            for table in ['customers', 'orders', 'products']:
                columns = mock_inspector.get_columns(table)
                for col in columns:
                    col['type'] = Mock()
                    col['type'].__str__ = Mock(return_value='INTEGER' if 'id' in col['name'] else 'VARCHAR(100)')
            
            postgres_result = get_postgres_schema(temp_config_file)
            mysql_result = get_mysql_schema(temp_config_file)
            mssql_result = get_mssql_schema(temp_config_file)
            
            # Check consistent structure
            for result in [postgres_result, mysql_result, mssql_result]:
                assert 'database' in result
                assert 'tables' in result
                assert isinstance(result['tables'], list)
                
                for table in result['tables']:
                    assert 'name' in table
                    assert 'columns' in table
                    assert isinstance(table['columns'], list)
                    
                    for column in table['columns']:
                        assert 'name' in column
                        assert 'type' in column
                        assert 'nullable' in column
                        assert 'primary_key' in column 
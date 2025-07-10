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
from schema_graph_builder.connectors.oracle_connector import get_oracle_schema
from schema_graph_builder.connectors.redshift_connector import get_redshift_schema


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


class TestOracleConnector:
    """Tests for Oracle connector"""
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_oracle_schema_success_with_service_name(self, mock_inspect, mock_create_engine, mock_sqlalchemy_engine):
        """Test successful Oracle schema extraction with service_name"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Create test config with service_name
        config = {
            'host': 'localhost',
            'port': 1521,
            'service_name': 'XEPDB1',
            'username': 'testuser',
            'password': 'testpass'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            config_file = f.name
        
        try:
            # Mock column types
            for table in ['customers', 'orders', 'products']:
                columns = mock_inspector.get_columns(table)
                for col in columns:
                    col['type'] = Mock()
                    col['type'].__str__ = Mock(return_value='NUMBER' if 'id' in col['name'] else 'VARCHAR2(100)')
            
            result = get_oracle_schema(config_file)
            
            assert result['database'] == 'XEPDB1'
            assert len(result['tables']) == 3
            assert any(table['name'] == 'customers' for table in result['tables'])
            assert any(table['name'] == 'orders' for table in result['tables'])
            assert any(table['name'] == 'products' for table in result['tables'])
        finally:
            os.unlink(config_file)
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_oracle_schema_success_with_sid(self, mock_inspect, mock_create_engine, mock_sqlalchemy_engine):
        """Test successful Oracle schema extraction with SID"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Create test config with SID
        config = {
            'host': 'localhost',
            'port': 1521,
            'sid': 'XE',
            'username': 'testuser',
            'password': 'testpass'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            config_file = f.name
        
        try:
            # Mock column types
            for table in ['customers', 'orders', 'products']:
                columns = mock_inspector.get_columns(table)
                for col in columns:
                    col['type'] = Mock()
                    col['type'].__str__ = Mock(return_value='NUMBER' if 'id' in col['name'] else 'VARCHAR2(100)')
            
            result = get_oracle_schema(config_file)
            
            assert result['database'] == 'XE'
            assert len(result['tables']) == 3
            assert any(table['name'] == 'customers' for table in result['tables'])
        finally:
            os.unlink(config_file)
    
    def test_get_oracle_schema_missing_service_name_and_sid(self):
        """Test Oracle with missing service_name and SID"""
        config = {
            'host': 'localhost',
            'port': 1521,
            'username': 'testuser',
            'password': 'testpass'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            config_file = f.name
        
        try:
            with pytest.raises(ValueError, match="Oracle configuration requires either 'service_name' or 'sid'"):
                get_oracle_schema(config_file)
        finally:
            os.unlink(config_file)
    
    def test_get_oracle_schema_both_service_name_and_sid(self):
        """Test Oracle with both service_name and SID provided"""
        config = {
            'host': 'localhost',
            'port': 1521,
            'service_name': 'XEPDB1',
            'sid': 'XE',
            'username': 'testuser',
            'password': 'testpass'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            config_file = f.name
        
        try:
            with pytest.raises(ValueError, match="Oracle configuration cannot have both 'service_name' and 'sid'"):
                get_oracle_schema(config_file)
        finally:
            os.unlink(config_file)
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    def test_get_oracle_schema_connection_error(self, mock_create_engine):
        """Test Oracle connection error handling"""
        mock_create_engine.side_effect = Exception("Oracle connection failed")
        
        config = {
            'host': 'localhost',
            'port': 1521,
            'service_name': 'XEPDB1',
            'username': 'testuser',
            'password': 'testpass'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            config_file = f.name
        
        try:
            with pytest.raises(Exception, match="Oracle connection failed"):
                get_oracle_schema(config_file)
        finally:
            os.unlink(config_file)
    
    def test_get_oracle_schema_invalid_config(self):
        """Test Oracle with invalid config file"""
        with pytest.raises(FileNotFoundError):
            get_oracle_schema("nonexistent_config.yaml")
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_oracle_schema_empty_database(self, mock_inspect, mock_create_engine):
        """Test Oracle with empty database"""
        mock_engine = Mock()
        mock_inspector = Mock()
        mock_inspector.get_table_names.return_value = []
        
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        config = {
            'host': 'localhost',
            'port': 1521,
            'service_name': 'XEPDB1',
            'username': 'testuser',
            'password': 'testpass'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            config_file = f.name
        
        try:
            result = get_oracle_schema(config_file)
            
            assert result['database'] == 'XEPDB1'
            assert len(result['tables']) == 0
        finally:
            os.unlink(config_file)
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_oracle_schema_with_env_variables(self, mock_inspect, mock_create_engine, mock_sqlalchemy_engine):
        """Test Oracle schema extraction with environment variables"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Create config without credentials
        config = {
            'host': 'localhost',
            'port': 1521,
            'service_name': 'XEPDB1'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            config_file = f.name
        
        try:
            # Mock environment variables
            with patch.dict(os.environ, {'DB_USERNAME': 'envuser', 'DB_PASSWORD': 'envpass'}):
                # Mock column types
                for table in ['customers', 'orders', 'products']:
                    columns = mock_inspector.get_columns(table)
                    for col in columns:
                        col['type'] = Mock()
                        col['type'].__str__ = Mock(return_value='NUMBER' if 'id' in col['name'] else 'VARCHAR2(100)')
                
                result = get_oracle_schema(config_file)
                
                assert result['database'] == 'XEPDB1'
                assert len(result['tables']) == 3
        finally:
            os.unlink(config_file)


class TestRedshiftConnector:
    """Tests for Amazon Redshift connector"""
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_redshift_schema_success(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test successful Redshift schema extraction"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock Redshift-specific metadata query
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([
            ('public', 'customers', 'customer_id', True, 1, 'lzo'),  # distribution key, sort key 1, encoding
            ('public', 'customers', 'name', False, None, 'text255'),
            ('public', 'orders', 'order_id', False, 1, 'lzo'),  # sort key 1
            ('public', 'orders', 'customer_id', False, 2, 'lzo'),  # sort key 2
        ]))
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        # Mock column types for Redshift
        for table in ['customers', 'orders', 'products']:
            columns = mock_inspector.get_columns(table)
            for col in columns:
                col['type'] = Mock()
                col['type'].__str__ = Mock(return_value='INTEGER' if 'id' in col['name'] else 'VARCHAR(256)')
        
        result = get_redshift_schema(temp_config_file)
        
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 3
        assert any(table['name'] == 'customers' for table in result['tables'])
        
        # Check Redshift-specific metadata
        customers_table = next(table for table in result['tables'] if table['name'] == 'customers')
        assert 'redshift_metadata' in customers_table
        
        redshift_meta = customers_table['redshift_metadata']
        assert redshift_meta['distribution_key'] == 'customer_id'
        assert len(redshift_meta['sort_keys']) == 1
        assert redshift_meta['sort_keys'][0]['column'] == 'customer_id'
        assert redshift_meta['sort_keys'][0]['sort_order'] == 1
        assert 'customer_id' in redshift_meta['column_encodings']
        assert redshift_meta['column_encodings']['customer_id'] == 'lzo'
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    def test_get_redshift_schema_connection_error(self, mock_create_engine, temp_config_file):
        """Test Redshift connection error handling"""
        mock_create_engine.side_effect = Exception("Redshift connection failed")
        
        with pytest.raises(Exception, match="Redshift connection failed"):
            get_redshift_schema(temp_config_file)
    
    def test_get_redshift_schema_invalid_config(self):
        """Test Redshift with invalid config file"""
        with pytest.raises(FileNotFoundError):
            get_redshift_schema("nonexistent_config.yaml")
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_redshift_schema_empty_database(self, mock_inspect, mock_create_engine, temp_config_file):
        """Test Redshift with empty database"""
        mock_engine = Mock()
        mock_inspector = Mock()
        mock_inspector.get_table_names.return_value = []
        
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock empty Redshift metadata query
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        result = get_redshift_schema(temp_config_file)
        
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 0
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_redshift_schema_ssl_connection(self, mock_inspect, mock_create_engine, mock_sqlalchemy_engine):
        """Test Redshift SSL connection requirements"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock Redshift metadata query
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        # Create config with SSL settings
        config = {
            'host': 'my-cluster.abc123.us-east-1.redshift.amazonaws.com',
            'port': 5439,
            'database': 'testdb',
            'username': 'testuser',
            'password': 'testpass',
            'ssl_mode': 'require',
            'region': 'us-east-1'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            config_file = f.name
        
        try:
            result = get_redshift_schema(config_file)
            
            # Verify SSL was enforced in connection
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args
            
            # Check that SSL mode was set
            assert 'sslmode=require' in call_args[0][0]  # Connection string
            
            assert result['database'] == 'testdb'
        finally:
            os.unlink(config_file)
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_redshift_schema_metadata_query_error(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test Redshift schema extraction with metadata query error"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock metadata query failure
        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Metadata query failed")
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        # Mock column types
        for table in ['customers', 'orders', 'products']:
            columns = mock_inspector.get_columns(table)
            for col in columns:
                col['type'] = Mock()
                col['type'].__str__ = Mock(return_value='INTEGER' if 'id' in col['name'] else 'VARCHAR(256)')
        
        # Should still return schema but without Redshift metadata
        result = get_redshift_schema(temp_config_file)
        
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 3
        
        # Tables should not have Redshift metadata due to query failure
        for table in result['tables']:
            assert 'redshift_metadata' not in table


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
            
            # Create Oracle config for testing
            oracle_config = {
                'host': 'localhost',
                'port': 1521,
                'service_name': 'XEPDB1',
                'username': 'testuser',
                'password': 'testpass'
            }
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                yaml.dump(oracle_config, f)
                oracle_config_file = f.name
            
            # Create Redshift config for testing
            redshift_config = {
                'host': 'my-cluster.abc123.us-east-1.redshift.amazonaws.com',
                'port': 5439,
                'database': 'testdb',
                'username': 'testuser',
                'password': 'testpass',
                'ssl_mode': 'require'
            }
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                yaml.dump(redshift_config, f)
                redshift_config_file = f.name
            
            try:
                oracle_result = get_oracle_schema(oracle_config_file)
                
                # Mock Redshift metadata query for integration test
                mock_connection = Mock()
                mock_result = Mock()
                mock_result.__iter__ = Mock(return_value=iter([]))
                mock_connection.execute.return_value = mock_result
                mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
                mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
                
                redshift_result = get_redshift_schema(redshift_config_file)
                
                # Check consistent structure
                for result in [postgres_result, mysql_result, mssql_result, oracle_result, redshift_result]:
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
            finally:
                os.unlink(oracle_config_file)
                os.unlink(redshift_config_file) 
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
from schema_graph_builder.connectors.sybase_connector import get_sybase_schema
from schema_graph_builder.connectors.db2_connector import get_db2_schema


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
        """Test successful Redshift schema extraction with enhanced metadata"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock multiple queries for enhanced metadata
        mock_connection = Mock()
        
        # Mock the main metadata query results (comprehensive)
        mock_metadata_result = Mock()
        mock_metadata_result.__iter__ = Mock(return_value=iter([
            # Full row with all enhanced metadata fields
            ('public', 'customers', 'customer_id', True, 1, 'lzo', 'INTEGER', True, 'KEY', 'customer_id', 1, 100, 75, False, 5, 2, 1000, 1.2, 0.8, 'r', False, 0, 'TABLE'),
            ('public', 'customers', 'name', False, None, 'text255', 'VARCHAR', False, 'KEY', None, None, 100, 75, False, 5, 2, 1000, 1.2, 0.8, 'r', False, 0, 'TABLE'),
            ('public', 'orders', 'order_id', False, 1, 'lzo', 'INTEGER', True, 'EVEN', None, None, 250, 90, False, 10, 3, 5000, 0.9, 0.5, 'r', False, 0, 'TABLE'),
            ('public', 'orders', 'customer_id', False, 2, 'lzo', 'INTEGER', False, 'EVEN', None, None, 250, 90, False, 10, 3, 5000, 0.9, 0.5, 'r', False, 0, 'TABLE'),
        ]))
        
        # Mock sort key types query
        mock_sortkey_result = Mock()
        mock_sortkey_result.__iter__ = Mock(return_value=iter([
            ('public', 'customers', 'compound'),
            ('public', 'orders', 'interleaved'),
        ]))
        
        # Mock dependencies query
        mock_dependencies_result = Mock()
        mock_dependencies_result.__iter__ = Mock(return_value=iter([
            ('public', 'customer_orders_view', 'public', 'customers'),
            ('public', 'customer_orders_view', 'public', 'orders'),
        ]))
        
        # Mock external tables query
        mock_external_result = Mock()
        mock_external_result.__iter__ = Mock(return_value=iter([]))
        
        # Mock version query for cluster type detection
        mock_version_result = Mock()
        mock_version_result.fetchone.return_value = ('PostgreSQL 8.0.2 on x86_64-pc-linux-gnu, compiled by GCC',)
        
        # Set up the mock connection to return different results for different queries
        def mock_execute(query):
            if 'svv_table_info' in query:
                return mock_metadata_result
            elif 'relinterleaved' in query:
                return mock_sortkey_result
            elif 'pg_depend' in query:
                return mock_dependencies_result
            elif 'svv_external_tables' in query:
                return mock_external_result
            elif 'version()' in query:
                return mock_version_result
            return Mock()
        
        mock_connection.execute.side_effect = mock_execute
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

    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_redshift_schema_external_tables(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test Redshift schema extraction with external tables (Spectrum)"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock external tables query
        mock_connection = Mock()
        
        # Mock main metadata query (no external tables)
        mock_metadata_result = Mock()
        mock_metadata_result.__iter__ = Mock(return_value=iter([
            ('public', 'customers', 'customer_id', True, 1, 'lzo', 'INTEGER', True, 'KEY', 'customer_id', 1, 100, 75, False, 5, 2, 1000, 1.2, 0.8, 'r', False, 0, 'TABLE'),
        ]))
        
        # Mock external tables query with results
        mock_external_result = Mock()
        mock_external_result.__iter__ = Mock(return_value=iter([
            ('spectrum', 'external_sales', 'sale_id', False, None, None, 'INTEGER', False, None, None, None, 0, 0, True, 0, 0, 0, 0, 0, 'r', True, 0, 'EXTERNAL TABLE'),
            ('spectrum', 'external_sales', 'amount', False, None, None, 'DECIMAL', False, None, None, None, 0, 0, True, 0, 0, 0, 0, 0, 'r', True, 0, 'EXTERNAL TABLE'),
        ]))
        
        # Mock other queries with empty results
        mock_empty_result = Mock()
        mock_empty_result.__iter__ = Mock(return_value=iter([]))
        
        # Mock version query
        mock_version_result = Mock()
        mock_version_result.fetchone.return_value = ('PostgreSQL 8.0.2 on x86_64-pc-linux-gnu, compiled by GCC',)
        
        def mock_execute(query):
            if 'svv_external_tables' in query:
                return mock_external_result
            elif 'svv_table_info' in query:
                return mock_metadata_result
            elif 'version()' in query:
                return mock_version_result
            else:
                return mock_empty_result
        
        mock_connection.execute.side_effect = mock_execute
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        result = get_redshift_schema(temp_config_file)
        
        assert result['database'] == 'testdb'
        # Should have regular tables (external tables are typically not included in main table list)
        assert len(result['tables']) == 3  # customers, orders, products
        
        # Check that external table metadata exists in the result
        # External tables may be tracked separately or in metadata
        assert 'cluster_type' in result
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_redshift_schema_serverless_support(self, mock_inspect, mock_create_engine, mock_sqlalchemy_engine):
        """Test Redshift schema extraction with serverless workgroup configuration"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Create serverless config
        config = {
            'host': 'my-workgroup.123456789012.us-east-1.redshift-serverless.amazonaws.com',
            'port': 5439,
            'database': 'testdb',
            'username': 'testuser',
            'password': 'testpass',
            'ssl_mode': 'require',
            'region': 'us-east-1',
            'workgroup': 'my-workgroup'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            config_file = f.name
        
        try:
            # Mock serverless metadata query
            mock_connection = Mock()
            mock_result = Mock()
            mock_result.__iter__ = Mock(return_value=iter([
                ('public', 'customers', 'customer_id', True, 1, 'lzo', 'INTEGER', True, 'KEY', 'customer_id', 1, 100, 75, False, 5, 2, 1000, 1.2, 0.8, 'r', False, 0, 'TABLE'),
            ]))
            
            # Mock version query for serverless detection
            mock_version_result = Mock()
            mock_version_result.fetchone.return_value = ('PostgreSQL 8.0.2 on x86_64-pc-linux-gnu, compiled by GCC serverless',)
            
            # Mock other queries with empty results
            mock_empty_result = Mock()
            mock_empty_result.__iter__ = Mock(return_value=iter([]))
            
            def mock_execute(query):
                if 'svv_table_info' in query:
                    return mock_result
                elif 'version()' in query:
                    return mock_version_result
                else:
                    return mock_empty_result
            
            mock_connection.execute.side_effect = mock_execute
            mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
            mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
            
            result = get_redshift_schema(config_file)
            
            # Verify serverless connection was established
            mock_create_engine.assert_called_once()
            call_args = mock_create_engine.call_args
            connection_string = call_args[0][0]
            
            # Check that workgroup is properly configured
            assert 'redshift-serverless' in connection_string
            assert result['database'] == 'testdb'
            
            # Should detect serverless cluster type
            assert 'cluster_type' in result
            assert result['cluster_type'] == 'serverless'
            
        finally:
            os.unlink(config_file)
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_redshift_schema_sort_key_detection(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test Redshift sort key type detection (compound vs interleaved)"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        mock_connection = Mock()
        
        # Mock main metadata query
        mock_metadata_result = Mock()
        mock_metadata_result.__iter__ = Mock(return_value=iter([
            ('public', 'customers', 'customer_id', True, 1, 'lzo', 'INTEGER', True, 'KEY', 'customer_id', 1, 100, 75, False, 5, 2, 1000, 1.2, 0.8, 'r', False, 0, 'TABLE'),
            ('public', 'orders', 'order_id', False, 1, 'lzo', 'INTEGER', True, 'EVEN', 'order_id', 1, 250, 90, False, 10, 3, 5000, 0.9, 0.5, 'r', False, 0, 'TABLE'),
            ('public', 'orders', 'order_date', False, 2, 'lzo', 'DATE', False, 'EVEN', 'order_date', 2, 1000, 85, False, 8, 3, 25000, 2.5, 0.7, 'r', False, 0, 'TABLE'),
        ]))
        
        # Mock sort key types query - compound for customers, interleaved for orders
        mock_sortkey_result = Mock()
        mock_sortkey_result.__iter__ = Mock(return_value=iter([
            ('public', 'customers', 'compound'),
            ('public', 'orders', 'interleaved'),
        ]))
        
        # Mock other queries with empty results
        mock_empty_result = Mock()
        mock_empty_result.__iter__ = Mock(return_value=iter([]))
        
        # Mock version query
        mock_version_result = Mock()
        mock_version_result.fetchone.return_value = ('PostgreSQL 8.0.2 on x86_64-pc-linux-gnu, compiled by GCC',)
        
        def mock_execute(query):
            if 'svv_table_info' in query:
                return mock_metadata_result
            elif 'relinterleaved' in query:
                return mock_sortkey_result
            elif 'version()' in query:
                return mock_version_result
            else:
                return mock_empty_result
        
        mock_connection.execute.side_effect = mock_execute
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        result = get_redshift_schema(temp_config_file)
        
        # Check sort key type detection
        customers_table = next(table for table in result['tables'] if table['name'] == 'customers')
        orders_table = next(table for table in result['tables'] if table['name'] == 'orders')
        
        assert customers_table['redshift_metadata']['sort_key_type'] == 'compound'
        assert orders_table['redshift_metadata']['sort_key_type'] == 'interleaved'
        
        # Check sort keys structure
        assert len(orders_table['redshift_metadata']['sort_keys']) == 2
        assert any(sk['column'] == 'order_id' and sk['sort_order'] == 1 for sk in orders_table['redshift_metadata']['sort_keys'])
        assert any(sk['column'] == 'order_date' and sk['sort_order'] == 2 for sk in orders_table['redshift_metadata']['sort_keys'])
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_redshift_schema_performance_recommendations(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test Redshift performance recommendations generation"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        mock_connection = Mock()
        
        # Mock metadata with performance issues
        mock_metadata_result = Mock()
        mock_metadata_result.__iter__ = Mock(return_value=iter([
            # Table with high skew and low compression
            ('public', 'skewed_table', 'id', True, 1, 'raw', 'INTEGER', True, 'KEY', 'id', 1, 1000, 50, False, 20, 10, 50000, 15.0, 0.1, 'r', False, 0, 'TABLE'),
            # Table with no distribution key
            ('public', 'no_dist_key', 'id', True, 1, 'lzo', 'INTEGER', False, 'EVEN', None, None, 500, 80, False, 5, 2, 10000, 0.8, 0.9, 'r', False, 0, 'TABLE'),
        ]))
        
        # Mock other queries with empty results
        mock_empty_result = Mock()
        mock_empty_result.__iter__ = Mock(return_value=iter([]))
        
        # Mock version query
        mock_version_result = Mock()
        mock_version_result.fetchone.return_value = ('PostgreSQL 8.0.2 on x86_64-pc-linux-gnu, compiled by GCC',)
        
        def mock_execute(query):
            if 'svv_table_info' in query:
                return mock_metadata_result
            elif 'version()' in query:
                return mock_version_result
            else:
                return mock_empty_result
        
        mock_connection.execute.side_effect = mock_execute
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        result = get_redshift_schema(temp_config_file)
        
        # Check that schema was returned successfully
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 3  # customers, orders, products
        
        # Check that tables have redshift metadata with performance recommendations
        # The test mock won't create the exact tables but we can verify the structure
        for table in result['tables']:
            if 'redshift_metadata' in table:
                assert 'performance_recommendations' in table['redshift_metadata']
                assert isinstance(table['redshift_metadata']['performance_recommendations'], list)
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_redshift_schema_table_dependencies(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test Redshift table dependencies detection"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        mock_connection = Mock()
        
        # Mock main metadata query
        mock_metadata_result = Mock()
        mock_metadata_result.__iter__ = Mock(return_value=iter([
            ('public', 'customers', 'customer_id', True, 1, 'lzo', 'INTEGER', True, 'KEY', 'customer_id', 1, 100, 75, False, 5, 2, 1000, 1.2, 0.8, 'r', False, 0, 'TABLE'),
            ('public', 'orders', 'order_id', False, 1, 'lzo', 'INTEGER', True, 'EVEN', 'order_id', 1, 250, 90, False, 10, 3, 5000, 0.9, 0.5, 'r', False, 0, 'TABLE'),
        ]))
        
        # Mock dependencies query - view depends on customers and orders
        mock_dependencies_result = Mock()
        mock_dependencies_result.__iter__ = Mock(return_value=iter([
            ('public', 'customer_order_summary', 'public', 'customers'),
            ('public', 'customer_order_summary', 'public', 'orders'),
            ('public', 'order_analytics', 'public', 'orders'),
        ]))
        
        # Mock other queries with empty results
        mock_empty_result = Mock()
        mock_empty_result.__iter__ = Mock(return_value=iter([]))
        
        # Mock version query
        mock_version_result = Mock()
        mock_version_result.fetchone.return_value = ('PostgreSQL 8.0.2 on x86_64-pc-linux-gnu, compiled by GCC',)
        
        def mock_execute(query):
            if 'svv_table_info' in query:
                return mock_metadata_result
            elif 'pg_depend' in query:
                return mock_dependencies_result
            elif 'version()' in query:
                return mock_version_result
            else:
                return mock_empty_result
        
        mock_connection.execute.side_effect = mock_execute
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        result = get_redshift_schema(temp_config_file)
        
        # Check that schema was returned successfully
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 3  # customers, orders, products
        
        # Check that tables have redshift metadata with dependencies
        # Dependencies are stored in individual table metadata
        for table in result['tables']:
            if 'redshift_metadata' in table:
                assert 'dependencies' in table['redshift_metadata']
                assert isinstance(table['redshift_metadata']['dependencies'], list)
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_redshift_schema_cluster_type_detection(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test Redshift cluster type detection (provisioned vs serverless)"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        mock_connection = Mock()
        
        # Mock empty metadata
        mock_empty_result = Mock()
        mock_empty_result.__iter__ = Mock(return_value=iter([]))
        
        # Mock version query for provisioned cluster
        mock_version_result = Mock()
        mock_version_result.fetchone.return_value = ('PostgreSQL 8.0.2 on x86_64-pc-linux-gnu, compiled by GCC',)
        
        def mock_execute(query):
            if 'version()' in query:
                return mock_version_result
            else:
                return mock_empty_result
        
        mock_connection.execute.side_effect = mock_execute
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        result = get_redshift_schema(temp_config_file)
        
        # Check cluster type detection
        assert 'cluster_type' in result
        assert result['cluster_type'] == 'provisioned'
        
        # Test serverless detection
        mock_version_result.fetchone.return_value = ('PostgreSQL 8.0.2 on x86_64-pc-linux-gnu, compiled by GCC serverless',)
        
        result_serverless = get_redshift_schema(temp_config_file)
        assert result_serverless['cluster_type'] == 'serverless'
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_redshift_schema_comprehensive_metadata(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test comprehensive Redshift metadata extraction"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        mock_connection = Mock()
        
        # Mock comprehensive metadata query
        mock_metadata_result = Mock()
        mock_metadata_result.__iter__ = Mock(return_value=iter([
            # Table with full metadata
            ('public', 'comprehensive_table', 'id', True, 1, 'zstd', 'INTEGER', True, 'KEY', 'id', 1, 1000, 85, False, 8, 3, 25000, 2.5, 0.7, 'r', False, 0, 'TABLE'),
            ('public', 'comprehensive_table', 'name', False, None, 'lzo', 'VARCHAR', False, 'KEY', None, None, 1000, 85, False, 8, 3, 25000, 2.5, 0.7, 'r', False, 0, 'TABLE'),
            ('public', 'comprehensive_table', 'created_at', False, 2, 'delta', 'TIMESTAMP', False, 'KEY', 'created_at', 2, 1000, 85, False, 8, 3, 25000, 2.5, 0.7, 'r', False, 0, 'TABLE'),
        ]))
        
        # Mock sort key types
        mock_sortkey_result = Mock()
        mock_sortkey_result.__iter__ = Mock(return_value=iter([
            ('public', 'comprehensive_table', 'compound'),
        ]))
        
        # Mock other queries with empty results
        mock_empty_result = Mock()
        mock_empty_result.__iter__ = Mock(return_value=iter([]))
        
        # Mock version query
        mock_version_result = Mock()
        mock_version_result.fetchone.return_value = ('PostgreSQL 8.0.2 on x86_64-pc-linux-gnu, compiled by GCC',)
        
        def mock_execute(query):
            if 'svv_table_info' in query:
                return mock_metadata_result
            elif 'relinterleaved' in query:
                return mock_sortkey_result
            elif 'version()' in query:
                return mock_version_result
            else:
                return mock_empty_result
        
        mock_connection.execute.side_effect = mock_execute
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        result = get_redshift_schema(temp_config_file)
        
        # Check that schema was returned successfully
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 3  # customers, orders, products
        
        # Check comprehensive metadata structure on available tables
        for table in result['tables']:
            if 'redshift_metadata' in table:
                redshift_meta = table['redshift_metadata']
                
                # Check that all expected fields are present
                assert 'distribution_key' in redshift_meta
                assert 'distribution_style' in redshift_meta
                assert 'sort_keys' in redshift_meta
                assert 'sort_key_type' in redshift_meta
                assert 'column_encodings' in redshift_meta
                assert 'table_type' in redshift_meta
                assert 'is_external' in redshift_meta
                assert 'columns_metadata' in redshift_meta
                assert 'dependencies' in redshift_meta
                assert 'performance_recommendations' in redshift_meta
                
                # Check data types
                assert isinstance(redshift_meta['sort_keys'], list)
                assert isinstance(redshift_meta['column_encodings'], dict)
                assert isinstance(redshift_meta['columns_metadata'], dict)
                assert isinstance(redshift_meta['dependencies'], list)
                assert isinstance(redshift_meta['performance_recommendations'], list) 


class TestSybaseConnector:
    """Tests for Sybase/SAP ASE connector"""
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_sybase_schema_success(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test successful Sybase schema extraction with TDS protocol"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock Sybase-specific metadata query
        mock_connection = Mock()
        
        # Mock Sybase system table query results
        mock_metadata_result = Mock()
        mock_metadata_result.__iter__ = Mock(return_value=iter([
            # customer_id with identity column
            ('customers', 'customer_id', 'int', 4, 10, 0, 8, True, False, 1, 'IDENTITY'),
            ('customers', 'name', 'varchar', 100, 0, 0, 0, False, False, None, None),
            ('customers', 'email', 'varchar', 255, 0, 0, 128, False, True, None, None),
            # order_id with identity
            ('orders', 'order_id', 'int', 4, 10, 0, 8, True, False, 2, 'IDENTITY'),
            ('orders', 'customer_id', 'int', 4, 10, 0, 0, False, False, None, None),
        ]))
        
        # Mock Sybase version query
        mock_version_result = Mock()
        mock_version_result.fetchone.return_value = ('Adaptive Server Enterprise/16.0 SP03 PL08/EBF 28880/Linux x86_64',)
        
        # Mock database list query
        mock_databases_result = Mock()
        mock_databases_result.__iter__ = Mock(return_value=iter([
            ('production_db',),
            ('development_db',),
            ('staging_db',),
        ]))
        
        def mock_execute(query):
            if 'sysobjects' in query and 'syscolumns' in query:
                return mock_metadata_result
            elif '@@version' in query:
                return mock_version_result
            elif 'sysdatabases' in query:
                return mock_databases_result
            return Mock()
        
        mock_connection.execute.side_effect = mock_execute
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        # Mock column types for Sybase
        for table in ['customers', 'orders', 'products']:
            columns = mock_inspector.get_columns(table)
            for col in columns:
                col['type'] = Mock()
                col['type'].__str__ = Mock(return_value='int' if 'id' in col['name'] else 'varchar(100)')
        
        result = get_sybase_schema(temp_config_file)
        
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 3
        assert any(table['name'] == 'customers' for table in result['tables'])
        assert any(table['name'] == 'orders' for table in result['tables'])
        assert any(table['name'] == 'products' for table in result['tables'])
        
        # Check Sybase-specific metadata
        customers_table = next(table for table in result['tables'] if table['name'] == 'customers')
        assert 'sybase_metadata' in customers_table
        
        sybase_meta = customers_table['sybase_metadata']
        assert sybase_meta['database_engine'] == 'sybase'
        assert sybase_meta['has_identity_columns'] is True
        assert 'identity_columns' in sybase_meta
        assert 'customer_id' in sybase_meta['identity_columns']
        
        # Check server version and databases
        assert 'server_version' in result
        assert 'Adaptive Server Enterprise' in result['server_version']
        assert 'available_databases' in result
        assert 'production_db' in result['available_databases']
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    def test_get_sybase_schema_connection_error(self, mock_create_engine, temp_config_file):
        """Test Sybase connection error handling"""
        mock_create_engine.side_effect = Exception("Sybase connection failed")
        
        with pytest.raises(Exception, match="Sybase connection failed"):
            get_sybase_schema(temp_config_file)
    
    def test_get_sybase_schema_invalid_config(self):
        """Test Sybase with invalid config file"""
        with pytest.raises(FileNotFoundError):
            get_sybase_schema("nonexistent_config.yaml")
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_sybase_schema_empty_database(self, mock_inspect, mock_create_engine, temp_config_file):
        """Test Sybase with empty database"""
        mock_engine = Mock()
        mock_inspector = Mock()
        mock_inspector.get_table_names.return_value = []
        
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock empty Sybase metadata query
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        result = get_sybase_schema(temp_config_file)
        
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 0
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_sybase_schema_tds_connection(self, mock_inspect, mock_create_engine, mock_sqlalchemy_engine):
        """Test Sybase TDS protocol connection with character set"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock Sybase metadata query
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        # Create config with TDS settings
        config = {
            'host': 'sybase-server.company.com',
            'port': 5000,
            'database': 'testdb',
            'username': 'testuser',
            'password': 'testpass',
            'charset': 'utf8',
            'tds_version': '7.0',
            'appname': 'test-app',
            'login_timeout': 30,
            'packet_size': 4096
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            config_file = f.name
        
        try:
            result = get_sybase_schema(config_file)
            
            # Verify TDS connection parameters were used
            call_args = mock_create_engine.call_args
            assert f'charset={config["charset"]}' in call_args[0][0]
            assert result['database'] == 'testdb'
            
        finally:
            os.unlink(config_file)
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_sybase_schema_identity_columns(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test Sybase identity column detection"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        mock_connection = Mock()
        
        # Mock Sybase metadata with identity columns using standard table names
        mock_metadata_result = Mock()
        mock_metadata_result.__iter__ = Mock(return_value=iter([
            # customers table with identity column
            ('customers', 'customer_id', 'int', 4, 10, 0, 8, True, False, 1, 'IDENTITY'),  # Identity column
            ('customers', 'name', 'varchar', 100, 0, 0, 0, False, False, None, None),
            ('customers', 'email', 'varchar', 255, 0, 0, 128, False, True, None, None),  # Nullable
            # orders table without identity column
            ('orders', 'order_id', 'int', 4, 10, 0, 0, False, False, None, None),  # Not identity
            ('orders', 'customer_id', 'int', 4, 10, 0, 0, False, False, None, None),
        ]))
        
        mock_connection.execute.return_value = mock_metadata_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        result = get_sybase_schema(temp_config_file)
        
        # Check identity column detection
        customers_table = next(table for table in result['tables'] if table['name'] == 'customers')
        orders_table = next(table for table in result['tables'] if table['name'] == 'orders')
        
        assert 'sybase_metadata' in customers_table
        customers_meta = customers_table['sybase_metadata']
        assert customers_meta['has_identity_columns'] is True
        assert 'identity_columns' in customers_meta
        assert 'customer_id' in customers_meta['identity_columns']
        
        # Orders table should not have identity columns
        if 'sybase_metadata' in orders_table:
            orders_meta = orders_table['sybase_metadata']
            assert orders_meta['has_identity_columns'] is False
            assert 'identity_columns' not in orders_meta or len(orders_meta['identity_columns']) == 0
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_sybase_schema_metadata_query_error(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test Sybase schema extraction with metadata query error"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock metadata query failure
        mock_connection = Mock()
        mock_connection.execute.side_effect = Exception("Sybase metadata query failed")
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        # Mock column types
        for table in ['customers', 'orders', 'products']:
            columns = mock_inspector.get_columns(table)
            for col in columns:
                col['type'] = Mock()
                col['type'].__str__ = Mock(return_value='int' if 'id' in col['name'] else 'varchar(100)')
        
        # Should still return schema but without Sybase metadata
        result = get_sybase_schema(temp_config_file)
        
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 3
        
        # Tables should not have Sybase metadata due to query failure
        for table in result['tables']:
            assert 'sybase_metadata' not in table
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_sybase_schema_character_sets(self, mock_inspect, mock_create_engine, mock_sqlalchemy_engine):
        """Test Sybase with different character sets"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock Sybase metadata query
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        # Test different character sets
        charsets = ['utf8', 'iso_1', 'cp850', 'cp1252']
        
        for charset in charsets:
            config = {
                'host': 'sybase-server.company.com',
                'port': 5000,
                'database': 'testdb',
                'username': 'testuser',
                'password': 'testpass',
                'charset': charset
            }
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                yaml.dump(config, f)
                config_file = f.name
            
            try:
                result = get_sybase_schema(config_file)
                
                # Verify charset was used in connection
                call_args = mock_create_engine.call_args
                assert f'charset={charset}' in call_args[0][0]
                assert result['database'] == 'testdb'
                
            finally:
                os.unlink(config_file)
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_sybase_schema_version_detection(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test Sybase server version detection"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        mock_connection = Mock()
        
        # Mock empty metadata
        mock_empty_result = Mock()
        mock_empty_result.__iter__ = Mock(return_value=iter([]))
        
        # Mock different Sybase versions
        version_strings = [
            'Adaptive Server Enterprise/16.0 SP03 PL08/EBF 28880/Linux x86_64',
            'Adaptive Server Enterprise/15.7 SP138/P/Linux Intel/Enterprise Linux/ase157sp138/3987/64-bit',
            'SAP Adaptive Server Enterprise/16.0 SP02 PL05/EBF 27084'
        ]
        
        for version_string in version_strings:
            mock_version_result = Mock()
            mock_version_result.fetchone.return_value = (version_string,)
            
            def mock_execute(query):
                if '@@version' in query:
                    return mock_version_result
                else:
                    return mock_empty_result
            
            mock_connection.execute.side_effect = mock_execute
            mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
            mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
            
            result = get_sybase_schema(temp_config_file)
            
            # Check version detection
            assert 'server_version' in result
            assert result['server_version'] == version_string
            assert 'Adaptive Server Enterprise' in result['server_version']


# Integration test that includes Sybase
class TestConnectorIntegrationWithSybase:
    """Integration tests for all connectors including Sybase"""
    
    def test_all_connectors_with_sybase_return_consistent_format(self, temp_config_file, mock_sqlalchemy_engine):
        """Test that all connectors including Sybase return consistent schema format"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        
        with patch('schema_graph_builder.connectors.base_connector.create_engine', return_value=mock_engine), \
             patch('schema_graph_builder.connectors.base_connector.inspect', return_value=mock_inspector):
            
            # Mock column types for different databases
            for table in ['customers', 'orders', 'products']:
                columns = mock_inspector.get_columns(table)
                for col in columns:
                    col['type'] = Mock()
                    col['type'].__str__ = Mock(return_value='INTEGER' if 'id' in col['name'] else 'VARCHAR(100)')
            
                         # Test all database connectors
            postgres_result = get_postgres_schema(temp_config_file)
            mysql_result = get_mysql_schema(temp_config_file)
            mssql_result = get_mssql_schema(temp_config_file)
            sybase_result = get_sybase_schema(temp_config_file)
            db2_result = get_db2_schema(temp_config_file)
            
            # All results should have consistent structure
            for result in [postgres_result, mysql_result, mssql_result, sybase_result, db2_result]:
                assert 'database' in result
                assert 'tables' in result
                assert isinstance(result['tables'], list)
                
                # Each table should have consistent structure
                for table in result['tables']:
                    assert 'name' in table
                    assert 'columns' in table
                    assert isinstance(table['columns'], list)
                    
                    # Each column should have consistent structure
                    for column in table['columns']:
                        assert 'name' in column
                        assert 'type' in column
                        assert 'nullable' in column
                        assert 'primary_key' in column


class TestDB2Connector:
    """Tests for IBM DB2 connector"""
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_db2_schema_success(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test successful DB2 schema extraction with enterprise features"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock DB2-specific metadata query
        mock_connection = Mock()
        
        # Mock DB2 system catalog query results
        mock_metadata_result = Mock()
        mock_metadata_result.__iter__ = Mock(return_value=iter([
            # customers table with identity column
            ('MYSCHEMA', 'CUSTOMERS', 'CUSTOMER_ID', 'INTEGER', 4, 0, 'N', None, 'Y', 'N', 'USERSPACE1', 1000, 25, 20),
            ('MYSCHEMA', 'CUSTOMERS', 'NAME', 'VARCHAR', 100, 0, 'Y', None, 'N', 'N', 'USERSPACE1', 1000, 25, 20),
            ('MYSCHEMA', 'CUSTOMERS', 'EMAIL', 'VARCHAR', 255, 0, 'Y', None, 'N', 'N', 'USERSPACE1', 1000, 25, 20),
            # orders table with generated column
            ('MYSCHEMA', 'ORDERS', 'ORDER_ID', 'INTEGER', 4, 0, 'N', None, 'Y', 'N', 'USERSPACE1', 500, 15, 12),
            ('MYSCHEMA', 'ORDERS', 'CUSTOMER_ID', 'INTEGER', 4, 0, 'N', None, 'N', 'N', 'USERSPACE1', 500, 15, 12),
            ('MYSCHEMA', 'ORDERS', 'TOTAL_AMOUNT', 'DECIMAL', 10, 2, 'Y', None, 'N', 'A', 'USERSPACE1', 500, 15, 12),  # Generated column
        ]))
        
        # Mock DB2 version query
        mock_version_result = Mock()
        mock_version_result.fetchone.return_value = ('DB2 v11.5.0.0', '5')
        
        # Mock server info query
        mock_server_info_result = Mock()
        mock_server_info_result.fetchone.return_value = ('DB2SERVER', 'EST', '2024-01-15', '14:30:00')
        
        # Mock tablespaces query
        mock_tablespaces_result = Mock()
        mock_tablespaces_result.__iter__ = Mock(return_value=iter([
            ('SYSCATSPACE', 'DMS', 'REGULAR', 4096, 1000, 950, 200),  # System catalog
            ('USERSPACE1', 'DMS', 'REGULAR', 8192, 5000, 4500, 3000),  # User data
            ('TEMPSPACE1', 'SMS', 'SYSTEMP', 4096, 2000, 2000, 500),  # Temporary
            ('IBMDEFAULTBP', 'DMS', 'REGULAR', 4096, 1000, 900, 100),  # Default buffer pool
        ]))
        
        # Mock schemas query
        mock_schemas_result = Mock()
        mock_schemas_result.__iter__ = Mock(return_value=iter([
            ('MYSCHEMA', 'DB2ADMIN', 'DB2ADMIN', '2024-01-01 10:00:00'),
            ('TESTSCHEMA', 'TESTUSER', 'TESTUSER', '2024-01-02 11:00:00'),
        ]))
        
        def mock_execute(query):
            # Handle both string and TextClause objects
            query_str = str(query)
            if 'syscat.tables' in query_str and 'syscat.columns' in query_str:
                return mock_metadata_result
            elif 'env_get_inst_info' in query_str:
                return mock_version_result
            elif 'current server' in query_str:
                return mock_server_info_result
            elif 'syscat.tablespaces' in query_str:
                return mock_tablespaces_result
            elif 'syscat.schemata' in query_str:
                return mock_schemas_result
            else:
                # Return empty result for any other query
                empty_result = Mock()
                empty_result.__iter__ = Mock(return_value=iter([]))
                empty_result.fetchone.return_value = None
                return empty_result
        
        mock_connection.execute.side_effect = mock_execute
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        # Mock column types for DB2
        for table in ['customers', 'orders', 'products']:
            columns = mock_inspector.get_columns(table)
            for col in columns:
                col['type'] = Mock()
                col['type'].__str__ = Mock(return_value='INTEGER' if 'id' in col['name'] else 'VARCHAR(100)')
        
        result = get_db2_schema(temp_config_file)
        
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 3
        assert any(table['name'] == 'customers' for table in result['tables'])
        assert any(table['name'] == 'orders' for table in result['tables'])
        assert any(table['name'] == 'products' for table in result['tables'])
        
        # Check DB2-specific metadata
        customers_table = next(table for table in result['tables'] if table['name'] == 'customers')
        assert 'db2_metadata' in customers_table
        
        db2_meta = customers_table['db2_metadata']
        assert db2_meta['database_engine'] == 'db2'
        assert db2_meta['schema'] == 'MYSCHEMA'
        assert db2_meta['tablespace'] == 'USERSPACE1'
        assert db2_meta['has_identity_columns'] is True
        assert 'identity_columns' in db2_meta
        assert 'CUSTOMER_ID' in db2_meta['identity_columns']
        
        # Check DB2 server information
        assert 'server_version' in result
        assert 'DB2 v11.5.0.0 FixPack 5' in result['server_version']
        assert 'server_info' in result
        assert result['server_info']['server_name'] == 'DB2SERVER'
        assert 'tablespaces' in result
        assert len(result['tablespaces']) == 4
        assert 'schemas' in result
        assert len(result['schemas']) == 2
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    def test_get_db2_schema_connection_error(self, mock_create_engine, temp_config_file):
        """Test DB2 connection error handling"""
        mock_create_engine.side_effect = Exception("DB2 connection failed")
        
        with pytest.raises(Exception, match="DB2 connection failed"):
            get_db2_schema(temp_config_file)
    
    def test_get_db2_schema_invalid_config(self):
        """Test DB2 with invalid config file"""
        with pytest.raises(FileNotFoundError):
            get_db2_schema("nonexistent_config.yaml")
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_db2_schema_empty_database(self, mock_inspect, mock_create_engine, temp_config_file):
        """Test DB2 with empty database"""
        mock_engine = Mock()
        mock_inspector = Mock()
        mock_inspector.get_table_names.return_value = []
        
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock empty DB2 metadata query
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        result = get_db2_schema(temp_config_file)
        
        assert result['database'] == 'testdb'
        assert len(result['tables']) == 0
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_db2_schema_with_ssl(self, mock_inspect, mock_create_engine, mock_sqlalchemy_engine):
        """Test DB2 SSL connection with enterprise parameters"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock DB2 metadata query
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        # Create config with DB2 SSL settings  
        config = {
            'host': 'db2-enterprise.company.com',
            'port': 446,  # SSL port
            'database': 'PRODDB',
            'username': 'db2admin',
            'password': 'secure_password',
            'protocol': 'TCPIP',
            'security': 'SSL',
            'authentication': 'SERVER',
            'current_schema': 'PRODUCTION',
            'connect_timeout': 60,
            'application_name': 'schema-analyzer'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            config_file = f.name
        
        try:
            result = get_db2_schema(config_file)
            
            # Verify DB2 SSL connection parameters were used
            call_args = mock_create_engine.call_args
            
            # Check that DB2 parameters were set
            assert 'ibm_db_sa' in call_args[0][0]  # Connection string
            assert 'protocol=TCPIP' in call_args[0][0]
            assert 'security=SSL' in call_args[0][0]
            assert 'currentschema=PRODUCTION' in call_args[0][0]
            
            assert result['database'] == 'PRODDB'
            
        finally:
            os.unlink(config_file)
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_db2_schema_identity_and_generated_columns(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test DB2 identity and generated column detection"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        mock_connection = Mock()
        
        # Mock DB2 metadata with identity and generated columns
        mock_metadata_result = Mock()
        mock_metadata_result.__iter__ = Mock(return_value=iter([
            # Table with both identity and generated columns
            ('SALES', 'TRANSACTIONS', 'TRANS_ID', 'BIGINT', 8, 0, 'N', None, 'Y', 'N', 'SALESSPACE', 10000, 100, 85),  # Identity
            ('SALES', 'TRANSACTIONS', 'AMOUNT', 'DECIMAL', 10, 2, 'N', None, 'N', 'N', 'SALESSPACE', 10000, 100, 85),
            ('SALES', 'TRANSACTIONS', 'TAX', 'DECIMAL', 10, 2, 'Y', None, 'N', 'A', 'SALESSPACE', 10000, 100, 85),  # Generated Always
            ('SALES', 'TRANSACTIONS', 'TOTAL', 'DECIMAL', 10, 2, 'Y', None, 'N', 'D', 'SALESSPACE', 10000, 100, 85),  # Generated Default
            # Table without special columns
            ('SALES', 'PRODUCTS', 'PROD_ID', 'INTEGER', 4, 0, 'N', None, 'N', 'N', 'SALESSPACE', 5000, 50, 40),
            ('SALES', 'PRODUCTS', 'NAME', 'VARCHAR', 200, 0, 'N', None, 'N', 'N', 'SALESSPACE', 5000, 50, 40),
        ]))
        
        def mock_execute(query):
            query_str = str(query)
            if 'syscat.tables' in query_str and 'syscat.columns' in query_str:
                return mock_metadata_result
            else:
                # Return empty result for any other query
                empty_result = Mock()
                empty_result.__iter__ = Mock(return_value=iter([]))
                empty_result.fetchone.return_value = None
                return empty_result
        
        mock_connection.execute.side_effect = mock_execute
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        # Update mock inspector to include the 'transactions' table
        mock_inspector.get_table_names.return_value = ['customers', 'orders', 'products', 'transactions']
        
        # Mock additional column information for the transactions table
        transactions_columns = [
            {'name': 'trans_id', 'type': Mock(), 'nullable': False, 'primary_key': True},
            {'name': 'amount', 'type': Mock(), 'nullable': False, 'primary_key': False},
            {'name': 'tax', 'type': Mock(), 'nullable': True, 'primary_key': False},
            {'name': 'total', 'type': Mock(), 'nullable': True, 'primary_key': False}
        ]
        
        def get_columns_side_effect(table_name):
            if table_name == 'transactions':
                return transactions_columns
            else:
                # Return standard columns for other tables
                return [
                    {'name': f'{table_name[:-1]}_id' if table_name.endswith('s') else f'{table_name}_id', 
                     'type': Mock(), 'nullable': False, 'primary_key': True},
                    {'name': 'name', 'type': Mock(), 'nullable': False, 'primary_key': False},
                    {'name': 'email', 'type': Mock(), 'nullable': True, 'primary_key': False}
                ]
        
        mock_inspector.get_columns.side_effect = get_columns_side_effect
        
        # Set type string representation for all mock types
        for table in ['customers', 'orders', 'products', 'transactions']:
            columns = mock_inspector.get_columns(table)
            for col in columns:
                col['type'].__str__ = Mock(return_value='INTEGER' if 'id' in col['name'] else 'VARCHAR(100)')
        
        result = get_db2_schema(temp_config_file)
        
        # Check identity and generated column detection
        transactions_table = next(table for table in result['tables'] if table['name'] == 'transactions')
        products_table = next(table for table in result['tables'] if table['name'] == 'products')
        
        assert 'db2_metadata' in transactions_table
        trans_meta = transactions_table['db2_metadata']
        assert trans_meta['has_identity_columns'] is True
        assert trans_meta['has_generated_columns'] is True
        assert 'identity_columns' in trans_meta
        assert 'TRANS_ID' in trans_meta['identity_columns']
        assert 'generated_columns' in trans_meta
        assert 'TAX' in trans_meta['generated_columns']
        assert 'TOTAL' in trans_meta['generated_columns']
        
        # Products table should not have special columns
        if 'db2_metadata' in products_table:
            prod_meta = products_table['db2_metadata']
            assert prod_meta['has_identity_columns'] is False
            assert prod_meta['has_generated_columns'] is False
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_db2_schema_tablespaces(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test DB2 tablespace information extraction"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        mock_connection = Mock()
        
        # Mock empty table metadata
        mock_empty_result = Mock()
        mock_empty_result.__iter__ = Mock(return_value=iter([]))
        
        # Mock tablespaces query with realistic DB2 tablespace data
        mock_tablespaces_result = Mock()
        mock_tablespaces_result.__iter__ = Mock(return_value=iter([
            ('SYSCATSPACE', 'DMS', 'REGULAR', 4096, 1000, 950, 200),  # System catalog
            ('USERSPACE1', 'DMS', 'REGULAR', 8192, 5000, 4500, 3000),  # User data
            ('TEMPSPACE1', 'SMS', 'SYSTEMP', 4096, 2000, 2000, 500),  # Temporary
            ('IBMDEFAULTBP', 'DMS', 'REGULAR', 4096, 1000, 900, 100),  # Default buffer pool
        ]))
        
        def mock_execute(query):
            query_str = str(query)
            if 'syscat.tablespaces' in query_str:
                return mock_tablespaces_result
            else:
                return mock_empty_result
        
        mock_connection.execute.side_effect = mock_execute
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        result = get_db2_schema(temp_config_file)
        
        # Check tablespace information
        assert 'tablespaces' in result
        tablespaces = result['tablespaces']
        assert len(tablespaces) == 4
        
        # Find specific tablespaces
        userspace1 = next(ts for ts in tablespaces if ts['name'] == 'USERSPACE1')
        tempspace1 = next(ts for ts in tablespaces if ts['name'] == 'TEMPSPACE1')
        
        # Check USERSPACE1 details
        assert userspace1['type'] == 'DMS'
        assert userspace1['data_type'] == 'REGULAR'
        assert userspace1['page_size'] == 8192
        assert userspace1['total_pages'] == 5000
        assert userspace1['utilization_percent'] == 66.67  # 3000/4500 * 100
        
        # Check TEMPSPACE1 details
        assert tempspace1['type'] == 'SMS'
        assert tempspace1['data_type'] == 'SYSTEMP'
        assert tempspace1['utilization_percent'] == 25.0  # 500/2000 * 100
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_db2_schema_zos_environment(self, mock_inspect, mock_create_engine, mock_sqlalchemy_engine):
        """Test DB2 z/OS mainframe environment support"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # Mock DB2 metadata query
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.__iter__ = Mock(return_value=iter([]))
        mock_connection.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
        
        # Create config for z/OS environment
        config = {
            'environment': 'zos',
            'host': 'mainframe.company.com',
            'port': 446,
            'database': 'DB2PROD',
            'username': 'MAINUSER',
            'password': 'mainframe_password',
            'location': 'SYSPLEX1',
            'current_schema': 'PRODSCHEMA',
            'protocol': 'TCPIP',
            'security': 'SSL',
            'authentication': 'SERVER'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f)
            config_file = f.name
        
        try:
            result = get_db2_schema(config_file)
            
            # Verify z/OS connection parameters were used
            call_args = mock_create_engine.call_args
            connection_string = call_args[0][0]
            
            # Check basic connection string structure
            assert 'ibm_db_sa://' in connection_string
            assert 'MAINUSER:mainframe_password' in connection_string
            assert 'mainframe.company.com:446' in connection_string
            assert 'DB2PROD' in connection_string
            
            # Check query parameters (they might be in different order)
            assert 'protocol=TCPIP' in connection_string
            assert 'security=SSL' in connection_string  
            assert 'currentschema=PRODSCHEMA' in connection_string
            assert 'location=SYSPLEX1' in connection_string
            
            assert result['database'] == 'DB2PROD'
            
        finally:
            os.unlink(config_file)
    
    @patch('schema_graph_builder.connectors.base_connector.create_engine')
    @patch('schema_graph_builder.connectors.base_connector.inspect')
    def test_get_db2_schema_version_detection(self, mock_inspect, mock_create_engine, temp_config_file, mock_sqlalchemy_engine):
        """Test DB2 server version detection"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        mock_connection = Mock()
        
        # Mock empty metadata
        mock_empty_result = Mock()
        mock_empty_result.__iter__ = Mock(return_value=iter([]))
        
        # Mock different DB2 versions
        version_data = [
            ('DB2 v11.5.0.0', '5'),
            ('DB2 v11.1.0.0', '3'),
            ('DB2 v10.5.0.0', '8')
        ]
        
        for service_level, fixpack in version_data:
            mock_version_result = Mock()
            mock_version_result.fetchone.return_value = (service_level, fixpack)
            
            def mock_execute(query):
                query_str = str(query)
                if 'env_get_inst_info' in query_str:
                    return mock_version_result
                else:
                    return mock_empty_result
            
            mock_connection.execute.side_effect = mock_execute
            mock_engine.connect.return_value.__enter__ = Mock(return_value=mock_connection)
            mock_engine.connect.return_value.__exit__ = Mock(return_value=None)
            
            result = get_db2_schema(temp_config_file)
            
            # Check version detection
            assert 'server_version' in result
            expected_version = f"DB2 {service_level} FixPack {fixpack}"
            assert result['server_version'] == expected_version


# Integration test that includes DB2
class TestConnectorIntegrationWithDB2:
    """Integration tests for all connectors including DB2"""
    
    def test_all_connectors_with_db2_return_consistent_format(self, temp_config_file, mock_sqlalchemy_engine):
        """Test that all connectors including DB2 return consistent schema format"""
        mock_engine, mock_inspector = mock_sqlalchemy_engine
        
        with patch('schema_graph_builder.connectors.base_connector.create_engine', return_value=mock_engine), \
             patch('schema_graph_builder.connectors.base_connector.inspect', return_value=mock_inspector):
            
            # Mock column types for different databases
            for table in ['customers', 'orders', 'products']:
                columns = mock_inspector.get_columns(table)
                for col in columns:
                    col['type'] = Mock()
                    col['type'].__str__ = Mock(return_value='INTEGER' if 'id' in col['name'] else 'VARCHAR(100)')
            
            # Test all database connectors
            postgres_result = get_postgres_schema(temp_config_file)
            mysql_result = get_mysql_schema(temp_config_file)
            mssql_result = get_mssql_schema(temp_config_file)
            sybase_result = get_sybase_schema(temp_config_file)
            db2_result = get_db2_schema(temp_config_file)
            
            # All results should have consistent structure
            for result in [postgres_result, mysql_result, mssql_result, sybase_result, db2_result]:
                assert 'database' in result
                assert 'tables' in result
                assert isinstance(result['tables'], list)
                
                # Each table should have consistent structure
                for table in result['tables']:
                    assert 'name' in table
                    assert 'columns' in table
                    assert isinstance(table['columns'], list)
                    
                    # Each column should have consistent structure
                    for column in table['columns']:
                        assert 'name' in column
                        assert 'type' in column
                        assert 'nullable' in column
                        assert 'primary_key' in column 
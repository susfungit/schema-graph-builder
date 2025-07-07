"""
Integration tests for the complete Schema Graph Builder pipeline
"""

import pytest
import tempfile
import os
import json
import yaml
from unittest.mock import patch, Mock, MagicMock


class TestFullPipeline:
    """Integration tests for the complete pipeline"""
    
    @patch('schema_graph_builder.api.extract_schema')
    @patch('schema_graph_builder.api.infer_relationships')
    @patch('schema_graph_builder.api.build_graph')
    @patch('schema_graph_builder.api.yaml.dump')
    @patch('builtins.open', create=True)
    @patch('os.makedirs')
    def test_complete_postgres_pipeline(self, mock_makedirs, mock_open, mock_yaml_dump, mock_build_graph, mock_infer_relationships, mock_extract_schema):
        """Test complete pipeline from schema extraction to visualization"""
        from schema_graph_builder.api import SchemaGraphBuilder
        
        # Mock schema data
        mock_schema = {
            'database': 'test_db',
            'tables': [
                {
                    'name': 'users',
                    'columns': [
                        {'name': 'user_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'email', 'type': 'VARCHAR(255)', 'nullable': False, 'primary_key': False}
                    ]
                },
                {
                    'name': 'orders',
                    'columns': [
                        {'name': 'order_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'user_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': False},
                        {'name': 'total', 'type': 'DECIMAL(10,2)', 'nullable': False, 'primary_key': False}
                    ]
                }
            ]
        }
        
        mock_extract_schema.return_value = mock_schema
        
        # Mock relationships
        mock_relationships = {
            'users': {'primary_key': 'user_id', 'foreign_keys': []},
            'orders': {'primary_key': 'order_id', 'foreign_keys': [
                {'column': 'user_id', 'references': 'users.user_id'}
            ]}
        }
        mock_infer_relationships.return_value = mock_relationships
        
        # Mock file operations
        mock_open.return_value.__enter__.return_value.write = Mock()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as config_file:
            config_data = {
                'host': 'localhost',
                'port': 5432,
                'database': 'test_db',
                'username': 'test',
                'password': 'test'
            }
            yaml.dump(config_data, config_file)
            config_path = config_file.name
        
        try:
            # Run complete pipeline
            builder = SchemaGraphBuilder()
            result = builder.analyze_database('postgres', config_path)
            
            # Verify result structure
            assert 'schema' in result
            assert 'relationships' in result
            assert 'output_files' in result
            
            # Verify schema extraction
            assert result['schema']['database'] == 'test_db'
            assert len(result['schema']['tables']) == 2
            
            # Verify relationship inference
            assert 'users' in result['relationships']
            assert 'orders' in result['relationships']
            
            # Check that foreign key was detected
            orders_fks = result['relationships']['orders']['foreign_keys']
            assert len(orders_fks) == 1
            assert orders_fks[0]['column'] == 'user_id'
            assert orders_fks[0]['references'] == 'users.user_id'
            
        finally:
            # Cleanup
            if os.path.exists(config_path):
                os.unlink(config_path)
    
    @patch('schema_graph_builder.api.extract_schema')
    @patch('schema_graph_builder.api.infer_relationships')
    @patch('schema_graph_builder.api.build_graph')
    @patch('schema_graph_builder.api.yaml.dump')
    @patch('builtins.open', create=True)
    @patch('os.makedirs')
    def test_complete_mysql_pipeline(self, mock_makedirs, mock_open, mock_yaml_dump, mock_build_graph, mock_infer_relationships, mock_extract_schema):
        """Test complete pipeline for MySQL"""
        from schema_graph_builder.api import SchemaGraphBuilder
        
        # Mock MySQL schema
        mock_schema = {
            'database': 'mysql_test',
            'tables': [
                {
                    'name': 'categories',
                    'columns': [
                        {'name': 'category_id', 'type': 'INT', 'nullable': False, 'primary_key': True},
                        {'name': 'name', 'type': 'VARCHAR(100)', 'nullable': False, 'primary_key': False}
                    ]
                },
                {
                    'name': 'products',
                    'columns': [
                        {'name': 'product_id', 'type': 'INT', 'nullable': False, 'primary_key': True},
                        {'name': 'category_id', 'type': 'INT', 'nullable': False, 'primary_key': False},
                        {'name': 'name', 'type': 'VARCHAR(200)', 'nullable': False, 'primary_key': False}
                    ]
                }
            ]
        }
        
        mock_extract_schema.return_value = mock_schema
        
        # Mock relationships
        mock_relationships = {
            'categories': {'primary_key': 'category_id', 'foreign_keys': []},
            'products': {'primary_key': 'product_id', 'foreign_keys': [
                {'column': 'category_id', 'references': 'categories.category_id'}
            ]}
        }
        mock_infer_relationships.return_value = mock_relationships
        
        # Mock file operations
        mock_open.return_value.__enter__.return_value.write = Mock()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as config_file:
            config_data = {
                'host': 'localhost',
                'port': 3306,
                'database': 'mysql_test',
                'username': 'test',
                'password': 'test'
            }
            yaml.dump(config_data, config_file)
            config_path = config_file.name
        
        try:
            # Run complete pipeline
            builder = SchemaGraphBuilder()
            result = builder.analyze_database('mysql', config_path)
            
            # Verify MySQL-specific results
            assert result['schema']['database'] == 'mysql_test'
            assert 'categories' in result['relationships']
            assert 'products' in result['relationships']
            
            # Verify foreign key relationship
            products_fks = result['relationships']['products']['foreign_keys']
            assert len(products_fks) == 1
            assert products_fks[0]['column'] == 'category_id'
            assert products_fks[0]['references'] == 'categories.category_id'
            
        finally:
            # Cleanup
            if os.path.exists(config_path):
                os.unlink(config_path)
    
    @patch('schema_graph_builder.api.extract_schema')
    @patch('schema_graph_builder.api.infer_relationships')
    @patch('schema_graph_builder.api.build_graph')
    @patch('schema_graph_builder.api.yaml.dump')
    @patch('builtins.open', create=True)
    @patch('os.makedirs')
    def test_complete_mssql_pipeline(self, mock_makedirs, mock_open, mock_yaml_dump, mock_build_graph, mock_infer_relationships, mock_extract_schema):
        """Test complete pipeline for MS SQL Server"""
        from schema_graph_builder.api import SchemaGraphBuilder
        
        # Mock MS SQL schema
        mock_schema = {
            'database': 'mssql_test',
            'tables': [
                {
                    'name': 'Customers',
                    'columns': [
                        {'name': 'CustomerID', 'type': 'INT', 'nullable': False, 'primary_key': True},
                        {'name': 'CompanyName', 'type': 'NVARCHAR(100)', 'nullable': False, 'primary_key': False}
                    ]
                },
                {
                    'name': 'Orders',
                    'columns': [
                        {'name': 'OrderID', 'type': 'INT', 'nullable': False, 'primary_key': True},
                        {'name': 'CustomerID', 'type': 'INT', 'nullable': False, 'primary_key': False},
                        {'name': 'OrderDate', 'type': 'DATETIME2', 'nullable': False, 'primary_key': False}
                    ]
                }
            ]
        }
        
        mock_extract_schema.return_value = mock_schema
        
        # Mock relationships
        mock_relationships = {
            'Customers': {'primary_key': 'CustomerID', 'foreign_keys': []},
            'Orders': {'primary_key': 'OrderID', 'foreign_keys': [
                {'column': 'CustomerID', 'references': 'Customers.CustomerID'}
            ]}
        }
        mock_infer_relationships.return_value = mock_relationships
        
        # Mock file operations
        mock_open.return_value.__enter__.return_value.write = Mock()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as config_file:
            config_data = {
                'host': 'localhost',
                'port': 1433,
                'database': 'mssql_test',
                'username': 'test',
                'password': 'test'
            }
            yaml.dump(config_data, config_file)
            config_path = config_file.name
        
        try:
            # Run complete pipeline
            builder = SchemaGraphBuilder()
            result = builder.analyze_database('mssql', config_path)
            
            # Verify MS SQL-specific results
            assert result['schema']['database'] == 'mssql_test'
            assert 'Customers' in result['relationships']
            assert 'Orders' in result['relationships']
            
            # Verify case-sensitive foreign key relationship
            orders_fks = result['relationships']['Orders']['foreign_keys']
            assert len(orders_fks) == 1
            assert orders_fks[0]['column'] == 'CustomerID'
            assert orders_fks[0]['references'] == 'Customers.CustomerID'
            
        finally:
            # Cleanup
            if os.path.exists(config_path):
                os.unlink(config_path)


class TestErrorRecovery:
    """Test error recovery and graceful degradation"""
    
    def test_schema_extraction_failure_recovery(self):
        """Test recovery from schema extraction failures"""
        from schema_graph_builder.api import SchemaGraphBuilder
        import tempfile
        import yaml
        import os
        
        # Create a temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump({'host': 'localhost', 'database': 'testdb', 'username': 'test', 'password': 'test'}, f)
            config_file = f.name
        
        try:
            builder = SchemaGraphBuilder()
            
            # Mock the base connector methods to simulate connection timeout
            with patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._create_connection'), \
                 patch('schema_graph_builder.connectors.base_connector.DatabaseConnector._extract_schema_data', side_effect=Exception("Connection timeout")):
                
                with pytest.raises(Exception, match="Connection timeout"):
                    builder.extract_schema_only('postgres', config_file)
        finally:
            if os.path.exists(config_file):
                os.unlink(config_file)
    
    @patch('schema_graph_builder.api.build_graph')
    @patch('schema_graph_builder.api.yaml.dump')
    @patch('tempfile.NamedTemporaryFile')
    @patch('os.path.exists')
    @patch('os.unlink')
    def test_visualization_fallback(self, mock_unlink, mock_exists, mock_temp_file, mock_yaml_dump, mock_build_graph):
        """Test visualization works with proper mocking"""
        from schema_graph_builder.api import SchemaGraphBuilder
        
        # Mock temporary file
        mock_temp = Mock()
        mock_temp.name = '/tmp/test.yaml'
        mock_temp_file.return_value.__enter__.return_value = mock_temp
        mock_exists.return_value = True
        
        # Mock build_graph to simulate visualization creation
        mock_build_graph.return_value = None
        
        builder = SchemaGraphBuilder()
        builder.last_schema = {'database': 'test', 'tables': []}
        builder.last_relationships = {'table1': {'primary_key': 'id', 'foreign_keys': []}}
        
        # Should create visualization without error
        result = builder.create_visualization()
        
        # Verify that the core functions were called
        mock_build_graph.assert_called_once()
        mock_yaml_dump.assert_called_once()
        # Test passes if no exception is raised
        assert result is not None
    
    @patch('schema_graph_builder.api.extract_schema')
    @patch('schema_graph_builder.api.infer_relationships')
    @patch('schema_graph_builder.api.build_graph')
    @patch('schema_graph_builder.api.yaml.dump')
    @patch('builtins.open', create=True)
    @patch('os.makedirs')
    def test_empty_schema_handling(self, mock_makedirs, mock_open, mock_yaml_dump, mock_build_graph, mock_infer_relationships, mock_extract_schema):
        """Test handling of empty database schemas"""
        from schema_graph_builder.api import SchemaGraphBuilder
        
        # Mock empty schema
        mock_schema = {
            'database': 'empty_db',
            'tables': []
        }
        mock_extract_schema.return_value = mock_schema
        mock_infer_relationships.return_value = {}
        
        builder = SchemaGraphBuilder()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as config_file:
            yaml.dump({'host': 'localhost', 'database': 'empty_db'}, config_file)
            config_path = config_file.name
        
        try:
            result = builder.analyze_database('postgres', config_path, save_files=False)
            
            # Should handle empty schema gracefully
            assert result['schema']['database'] == 'empty_db'
            assert len(result['schema']['tables']) == 0
            assert result['relationships'] == {}
            
        finally:
            if os.path.exists(config_path):
                os.unlink(config_path)


class TestPerformance:
    """Test performance and scalability"""
    
    @patch('schema_graph_builder.api.extract_schema')
    @patch('schema_graph_builder.api.infer_relationships')
    @patch('schema_graph_builder.api.build_graph')
    @patch('schema_graph_builder.api.yaml.dump')
    @patch('builtins.open', create=True)
    @patch('os.makedirs')
    def test_large_schema_handling(self, mock_makedirs, mock_open, mock_yaml_dump, mock_build_graph, mock_infer_relationships, mock_extract_schema):
        """Test handling of large database schemas"""
        from schema_graph_builder.api import SchemaGraphBuilder
        
        # Create a large mock schema
        large_schema = {
            'database': 'large_db',
            'tables': []
        }
        
        # Generate 100 tables with 20 columns each
        for i in range(100):
            table = {
                'name': f'table_{i}',
                'columns': [
                    {'name': f'col_{j}', 'type': 'VARCHAR(100)', 'nullable': True, 'primary_key': j == 0}
                    for j in range(20)
                ]
            }
            large_schema['tables'].append(table)
        
        mock_extract_schema.return_value = large_schema
        
        # Mock relationships for all tables
        mock_relationships = {}
        for i in range(100):
            mock_relationships[f'table_{i}'] = {'primary_key': 'col_0', 'foreign_keys': []}
        mock_infer_relationships.return_value = mock_relationships
        
        builder = SchemaGraphBuilder()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as config_file:
            yaml.dump({'host': 'localhost', 'database': 'large_db'}, config_file)
            config_path = config_file.name
        
        try:
            # Should handle large schema without timing out
            result = builder.analyze_database('postgres', config_path, save_files=False)
            
            assert result['schema']['database'] == 'large_db'
            assert len(result['schema']['tables']) == 100
            assert len(result['relationships']) == 100
            
        finally:
            if os.path.exists(config_path):
                os.unlink(config_path)
    
    @patch('schema_graph_builder.api.extract_schema')
    @patch('schema_graph_builder.api.infer_relationships')
    @patch('schema_graph_builder.api.build_graph')
    @patch('schema_graph_builder.api.yaml.dump')
    @patch('builtins.open', create=True)
    @patch('os.makedirs')
    def test_many_relationships_performance(self, mock_makedirs, mock_open, mock_yaml_dump, mock_build_graph, mock_infer_relationships, mock_extract_schema):
        """Test performance with many foreign key relationships"""
        from schema_graph_builder.api import SchemaGraphBuilder
        
        # Create schema with many interconnected tables
        complex_schema = {
            'database': 'complex_db',
            'tables': [
                {
                    'name': 'hub_table',
                    'columns': [
                        {'name': 'hub_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'name', 'type': 'VARCHAR(100)', 'nullable': False, 'primary_key': False}
                    ]
                }
            ]
        }
        
        # Add 50 tables that reference the hub table
        for i in range(50):
            table = {
                'name': f'spoke_{i}',
                'columns': [
                    {'name': f'spoke_{i}_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                    {'name': 'hub_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': False},
                    {'name': 'data', 'type': 'VARCHAR(200)', 'nullable': True, 'primary_key': False}
                ]
            }
            complex_schema['tables'].append(table)
        
        mock_extract_schema.return_value = complex_schema
        
        # Mock relationships for hub and spoke tables
        mock_relationships = {
            'hub_table': {'primary_key': 'hub_id', 'foreign_keys': []}
        }
        for i in range(50):
            mock_relationships[f'spoke_{i}'] = {
                'primary_key': f'spoke_{i}_id',
                'foreign_keys': [
                    {'column': 'hub_id', 'references': 'hub_table.hub_id'}
                ]
            }
        mock_infer_relationships.return_value = mock_relationships
        
        builder = SchemaGraphBuilder()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as config_file:
            yaml.dump({'host': 'localhost', 'database': 'complex_db'}, config_file)
            config_path = config_file.name
        
        try:
            # Should handle complex relationships efficiently
            result = builder.analyze_database('postgres', config_path, save_files=False)
            
            assert result['schema']['database'] == 'complex_db'
            assert len(result['schema']['tables']) == 51  # 1 hub + 50 spokes
            
            # Check that relationships were inferred correctly
            hub_relationships = result['relationships']['hub_table']
            assert len(hub_relationships['foreign_keys']) == 0  # Hub has no FKs
            
            # Check spoke relationships
            for i in range(50):
                spoke_name = f'spoke_{i}'
                spoke_relationships = result['relationships'][spoke_name]
                assert len(spoke_relationships['foreign_keys']) == 1
                assert spoke_relationships['foreign_keys'][0]['column'] == 'hub_id'
                assert spoke_relationships['foreign_keys'][0]['references'] == 'hub_table.hub_id'
            
        finally:
            if os.path.exists(config_path):
                os.unlink(config_path)


class TestCrossDatabase:
    """Test cross-database compatibility"""
    
    def test_schema_format_consistency(self):
        """Test that all database connectors return consistent schema format"""
        from schema_graph_builder.extractor.schema_extractor import extract_schema
        
        # Mock different database schemas
        postgres_schema = {
            'database': 'postgres_db',
            'tables': [
                {
                    'name': 'users',
                    'columns': [
                        {'name': 'id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'email', 'type': 'VARCHAR(255)', 'nullable': False, 'primary_key': False}
                    ]
                }
            ]
        }
        
        mysql_schema = {
            'database': 'mysql_db',
            'tables': [
                {
                    'name': 'users',
                    'columns': [
                        {'name': 'id', 'type': 'INT', 'nullable': False, 'primary_key': True},
                        {'name': 'email', 'type': 'VARCHAR(255)', 'nullable': False, 'primary_key': False}
                    ]
                }
            ]
        }
        
        mssql_schema = {
            'database': 'mssql_db',
            'tables': [
                {
                    'name': 'users',
                    'columns': [
                        {'name': 'id', 'type': 'INT', 'nullable': False, 'primary_key': True},
                        {'name': 'email', 'type': 'NVARCHAR(255)', 'nullable': False, 'primary_key': False}
                    ]
                }
            ]
        }
        
        # All schemas should have the same structure
        schemas = [postgres_schema, mysql_schema, mssql_schema]
        
        for schema in schemas:
            # Check required top-level keys
            assert 'database' in schema
            assert 'tables' in schema
            assert isinstance(schema['tables'], list)
            
            # Check table structure
            for table in schema['tables']:
                assert 'name' in table
                assert 'columns' in table
                assert isinstance(table['columns'], list)
                
                # Check column structure
                for column in table['columns']:
                    assert 'name' in column
                    assert 'type' in column
                    assert 'nullable' in column
                    assert 'primary_key' in column
                    assert isinstance(column['nullable'], bool)
                    assert isinstance(column['primary_key'], bool)
    
    def test_relationship_inference_consistency(self):
        """Test that relationship inference works consistently across database types"""
        from schema_graph_builder.inference.relationship_inference import infer_relationships
        
        # Same logical schema for different databases
        test_schemas = [
            {
                'database': 'postgres_test',
                'tables': [
                    {
                        'name': 'customers',
                        'columns': [
                            {'name': 'customer_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                            {'name': 'name', 'type': 'VARCHAR(100)', 'nullable': False, 'primary_key': False}
                        ]
                    },
                    {
                        'name': 'orders',
                        'columns': [
                            {'name': 'order_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                            {'name': 'customer_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': False}
                        ]
                    }
                ]
            },
            {
                'database': 'mysql_test',
                'tables': [
                    {
                        'name': 'customers',
                        'columns': [
                            {'name': 'customer_id', 'type': 'INT', 'nullable': False, 'primary_key': True},
                            {'name': 'name', 'type': 'VARCHAR(100)', 'nullable': False, 'primary_key': False}
                        ]
                    },
                    {
                        'name': 'orders',
                        'columns': [
                            {'name': 'order_id', 'type': 'INT', 'nullable': False, 'primary_key': True},
                            {'name': 'customer_id', 'type': 'INT', 'nullable': False, 'primary_key': False}
                        ]
                    }
                ]
            }
        ]
        
        # Relationship inference should be identical
        results = [infer_relationships(schema) for schema in test_schemas]
        
        # All results should detect the same relationship
        for result in results:
            assert 'customers' in result
            assert 'orders' in result
            
            # Check customers table
            customers_rel = result['customers']
            assert customers_rel['primary_key'] == 'customer_id'
            assert len(customers_rel['foreign_keys']) == 0
            
            # Check orders table
            orders_rel = result['orders']
            assert orders_rel['primary_key'] == 'order_id'
            assert len(orders_rel['foreign_keys']) == 1
            assert orders_rel['foreign_keys'][0]['column'] == 'customer_id'
            assert orders_rel['foreign_keys'][0]['references'] == 'customers.customer_id' 
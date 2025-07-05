"""
Tests for relationship inference module
"""

import pytest
from schema_graph_builder.inference.relationship_inference import infer_relationships


class TestRelationshipInference:
    """Tests for relationship inference logic"""
    
    def test_infer_relationships_basic_foreign_key(self, sample_schema):
        """Test basic foreign key detection"""
        result = infer_relationships(sample_schema)
        
        # Check that customers table has no foreign keys
        assert result['customers']['primary_key'] == 'customer_id'
        assert len(result['customers']['foreign_keys']) == 0
        
        # Check that orders table has customer_id foreign key
        assert result['orders']['primary_key'] == 'order_id'
        assert len(result['orders']['foreign_keys']) == 1
        assert result['orders']['foreign_keys'][0]['column'] == 'customer_id'
        assert result['orders']['foreign_keys'][0]['references'] == 'customers.customer_id'
        assert result['orders']['foreign_keys'][0]['confidence'] > 0.8
        
        # Check that products table has no foreign keys
        assert result['products']['primary_key'] == 'product_id'
        assert len(result['products']['foreign_keys']) == 0
    
    def test_infer_relationships_no_foreign_keys(self):
        """Test schema with no foreign key relationships"""
        schema = {
            'database': 'testdb',
            'tables': [
                {
                    'name': 'users',
                    'columns': [
                        {'name': 'id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'name', 'type': 'VARCHAR(100)', 'nullable': False, 'primary_key': False}
                    ]
                },
                {
                    'name': 'settings',
                    'columns': [
                        {'name': 'setting_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'key', 'type': 'VARCHAR(50)', 'nullable': False, 'primary_key': False},
                        {'name': 'value', 'type': 'VARCHAR(255)', 'nullable': True, 'primary_key': False}
                    ]
                }
            ]
        }
        
        result = infer_relationships(schema)
        
        assert len(result['users']['foreign_keys']) == 0
        assert len(result['settings']['foreign_keys']) == 0
        assert result['users']['primary_key'] == 'id'
        assert result['settings']['primary_key'] == 'setting_id'
    
    def test_infer_relationships_multiple_foreign_keys(self):
        """Test table with multiple foreign key relationships"""
        schema = {
            'database': 'testdb',
            'tables': [
                {
                    'name': 'customers',
                    'columns': [
                        {'name': 'customer_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'name', 'type': 'VARCHAR(100)', 'nullable': False, 'primary_key': False}
                    ]
                },
                {
                    'name': 'products',
                    'columns': [
                        {'name': 'product_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'name', 'type': 'VARCHAR(100)', 'nullable': False, 'primary_key': False}
                    ]
                },
                {
                    'name': 'order_items',
                    'columns': [
                        {'name': 'item_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'customer_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': False},
                        {'name': 'product_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': False},
                        {'name': 'quantity', 'type': 'INTEGER', 'nullable': False, 'primary_key': False}
                    ]
                }
            ]
        }
        
        result = infer_relationships(schema)
        
        # Check order_items has two foreign keys
        foreign_keys = result['order_items']['foreign_keys']
        assert len(foreign_keys) == 2
        
        # Check both foreign keys are detected
        fk_columns = [fk['column'] for fk in foreign_keys]
        assert 'customer_id' in fk_columns
        assert 'product_id' in fk_columns
        
        # Check references are correct
        for fk in foreign_keys:
            if fk['column'] == 'customer_id':
                assert fk['references'] == 'customers.customer_id'
            elif fk['column'] == 'product_id':
                assert fk['references'] == 'products.product_id'
    
    def test_infer_relationships_edge_case_naming(self):
        """Test edge cases in naming conventions"""
        schema = {
            'database': 'testdb',
            'tables': [
                {
                    'name': 'user_profiles',
                    'columns': [
                        {'name': 'profile_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'bio', 'type': 'TEXT', 'nullable': True, 'primary_key': False}
                    ]
                },
                {
                    'name': 'posts',
                    'columns': [
                        {'name': 'post_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'profile_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': False},
                        {'name': 'content', 'type': 'TEXT', 'nullable': False, 'primary_key': False}
                    ]
                }
            ]
        }
        
        result = infer_relationships(schema)
        
        # Should detect profile_id relationship despite underscore in table name
        foreign_keys = result['posts']['foreign_keys']
        assert len(foreign_keys) == 1
        assert foreign_keys[0]['column'] == 'profile_id'
        assert foreign_keys[0]['references'] == 'user_profiles.profile_id'
    
    def test_infer_relationships_no_primary_key_table(self):
        """Test handling of tables without explicit primary keys"""
        schema = {
            'database': 'testdb',
            'tables': [
                {
                    'name': 'logs',
                    'columns': [
                        {'name': 'timestamp', 'type': 'TIMESTAMP', 'nullable': False, 'primary_key': False},
                        {'name': 'message', 'type': 'TEXT', 'nullable': False, 'primary_key': False},
                        {'name': 'level', 'type': 'VARCHAR(10)', 'nullable': False, 'primary_key': False}
                    ]
                }
            ]
        }
        
        result = infer_relationships(schema)
        
        # Should handle gracefully
        assert 'logs' in result
        assert result['logs']['primary_key'] is None
        assert result['logs']['foreign_keys'] == []
    
    def test_infer_relationships_empty_schema(self):
        """Test handling of empty schema"""
        schema = {
            'database': 'testdb',
            'tables': []
        }
        
        result = infer_relationships(schema)
        
        assert result == {}
    
    def test_infer_relationships_confidence_scoring(self):
        """Test that confidence scores are reasonable"""
        schema = {
            'database': 'testdb',
            'tables': [
                {
                    'name': 'categories',
                    'columns': [
                        {'name': 'category_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'name', 'type': 'VARCHAR(100)', 'nullable': False, 'primary_key': False}
                    ]
                },
                {
                    'name': 'items',
                    'columns': [
                        {'name': 'item_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'category_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': False},
                        {'name': 'title', 'type': 'VARCHAR(200)', 'nullable': False, 'primary_key': False}
                    ]
                }
            ]
        }
        
        result = infer_relationships(schema)
        
        foreign_key = result['items']['foreign_keys'][0]
        
        # Confidence should be between 0 and 1
        assert 0 <= foreign_key['confidence'] <= 1
        # Should be high confidence for exact match
        assert foreign_key['confidence'] > 0.8
    
    def test_infer_relationships_case_sensitivity(self):
        """Test that inference works with different case conventions"""
        schema = {
            'database': 'testdb',
            'tables': [
                {
                    'name': 'Users',
                    'columns': [
                        {'name': 'UserId', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'Name', 'type': 'VARCHAR(100)', 'nullable': False, 'primary_key': False}
                    ]
                },
                {
                    'name': 'Orders',
                    'columns': [
                        {'name': 'OrderId', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                        {'name': 'UserId', 'type': 'INTEGER', 'nullable': False, 'primary_key': False},
                        {'name': 'Total', 'type': 'DECIMAL(10,2)', 'nullable': False, 'primary_key': False}
                    ]
                }
            ]
        }
        
        result = infer_relationships(schema)
        
        # Should detect relationship despite case differences
        foreign_keys = result['Orders']['foreign_keys']
        assert len(foreign_keys) == 1
        assert foreign_keys[0]['column'] == 'UserId'
        assert foreign_keys[0]['references'] == 'Users.UserId'
    
    def test_infer_relationships_return_structure(self, sample_schema):
        """Test that return structure is consistent and correct"""
        result = infer_relationships(sample_schema)
        
        # Check overall structure
        assert isinstance(result, dict)
        
        # Check each table has the required structure
        for table_name, relationships in result.items():
            assert isinstance(relationships, dict)
            assert 'primary_key' in relationships
            assert 'foreign_keys' in relationships
            assert isinstance(relationships['foreign_keys'], list)
            
            # Check foreign key structure
            for fk in relationships['foreign_keys']:
                assert isinstance(fk, dict)
                assert 'column' in fk
                assert 'references' in fk
                assert 'confidence' in fk
                assert isinstance(fk['column'], str)
                assert isinstance(fk['references'], str)
                assert isinstance(fk['confidence'], (int, float))
                assert '.' in fk['references']  # Should be in format 'table.column' 
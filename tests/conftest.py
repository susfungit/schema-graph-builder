"""
Pytest configuration and shared fixtures for Schema Graph Builder tests
"""

import pytest
import tempfile
import os
import yaml
from unittest.mock import Mock, MagicMock
import sys

# Add the parent directory to the path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def sample_schema():
    """Fixture providing a sample database schema for testing"""
    return {
        'database': 'testdb',
        'tables': [
            {
                'name': 'customers',
                'columns': [
                    {'name': 'customer_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                    {'name': 'name', 'type': 'VARCHAR(100)', 'nullable': False, 'primary_key': False},
                    {'name': 'email', 'type': 'VARCHAR(255)', 'nullable': True, 'primary_key': False}
                ]
            },
            {
                'name': 'orders',
                'columns': [
                    {'name': 'order_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                    {'name': 'customer_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': False},
                    {'name': 'order_date', 'type': 'TIMESTAMP', 'nullable': False, 'primary_key': False},
                    {'name': 'total', 'type': 'DECIMAL(10,2)', 'nullable': True, 'primary_key': False}
                ]
            },
            {
                'name': 'products',
                'columns': [
                    {'name': 'product_id', 'type': 'INTEGER', 'nullable': False, 'primary_key': True},
                    {'name': 'name', 'type': 'VARCHAR(100)', 'nullable': False, 'primary_key': False},
                    {'name': 'price', 'type': 'DECIMAL(8,2)', 'nullable': False, 'primary_key': False}
                ]
            }
        ]
    }


@pytest.fixture
def sample_relationships():
    """Fixture providing sample relationship inference results"""
    return {
        'customers': {
            'primary_key': 'customer_id',
            'foreign_keys': []
        },
        'orders': {
            'primary_key': 'order_id',
            'foreign_keys': [
                {
                    'column': 'customer_id',
                    'references': 'customers.customer_id',
                    'confidence': 0.9
                }
            ]
        },
        'products': {
            'primary_key': 'product_id',
            'foreign_keys': []
        }
    }


@pytest.fixture
def temp_config_file():
    """Fixture providing a temporary configuration file"""
    config_data = {
        'host': 'localhost',
        'port': 5432,
        'database': 'testdb',
        'username': 'testuser',
        'password': 'testpass'
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_data, f)
        temp_file = f.name
    
    yield temp_file
    
    # Cleanup
    if os.path.exists(temp_file):
        os.unlink(temp_file)


@pytest.fixture
def temp_output_dir():
    """Fixture providing a temporary output directory"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def mock_sqlalchemy_engine():
    """Fixture providing a mock SQLAlchemy engine"""
    mock_engine = Mock()
    mock_inspector = Mock()
    
    # Mock table names
    mock_inspector.get_table_names.return_value = ['customers', 'orders', 'products']
    
    # Mock column details
    def mock_get_columns(table_name):
        columns_map = {
            'customers': [
                {'name': 'customer_id', 'type': Mock(), 'nullable': False, 'primary_key': 1},
                {'name': 'name', 'type': Mock(), 'nullable': False, 'primary_key': 0},
                {'name': 'email', 'type': Mock(), 'nullable': True, 'primary_key': 0}
            ],
            'orders': [
                {'name': 'order_id', 'type': Mock(), 'nullable': False, 'primary_key': 1},
                {'name': 'customer_id', 'type': Mock(), 'nullable': False, 'primary_key': 0},
                {'name': 'order_date', 'type': Mock(), 'nullable': False, 'primary_key': 0},
                {'name': 'total', 'type': Mock(), 'nullable': True, 'primary_key': 0}
            ],
            'products': [
                {'name': 'product_id', 'type': Mock(), 'nullable': False, 'primary_key': 1},
                {'name': 'name', 'type': Mock(), 'nullable': False, 'primary_key': 0},
                {'name': 'price', 'type': Mock(), 'nullable': False, 'primary_key': 0}
            ]
        }
        return columns_map.get(table_name, [])
    
    mock_inspector.get_columns.side_effect = mock_get_columns
    
    # Mock primary keys
    def mock_get_pk_constraint(table_name):
        pk_map = {
            'customers': {'constrained_columns': ['customer_id']},
            'orders': {'constrained_columns': ['order_id']},
            'products': {'constrained_columns': ['product_id']}
        }
        return pk_map.get(table_name, {'constrained_columns': []})
    
    mock_inspector.get_pk_constraint.side_effect = mock_get_pk_constraint
    
    return mock_engine, mock_inspector


@pytest.fixture
def mock_networkx_graph():
    """Fixture providing a mock NetworkX graph"""
    mock_graph = Mock()
    mock_graph.nodes.return_value = ['customers', 'orders', 'products']
    mock_graph.edges.return_value = [('orders', 'customers', {'label': 'customer_id'})]
    return mock_graph


@pytest.fixture
def sample_config_data():
    """Fixture providing sample configuration data"""
    return {
        'postgres': {
            'host': 'localhost',
            'port': 5432,
            'database': 'testdb',
            'username': 'testuser',
            'password': 'testpass'
        },
        'mysql': {
            'host': 'localhost',
            'port': 3306,
            'database': 'testdb',
            'username': 'testuser',
            'password': 'testpass'
        },
        'mssql': {
            'host': 'localhost',
            'port': 1433,
            'database': 'testdb',
            'username': 'testuser',
            'password': 'testpass'
        }
    } 
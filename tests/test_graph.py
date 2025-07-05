"""
Tests for graph builder module
"""

import pytest
import tempfile
import json
import os
import yaml
from unittest.mock import patch, Mock, mock_open
from schema_graph_builder.graph.graph_builder import build_graph, _create_fallback_html


class TestGraphBuilder:
    """Tests for graph building functionality"""
    
    @patch('schema_graph_builder.graph.graph_builder.nx.DiGraph')
    @patch('schema_graph_builder.graph.graph_builder.yaml.safe_load')
    @patch('builtins.open', new_callable=mock_open)
    def test_build_graph_basic(self, mock_file, mock_yaml, mock_digraph, sample_schema, sample_relationships, temp_output_dir):
        """Test basic graph building functionality"""
        mock_yaml.return_value = sample_relationships
        mock_graph = Mock()
        mock_digraph.return_value = mock_graph
        
        # Mock NetworkX operations
        mock_graph.add_node = Mock()
        mock_graph.add_edge = Mock()
        
        with patch('schema_graph_builder.graph.graph_builder.nx.readwrite.json_graph.node_link_data') as mock_node_link:
            mock_node_link.return_value = {'nodes': [], 'links': []}
            
            output_path = os.path.join(temp_output_dir, 'test_graph.json')
            build_graph(sample_schema, 'dummy_config.yaml', visualize=False, output_json_path=output_path)
            
            # Verify graph nodes were added
            assert mock_graph.add_node.call_count == 3  # 3 tables
            
            # Verify graph edges were added
            mock_graph.add_edge.assert_called()
    
    @patch('schema_graph_builder.graph.graph_builder.Network')
    @patch('schema_graph_builder.graph.graph_builder.nx.DiGraph')
    @patch('schema_graph_builder.graph.graph_builder.yaml.safe_load')
    @patch('builtins.open', new_callable=mock_open)
    def test_build_graph_with_visualization(self, mock_file, mock_yaml, mock_digraph, mock_network, sample_schema, sample_relationships):
        """Test graph building with visualization"""
        mock_yaml.return_value = sample_relationships
        mock_graph = Mock()
        mock_digraph.return_value = mock_graph
        mock_graph.nodes.return_value = ['customers', 'orders', 'products']
        mock_graph.edges.return_value = [('orders', 'customers', {'label': 'customer_id'})]
        
        mock_net = Mock()
        mock_network.return_value = mock_net
        mock_net.show = Mock()
        
        with patch('schema_graph_builder.graph.graph_builder.nx.readwrite.json_graph.node_link_data') as mock_node_link:
            mock_node_link.return_value = {'nodes': [], 'links': []}
            
            build_graph(sample_schema, 'dummy_config.yaml', visualize=True)
            
            # Verify visualization was created
            mock_network.assert_called_once()
            mock_net.add_node.assert_called()
            mock_net.add_edge.assert_called()
            mock_net.show.assert_called_once()
    
    @patch('schema_graph_builder.graph.graph_builder.Network')
    @patch('schema_graph_builder.graph.graph_builder.nx.DiGraph')
    @patch('schema_graph_builder.graph.graph_builder.yaml.safe_load')
    @patch('builtins.open', new_callable=mock_open)
    def test_build_graph_pyvis_fallback(self, mock_file, mock_yaml, mock_digraph, mock_network, sample_schema, sample_relationships):
        """Test fallback when pyvis fails"""
        mock_yaml.return_value = sample_relationships
        mock_graph = Mock()
        mock_digraph.return_value = mock_graph
        mock_graph.nodes.return_value = ['customers', 'orders', 'products']
        mock_graph.edges.return_value = [('orders', 'customers', {'label': 'customer_id'})]
        
        mock_net = Mock()
        mock_network.return_value = mock_net
        # Simulate pyvis failure
        mock_net.show.side_effect = AttributeError("'NoneType' object has no attribute 'render'")
        
        with patch('schema_graph_builder.graph.graph_builder.nx.readwrite.json_graph.node_link_data') as mock_node_link, \
             patch('schema_graph_builder.graph.graph_builder._create_fallback_html') as mock_fallback:
            mock_node_link.return_value = {'nodes': [], 'links': []}
            
            build_graph(sample_schema, 'dummy_config.yaml', visualize=True)
            
            # Verify fallback was called
            mock_fallback.assert_called_once()
    
    def test_create_fallback_html(self, mock_networkx_graph, temp_output_dir):
        """Test fallback HTML creation"""
        output_file = os.path.join(temp_output_dir, 'test_fallback.html')
        
        _create_fallback_html(mock_networkx_graph, output_file)
        
        # Verify file was created
        assert os.path.exists(output_file)
        
        # Verify HTML content
        with open(output_file, 'r') as f:
            content = f.read()
            assert '<!DOCTYPE html>' in content
            assert 'vis.Network' in content
            assert 'Database Schema Relationships' in content
    
    @patch('schema_graph_builder.graph.graph_builder.yaml.safe_load')
    @patch('builtins.open', new_callable=mock_open)
    def test_build_graph_invalid_config(self, mock_file, mock_yaml, sample_schema):
        """Test handling of invalid configuration file"""
        mock_yaml.side_effect = yaml.YAMLError("Invalid YAML")
        
        with pytest.raises(yaml.YAMLError):
            build_graph(sample_schema, 'invalid_config.yaml')
    
    @patch('schema_graph_builder.graph.graph_builder.nx.DiGraph')
    @patch('schema_graph_builder.graph.graph_builder.yaml.safe_load')
    @patch('builtins.open', new_callable=mock_open)
    def test_build_graph_empty_relationships(self, mock_file, mock_yaml, mock_digraph, sample_schema):
        """Test graph building with empty relationships"""
        mock_yaml.return_value = {}
        mock_graph = Mock()
        mock_digraph.return_value = mock_graph
        
        with patch('schema_graph_builder.graph.graph_builder.nx.readwrite.json_graph.node_link_data') as mock_node_link:
            mock_node_link.return_value = {'nodes': [], 'links': []}
            
            build_graph(sample_schema, 'empty_config.yaml', visualize=False)
            
            # Should still add nodes for tables
            assert mock_graph.add_node.call_count == 3
            # But no edges should be added
            mock_graph.add_edge.assert_not_called()
    
    def test_build_graph_file_permissions(self, sample_schema, sample_relationships):
        """Test handling of file permission errors"""
        with patch('schema_graph_builder.graph.graph_builder.yaml.safe_load', return_value=sample_relationships), \
             patch('builtins.open', side_effect=PermissionError("Permission denied")):
            
            with pytest.raises(PermissionError):
                build_graph(sample_schema, 'restricted_config.yaml')


class TestGraphFilenameGeneration:
    """Tests for graph filename generation logic"""
    
    @patch('schema_graph_builder.graph.graph_builder.nx.DiGraph')
    @patch('schema_graph_builder.graph.graph_builder.yaml.safe_load')
    @patch('builtins.open', new_callable=mock_open)
    @patch('schema_graph_builder.graph.graph_builder.Network')
    def test_mysql_filename_generation(self, mock_network, mock_file, mock_yaml, mock_digraph, sample_schema, sample_relationships):
        """Test MySQL-specific filename generation"""
        mock_yaml.return_value = sample_relationships
        mock_graph = Mock()
        mock_digraph.return_value = mock_graph
        mock_graph.nodes.return_value = []
        mock_graph.edges.return_value = []
        
        mock_net = Mock()
        mock_network.return_value = mock_net
        
        with patch('schema_graph_builder.graph.graph_builder.nx.readwrite.json_graph.node_link_data') as mock_node_link:
            mock_node_link.return_value = {'nodes': [], 'links': []}
            
            build_graph(sample_schema, 'config.yaml', visualize=True, output_json_path='output/mysql_schema_graph.json')
            
            # Should call show with mysql filename
            mock_net.show.assert_called_with('mysql_schema_graph.html')
    
    @patch('schema_graph_builder.graph.graph_builder.nx.DiGraph')
    @patch('schema_graph_builder.graph.graph_builder.yaml.safe_load')
    @patch('builtins.open', new_callable=mock_open)
    @patch('schema_graph_builder.graph.graph_builder.Network')
    def test_postgres_filename_generation(self, mock_network, mock_file, mock_yaml, mock_digraph, sample_schema, sample_relationships):
        """Test PostgreSQL-specific filename generation"""
        mock_yaml.return_value = sample_relationships
        mock_graph = Mock()
        mock_digraph.return_value = mock_graph
        mock_graph.nodes.return_value = []
        mock_graph.edges.return_value = []
        
        mock_net = Mock()
        mock_network.return_value = mock_net
        
        with patch('schema_graph_builder.graph.graph_builder.nx.readwrite.json_graph.node_link_data') as mock_node_link:
            mock_node_link.return_value = {'nodes': [], 'links': []}
            
            build_graph(sample_schema, 'config.yaml', visualize=True, output_json_path='output/postgres_schema_graph.json')
            
            # Should call show with postgres filename
            mock_net.show.assert_called_with('postgres_schema_graph.html')
    
    @patch('schema_graph_builder.graph.graph_builder.nx.DiGraph')
    @patch('schema_graph_builder.graph.graph_builder.yaml.safe_load')
    @patch('builtins.open', new_callable=mock_open)
    @patch('schema_graph_builder.graph.graph_builder.Network')
    def test_default_filename_generation(self, mock_network, mock_file, mock_yaml, mock_digraph, sample_schema, sample_relationships):
        """Test default filename generation"""
        mock_yaml.return_value = sample_relationships
        mock_graph = Mock()
        mock_digraph.return_value = mock_graph
        mock_graph.nodes.return_value = []
        mock_graph.edges.return_value = []
        
        mock_net = Mock()
        mock_network.return_value = mock_net
        
        with patch('schema_graph_builder.graph.graph_builder.nx.readwrite.json_graph.node_link_data') as mock_node_link:
            mock_node_link.return_value = {'nodes': [], 'links': []}
            
            build_graph(sample_schema, 'config.yaml', visualize=True, output_json_path='output/schema_graph.json')
            
            # Should call show with default filename
            mock_net.show.assert_called_with('schema_graph.html') 
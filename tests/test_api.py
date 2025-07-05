"""
Tests for the high-level API module
"""

import pytest
import tempfile
import os
from unittest.mock import patch, Mock
from schema_graph_builder.api import SchemaGraphBuilder


class TestSchemaGraphBuilder:
    """Tests for the SchemaGraphBuilder class"""
    
    def test_init(self):
        """Test SchemaGraphBuilder initialization"""
        builder = SchemaGraphBuilder()
        
        assert builder.last_schema is None
        assert builder.last_relationships is None
    
    @patch('schema_graph_builder.api.extract_schema')
    @patch('schema_graph_builder.api.infer_relationships')
    @patch('schema_graph_builder.api.build_graph')
    @patch('schema_graph_builder.api.yaml.dump')
    @patch('schema_graph_builder.api.os.makedirs')
    @patch('builtins.open')
    def test_analyze_database_success(self, mock_open, mock_makedirs, mock_yaml_dump, mock_build_graph, 
                                     mock_infer, mock_extract, sample_schema, sample_relationships, temp_config_file):
        """Test successful database analysis"""
        mock_extract.return_value = sample_schema
        mock_infer.return_value = sample_relationships
        
        builder = SchemaGraphBuilder()
        result = builder.analyze_database('postgres', temp_config_file, 'output', True, True)
        
        # Verify method calls
        mock_extract.assert_called_once_with('postgres', temp_config_file)
        mock_infer.assert_called_once_with(sample_schema)
        mock_build_graph.assert_called_once()
        
        # Verify result structure
        assert 'schema' in result
        assert 'relationships' in result
        assert 'output_files' in result
        assert result['schema'] == sample_schema
        assert result['relationships'] == sample_relationships
        
        # Verify state updates
        assert builder.last_schema == sample_schema
        assert builder.last_relationships == sample_relationships
    
    @patch('schema_graph_builder.api.extract_schema')
    @patch('schema_graph_builder.api.infer_relationships')
    def test_analyze_database_no_save_files(self, mock_infer, mock_extract, sample_schema, sample_relationships, temp_config_file):
        """Test database analysis without saving files"""
        mock_extract.return_value = sample_schema
        mock_infer.return_value = sample_relationships
        
        builder = SchemaGraphBuilder()
        result = builder.analyze_database('postgres', temp_config_file, save_files=False)
        
        # Should not have output_files when save_files=False
        assert len(result['output_files']) == 0
    
    def test_analyze_database_invalid_config(self):
        """Test analysis with non-existent config file"""
        builder = SchemaGraphBuilder()
        
        with pytest.raises(FileNotFoundError, match="Configuration file not found"):
            builder.analyze_database('postgres', 'nonexistent.yaml')
    
    @patch('schema_graph_builder.api.extract_schema')
    def test_extract_schema_only(self, mock_extract, sample_schema, temp_config_file):
        """Test schema extraction only"""
        mock_extract.return_value = sample_schema
        
        builder = SchemaGraphBuilder()
        result = builder.extract_schema_only('postgres', temp_config_file)
        
        mock_extract.assert_called_once_with('postgres', temp_config_file)
        assert result == sample_schema
        assert builder.last_schema == sample_schema
    
    @patch('schema_graph_builder.api.infer_relationships')
    def test_infer_relationships_only_with_schema(self, mock_infer, sample_schema, sample_relationships):
        """Test relationship inference with provided schema"""
        mock_infer.return_value = sample_relationships
        
        builder = SchemaGraphBuilder()
        result = builder.infer_relationships_only(sample_schema)
        
        mock_infer.assert_called_once_with(sample_schema)
        assert result == sample_relationships
        assert builder.last_relationships == sample_relationships
    
    @patch('schema_graph_builder.api.infer_relationships')
    def test_infer_relationships_only_with_last_schema(self, mock_infer, sample_schema, sample_relationships):
        """Test relationship inference using last extracted schema"""
        mock_infer.return_value = sample_relationships
        
        builder = SchemaGraphBuilder()
        builder.last_schema = sample_schema
        result = builder.infer_relationships_only()
        
        mock_infer.assert_called_once_with(sample_schema)
        assert result == sample_relationships
    
    def test_infer_relationships_only_no_schema(self):
        """Test relationship inference without schema"""
        builder = SchemaGraphBuilder()
        
        with pytest.raises(ValueError, match="No schema provided and no previous schema available"):
            builder.infer_relationships_only()
    
    @patch('schema_graph_builder.api.build_graph')
    @patch('schema_graph_builder.api.yaml.dump')
    @patch('tempfile.NamedTemporaryFile')
    @patch('os.path.exists')
    @patch('os.unlink')
    def test_create_visualization_success(self, mock_unlink, mock_exists, mock_temp, mock_yaml_dump, 
                                         mock_build_graph, sample_schema, sample_relationships):
        """Test successful visualization creation"""
        mock_temp_file = Mock()
        mock_temp_file.name = '/tmp/test_relationships.yaml'
        mock_temp.return_value.__enter__.return_value = mock_temp_file
        mock_exists.return_value = True
        
        builder = SchemaGraphBuilder()
        builder.last_schema = sample_schema
        builder.last_relationships = sample_relationships
        
        result = builder.create_visualization()
        
        mock_build_graph.assert_called_once()
        mock_unlink.assert_called_once_with('/tmp/test_relationships.yaml')
        assert result == 'schema_graph.html'
    
    def test_create_visualization_no_schema(self):
        """Test visualization creation without schema"""
        builder = SchemaGraphBuilder()
        
        with pytest.raises(ValueError, match="No schema provided and no previous schema available"):
            builder.create_visualization()
    
    def test_create_visualization_no_relationships(self, sample_schema):
        """Test visualization creation without relationships"""
        builder = SchemaGraphBuilder()
        builder.last_schema = sample_schema
        
        with pytest.raises(ValueError, match="No relationships provided and no previous relationships available"):
            builder.create_visualization()
    
    @patch('schema_graph_builder.api.extract_schema')
    def test_database_types_support(self, mock_extract, temp_config_file):
        """Test that all supported database types work"""
        mock_extract.return_value = {'database': 'test', 'tables': []}
        
        builder = SchemaGraphBuilder()
        
        # Test all supported database types
        for db_type in ['postgres', 'mysql', 'mssql']:
            result = builder.extract_schema_only(db_type, temp_config_file)
            assert result['database'] == 'test'
    
    @patch('schema_graph_builder.api.extract_schema')
    @patch('schema_graph_builder.api.infer_relationships')
    @patch('schema_graph_builder.api.build_graph')
    @patch('schema_graph_builder.api.yaml.dump')
    @patch('schema_graph_builder.api.os.makedirs')
    @patch('builtins.open')
    def test_output_files_structure(self, mock_open, mock_makedirs, mock_yaml_dump, mock_build_graph,
                                   mock_infer, mock_extract, sample_schema, sample_relationships, temp_config_file):
        """Test output files structure and paths"""
        mock_extract.return_value = sample_schema
        mock_infer.return_value = sample_relationships
        
        builder = SchemaGraphBuilder()
        result = builder.analyze_database('mysql', temp_config_file, 'custom_output', True, True)
        
        output_files = result['output_files']
        
        # Check output file paths
        assert 'relationships_yaml' in output_files
        assert 'graph_json' in output_files
        assert 'visualization_html' in output_files
        
        # Check custom output directory is used
        assert 'custom_output/mysql_inferred_relationships.yaml' in output_files['relationships_yaml']
        assert 'custom_output/mysql_schema_graph.json' in output_files['graph_json']
        assert 'mysql_schema_graph.html' in output_files['visualization_html']
    
    @patch('schema_graph_builder.api.extract_schema')
    def test_error_propagation(self, mock_extract, temp_config_file):
        """Test that errors from underlying modules are properly propagated"""
        mock_extract.side_effect = ValueError("Database connection failed")
        
        builder = SchemaGraphBuilder()
        
        with pytest.raises(ValueError, match="Database connection failed"):
            builder.extract_schema_only('postgres', temp_config_file)


class TestSchemaGraphBuilderIntegration:
    """Integration tests for SchemaGraphBuilder"""
    
    @patch('schema_graph_builder.api.extract_schema')
    @patch('schema_graph_builder.api.infer_relationships')
    @patch('schema_graph_builder.api.build_graph')
    @patch('schema_graph_builder.api.yaml.dump')
    @patch('schema_graph_builder.api.os.makedirs')
    @patch('builtins.open')
    def test_full_workflow(self, mock_open, mock_makedirs, mock_yaml_dump, mock_build_graph,
                          mock_infer, mock_extract, sample_schema, sample_relationships, temp_config_file):
        """Test complete workflow from start to finish"""
        mock_extract.return_value = sample_schema
        mock_infer.return_value = sample_relationships
        
        builder = SchemaGraphBuilder()
        
        # Step 1: Full analysis
        result = builder.analyze_database('postgres', temp_config_file)
        assert result['schema'] == sample_schema
        assert result['relationships'] == sample_relationships
        
        # Step 2: Use stored data for visualization
        visualization_path = builder.create_visualization(output_path='custom_graph.html')
        assert visualization_path == 'custom_graph.html'
        
        # Step 3: Extract only with new data
        new_schema = {'database': 'newdb', 'tables': []}
        mock_extract.return_value = new_schema
        schema_result = builder.extract_schema_only('mysql', temp_config_file)
        assert schema_result == new_schema
        assert builder.last_schema == new_schema  # Should update state
    
    def test_state_management(self, sample_schema, sample_relationships):
        """Test that internal state is managed correctly"""
        builder = SchemaGraphBuilder()
        
        # Initially empty
        assert builder.last_schema is None
        assert builder.last_relationships is None
        
        # Manual state setting
        builder.last_schema = sample_schema
        builder.last_relationships = sample_relationships
        
        # Should use stored state
        with patch('schema_graph_builder.api.infer_relationships') as mock_infer:
            mock_infer.return_value = sample_relationships
            result = builder.infer_relationships_only()
            mock_infer.assert_called_with(sample_schema)
        
        with patch('schema_graph_builder.api.build_graph'), \
             patch('schema_graph_builder.api.yaml.dump'), \
             patch('tempfile.NamedTemporaryFile') as mock_temp, \
             patch('os.path.exists', return_value=True), \
             patch('os.unlink'):
            
            mock_temp_file = Mock()
            mock_temp_file.name = '/tmp/test.yaml'
            mock_temp.return_value.__enter__.return_value = mock_temp_file
            
            result = builder.create_visualization()
            assert result == 'schema_graph.html' 
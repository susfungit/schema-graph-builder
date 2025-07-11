"""
Tests for CLI module
"""

import pytest
import argparse
import sys
from unittest.mock import patch, Mock
from schema_graph_builder.cli import main, get_database_config, display_relationships


class TestDatabaseConfig:
    """Tests for database configuration"""
    
    def test_get_database_config_postgres(self):
        """Test PostgreSQL configuration"""
        config = get_database_config('postgres')
        
        assert config is not None
        assert config['display_name'] == 'PostgreSQL'
        assert config['icon'] == '🐘'
        assert 'config_path' in config
        assert 'output_config' in config
        assert 'output_json' in config
        assert 'html_file' in config
    
    def test_get_database_config_mysql(self):
        """Test MySQL configuration"""
        config = get_database_config('mysql')
        
        assert config is not None
        assert config['display_name'] == 'MySQL'
        assert config['icon'] == '🐬'
        assert 'mysql' in config['config_path']
        assert 'mysql' in config['output_config']
    
    def test_get_database_config_mssql(self):
        """Test MS SQL Server configuration"""
        config = get_database_config('mssql')
        
        assert config is not None
        assert config['display_name'] == 'MS SQL Server'
        assert config['icon'] == '🏢'
        assert 'mssql' in config['config_path']
    
    def test_get_database_config_case_insensitive(self):
        """Test case insensitive database type"""
        config_lower = get_database_config('postgres')
        config_upper = get_database_config('POSTGRES')
        config_mixed = get_database_config('PoStGrEs')
        
        assert config_lower == config_upper == config_mixed
    
    def test_get_database_config_oracle(self):
        """Test Oracle database configuration"""
        config = get_database_config('oracle')
        
        assert config is not None
        assert config['display_name'] == 'Oracle Database'
        assert config['icon'] == '🔶'
        assert config['config_path'] == 'config/oracle_db_connections.yaml'
        assert config['output_config'] == 'output/oracle_inferred_relationships.yaml'
        assert config['output_json'] == 'output/oracle_schema_graph.json'
        assert config['html_file'] == 'oracle_schema_graph.html'
    
    def test_get_database_config_invalid(self):
        """Test invalid database type"""
        config = get_database_config('nosql')
        assert config is None


class TestDisplayRelationships:
    """Tests for relationship display function"""
    
    def test_display_relationships_basic(self, capsys):
        """Test basic relationship display"""
        relationships = {
            'orders': {
                'primary_key': 'order_id',
                'foreign_keys': [
                    {'column': 'customer_id', 'references': 'customers.customer_id', 'confidence': 0.9}
                ]
            }
        }
        
        display_relationships(relationships)
        captured = capsys.readouterr()
        
        assert "🔗 Inferred Relationships:" in captured.out
        assert "📋 orders:" in captured.out
        assert "Primary Key: order_id" in captured.out
        assert "🟢 customer_id → customers.customer_id" in captured.out
    
    def test_display_relationships_empty(self, capsys):
        """Test display with empty relationships"""
        relationships = {}
        
        display_relationships(relationships)
        captured = capsys.readouterr()
        
        assert "🔗 Inferred Relationships:" in captured.out
        assert "No foreign key relationships detected" in captured.out
    
    def test_display_relationships_no_foreign_keys(self, capsys):
        """Test display with tables that have no foreign keys"""
        relationships = {
            'users': {
                'primary_key': 'user_id',
                'foreign_keys': []
            }
        }
        
        display_relationships(relationships)
        captured = capsys.readouterr()
        
        assert "🔗 Inferred Relationships:" in captured.out
        assert "No foreign key relationships detected" in captured.out


class TestCLIMain:
    """Tests for main CLI function"""
    
    @patch('schema_graph_builder.cli.extract_schema')
    @patch('schema_graph_builder.cli.infer_relationships')
    @patch('schema_graph_builder.cli.build_graph')
    @patch('schema_graph_builder.cli.yaml.dump')
    @patch('schema_graph_builder.cli.os.makedirs')
    @patch('builtins.open')
    def test_main_postgres_success(self, mock_open, mock_makedirs, mock_yaml_dump, mock_build_graph,
                                  mock_infer, mock_extract, sample_schema, sample_relationships, monkeypatch):
        """Test successful PostgreSQL CLI execution"""
        mock_extract.return_value = sample_schema
        mock_infer.return_value = sample_relationships
        
        # Mock command line arguments
        test_args = ['cli.py', 'analyze', 'postgres']
        monkeypatch.setattr(sys, 'argv', test_args)
        
        with patch('schema_graph_builder.cli.os.path.dirname') as mock_dirname:
            mock_dirname.return_value = 'output'
            main()
        
        mock_extract.assert_called_once()
        mock_infer.assert_called_once()
        mock_build_graph.assert_called_once()
    
    @patch('schema_graph_builder.cli.extract_schema')
    def test_main_mysql_success(self, mock_extract, sample_schema, monkeypatch):
        """Test successful MySQL CLI execution"""
        mock_extract.return_value = sample_schema
        
        test_args = ['cli.py', 'analyze', 'mysql', '--quiet']
        monkeypatch.setattr(sys, 'argv', test_args)
        
        with patch('schema_graph_builder.cli.infer_relationships') as mock_infer, \
             patch('schema_graph_builder.cli.build_graph') as mock_build_graph, \
             patch('schema_graph_builder.cli.yaml.dump'), \
             patch('schema_graph_builder.cli.os.makedirs'), \
             patch('builtins.open'), \
             patch('schema_graph_builder.cli.os.path.dirname', return_value='output'):
            
            mock_infer.return_value = {}
            main()
            
            mock_extract.assert_called_once()
    
    @patch('schema_graph_builder.cli.extract_schema')
    def test_main_oracle_success(self, mock_extract, sample_schema, monkeypatch):
        """Test successful Oracle CLI execution"""
        mock_extract.return_value = sample_schema
        
        test_args = ['cli.py', 'analyze', 'oracle', '--quiet']
        monkeypatch.setattr(sys, 'argv', test_args)
        
        with patch('schema_graph_builder.cli.infer_relationships') as mock_infer, \
             patch('schema_graph_builder.cli.build_graph') as mock_build_graph, \
             patch('schema_graph_builder.cli.yaml.dump'), \
             patch('schema_graph_builder.cli.os.makedirs'), \
             patch('builtins.open'), \
             patch('schema_graph_builder.cli.os.path.dirname', return_value='output'):
            
            mock_infer.return_value = {}
            main()
            
            mock_extract.assert_called_once()
    
    def test_main_unsupported_database(self, monkeypatch, capsys):
        """Test CLI with unsupported database type"""
        test_args = ['cli.py', 'analyze', 'nosql']
        monkeypatch.setattr(sys, 'argv', test_args)
        
        with pytest.raises(SystemExit) as excinfo:
            main()
        
        assert excinfo.value.code == 2  # argparse exits with code 2 for invalid choices
        captured = capsys.readouterr()
        assert "invalid choice: 'nosql'" in captured.err
    
    @patch('schema_graph_builder.cli.extract_schema')
    def test_main_with_custom_config(self, mock_extract, sample_schema, monkeypatch):
        """Test CLI with custom configuration file"""
        mock_extract.return_value = sample_schema
        
        test_args = ['cli.py', 'analyze', 'postgres', '--config', 'custom_config.yaml']
        monkeypatch.setattr(sys, 'argv', test_args)
        
        with patch('schema_graph_builder.cli.infer_relationships') as mock_infer, \
             patch('schema_graph_builder.cli.build_graph') as mock_build_graph, \
             patch('schema_graph_builder.cli.yaml.dump'), \
             patch('schema_graph_builder.cli.os.makedirs'), \
             patch('builtins.open'), \
             patch('schema_graph_builder.cli.os.path.dirname', return_value='output'):
            
            mock_infer.return_value = {}
            main()
            
            # Should use custom config path
            mock_extract.assert_called_with('postgres', 'custom_config.yaml')
    
    @patch('schema_graph_builder.cli.extract_schema')
    def test_main_with_custom_output(self, mock_extract, sample_schema, monkeypatch):
        """Test CLI with custom output directory"""
        mock_extract.return_value = sample_schema
        
        test_args = ['cli.py', 'analyze', 'postgres', '--output', 'custom_output']
        monkeypatch.setattr(sys, 'argv', test_args)
        
        with patch('schema_graph_builder.cli.infer_relationships') as mock_infer, \
             patch('schema_graph_builder.cli.build_graph') as mock_build_graph, \
             patch('schema_graph_builder.cli.yaml.dump'), \
             patch('schema_graph_builder.cli.os.makedirs'), \
             patch('builtins.open'):
            
            mock_infer.return_value = {}
            main()
            
            # Verify custom output paths are used
            call_args = mock_build_graph.call_args
            assert 'custom_output' in str(call_args)
    
    def test_main_quiet_mode(self, monkeypatch, capsys):
        """Test CLI quiet mode"""
        test_args = ['cli.py', 'analyze', 'postgres', '--quiet']
        monkeypatch.setattr(sys, 'argv', test_args)
        
        with patch('schema_graph_builder.cli.extract_schema') as mock_extract, \
             patch('schema_graph_builder.cli.infer_relationships') as mock_infer, \
             patch('schema_graph_builder.cli.build_graph') as mock_build_graph, \
             patch('schema_graph_builder.cli.yaml.dump'), \
             patch('schema_graph_builder.cli.os.makedirs'), \
             patch('builtins.open'), \
             patch('schema_graph_builder.cli.os.path.dirname', return_value='output'):
            
            mock_extract.return_value = {'tables': []}
            mock_infer.return_value = {}
            main()
        
        captured = capsys.readouterr()
        # Should have minimal output in quiet mode
        assert "✅ PostgreSQL schema analysis completed" in captured.out
        # Should not have banner
        assert "PostgreSQL Schema Graph Builder" not in captured.out
    
    @patch('schema_graph_builder.cli.extract_schema')
    def test_main_exception_handling(self, mock_extract, monkeypatch, capsys):
        """Test CLI exception handling"""
        mock_extract.side_effect = Exception("Database connection failed")
        
        test_args = ['cli.py', 'analyze', 'postgres']
        monkeypatch.setattr(sys, 'argv', test_args)
        
        with pytest.raises(SystemExit) as excinfo:
            main()
        
        assert excinfo.value.code == 1
        captured = capsys.readouterr()
        assert "❌ Error during PostgreSQL schema analysis: Database connection failed" in captured.out
    
    @patch('schema_graph_builder.cli.extract_schema')
    def test_main_exception_handling_quiet(self, mock_extract, monkeypatch, capsys):
        """Test CLI exception handling in quiet mode"""
        mock_extract.side_effect = Exception("Connection error")
        
        test_args = ['cli.py', 'analyze', 'postgres', '--quiet']
        monkeypatch.setattr(sys, 'argv', test_args)
        
        with pytest.raises(SystemExit) as excinfo:
            main()
        
        assert excinfo.value.code == 1
        captured = capsys.readouterr()
        # Should not have traceback in quiet mode
        assert "Traceback" not in captured.out
    
    def test_argument_parser(self):
        """Test argument parser configuration"""
        with patch('sys.argv', ['cli.py', 'analyze', 'postgres']):
            parser = argparse.ArgumentParser(description='Database Schema Graph Builder')
            subparsers = parser.add_subparsers(dest='command', help='Commands')
            analyze_parser = subparsers.add_parser('analyze', help='Analyze database schema')
            analyze_parser.add_argument('database', choices=['postgres', 'mysql', 'mssql'])
            analyze_parser.add_argument('--config', type=str)
            analyze_parser.add_argument('--output', type=str)
            analyze_parser.add_argument('--quiet', action='store_true')
            
            args = parser.parse_args(['analyze', 'postgres'])
            assert args.command == 'analyze'
            assert args.database == 'postgres'
            assert args.config is None
            assert args.output is None
            assert args.quiet is False
            
            args = parser.parse_args(['analyze', 'mysql', '--config', 'test.yaml', '--quiet'])
            assert args.command == 'analyze'
            assert args.database == 'mysql'
            assert args.config == 'test.yaml'
            assert args.quiet is True


class TestCLIIntegration:
    """Integration tests for CLI"""
    
    def test_cli_help(self, monkeypatch, capsys):
        """Test CLI help output"""
        test_args = ['cli.py', 'analyze', '--help']
        monkeypatch.setattr(sys, 'argv', test_args)
        
        with pytest.raises(SystemExit) as excinfo:
            main()
        
        assert excinfo.value.code == 0
        captured = capsys.readouterr()
        assert "postgres" in captured.out
        assert "mysql" in captured.out
        assert "mssql" in captured.out
    
    def test_cli_invalid_database_choice(self, monkeypatch, capsys):
        """Test CLI with invalid database choice"""
        test_args = ['cli.py', 'analyze', 'invalid_db']
        monkeypatch.setattr(sys, 'argv', test_args)
        
        with pytest.raises(SystemExit) as excinfo:
            main()
        
        # Should exit with error code
        assert excinfo.value.code == 2
        captured = capsys.readouterr()
        assert "invalid choice" in captured.err 
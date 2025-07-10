#!/usr/bin/env python3
"""
Command-line interface for Schema Graph Builder with security enhancements

This module provides the CLI functionality with comprehensive security features.
"""

import argparse
import sys
import os
import yaml
from .extractor.schema_extractor import extract_schema
from .inference.relationship_inference import infer_relationships
from .graph.graph_builder import build_graph

# Optional security imports - gracefully handle missing dependencies
try:
    from .utils.config_validator import validate_database_config
    from .utils.security import encrypt_password_cli
    from .utils.logging_config import setup_logging, get_logger
    SECURITY_AVAILABLE = True
    logger = get_logger(__name__)
except ImportError:
    SECURITY_AVAILABLE = False
    import logging
    logger = logging.getLogger(__name__)
    
    # Fallback functions when security features are not available
    def validate_database_config(config_path, db_type):
        class ValidationResult:
            is_valid = True
            errors = []
            warnings = ["Security features not available - skipping validation"]
        return ValidationResult()
    
    def encrypt_password_cli(password):
        print("❌ Security features not available. Install cryptography to use password encryption.")
        sys.exit(1)
    
    def setup_logging(level="INFO", filename=None):
        logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO))


def get_database_config(db_type):
    """Get database-specific configuration paths and settings."""
    configs = {
        "postgres": {
            "config_path": "config/db_connections.yaml",
            "output_config": "output/inferred_relationships.yaml",
            "output_json": "output/schema_graph.json",
            "html_file": "schema_graph.html",
            "display_name": "PostgreSQL",
            "icon": "🐘"
        },
        "mysql": {
            "config_path": "config/mysql_db_connections.yaml",
            "output_config": "output/mysql_inferred_relationships.yaml",
            "output_json": "output/mysql_schema_graph.json",
            "html_file": "mysql_schema_graph.html",
            "display_name": "MySQL",
            "icon": "🐬"
        },
        "mssql": {
            "config_path": "config/mssql_db_connections.yaml",
            "output_config": "output/mssql_inferred_relationships.yaml",
            "output_json": "output/mssql_schema_graph.json",
            "html_file": "mssql_schema_graph.html",
            "display_name": "MS SQL Server",
            "icon": "🏢"
        },
        "oracle": {
            "config_path": "config/oracle_db_connections.yaml",
            "output_config": "output/oracle_inferred_relationships.yaml",
            "output_json": "output/oracle_schema_graph.json",
            "html_file": "oracle_schema_graph.html",
            "display_name": "Oracle Database",
            "icon": "🔶"
        },
        "redshift": {
            "config_path": "config/redshift_db_connections.yaml",
            "output_config": "output/redshift_inferred_relationships.yaml",
            "output_json": "output/redshift_schema_graph.json",
            "html_file": "redshift_schema_graph.html",
            "display_name": "Amazon Redshift",
            "icon": "🔴"
        },
        "sybase": {
            "config_path": "config/sybase_db_connections.yaml",
            "output_config": "output/sybase_inferred_relationships.yaml",
            "output_json": "output/sybase_schema_graph.json",
            "html_file": "sybase_schema_graph.html",
            "display_name": "Sybase/SAP ASE",
            "icon": "🗂️"
        },
        "db2": {
            "config_path": "config/db2_db_connections.yaml",
            "output_config": "output/db2_inferred_relationships.yaml",
            "output_json": "output/db2_schema_graph.json",
            "html_file": "db2_schema_graph.html",
            "display_name": "IBM DB2",
            "icon": "🔷"
        }
    }
    
    return configs.get(db_type.lower())


def display_relationships(relationships):
    """Display inferred relationships in a formatted way."""
    print("\n🔗 Inferred Relationships:")
    
    has_relationships = False
    for table_name, table_rels in relationships.items():
        if table_rels.get('foreign_keys'):
            has_relationships = True
            print(f"\n📋 {table_name}:")
            pk = table_rels.get('primary_key', 'N/A')
            print(f"   Primary Key: {pk}")
            
            for fk in table_rels['foreign_keys']:
                confidence = fk.get('confidence', 0.0)
                confidence_bar = "🟢" if confidence > 0.8 else "🟡" if confidence > 0.6 else "🔴"
                print(f"   {confidence_bar} {fk['column']} → {fk['references']} (confidence: {confidence:.1f})")
    
    if not has_relationships:
        print("   No foreign key relationships detected")


def main():
    """Main CLI entry point with security enhancements."""
    parser = argparse.ArgumentParser(description='Database Schema Graph Builder')
    
    # Add subcommands for different operations
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Main schema analysis command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze database schema')
    analyze_parser.add_argument('database', choices=['postgres', 'mysql', 'mssql', 'oracle', 'redshift', 'sybase', 'db2'], 
                               help='Database type (postgres, mysql, mssql, oracle, redshift, sybase, or db2)')
    analyze_parser.add_argument('--config', type=str, 
                               help='Custom configuration file path')
    analyze_parser.add_argument('--output', type=str, 
                               help='Custom output directory')
    analyze_parser.add_argument('--quiet', action='store_true', 
                               help='Minimal output mode')
    analyze_parser.add_argument('--validate-config', action='store_true', default=False,
                               help='Validate configuration before proceeding (default: False)')
    analyze_parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                               default='INFO', help='Logging level')
    analyze_parser.add_argument('--audit-log', type=str,
                               help='Path for audit log file')
    
    # Password encryption utility
    encrypt_parser = subparsers.add_parser('encrypt-password', help='Encrypt password for config file')
    encrypt_parser.add_argument('password', help='Password to encrypt')
    
    # Configuration validation command
    validate_parser = subparsers.add_parser('validate-config', help='Validate configuration file')
    validate_parser.add_argument('config_file', help='Configuration file to validate')
    validate_parser.add_argument('db_type', choices=['postgres', 'mysql', 'mssql', 'oracle', 'redshift', 'sybase', 'db2'],
                                help='Database type')
    
    # Default to analyze command for backward compatibility
    if len(sys.argv) == 1:
        parser.print_help()
        return
    
    # Handle legacy usage (direct database argument)
    if len(sys.argv) >= 2 and sys.argv[1] in ['postgres', 'mysql', 'mssql', 'oracle', 'redshift', 'sybase', 'db2']:
        # Insert 'analyze' command for backward compatibility
        sys.argv.insert(1, 'analyze')
    
    args = parser.parse_args()
    
    # Handle different commands
    if args.command == 'encrypt-password':
        encrypt_password_cli(args.password)
        return
    
    if args.command == 'validate-config':
        validation_result = validate_database_config(args.config_file, args.db_type)
        if validation_result.is_valid:
            print("✅ Configuration is valid")
            if validation_result.warnings:
                print("\n⚠️  Warnings:")
                for warning in validation_result.warnings:
                    print(f"   - {warning}")
        else:
            print("❌ Configuration validation failed")
            for error in validation_result.errors:
                print(f"   - {error}")
            sys.exit(1)
        return
    
    if args.command != 'analyze':
        parser.print_help()
        return
    
    # Setup logging
    setup_logging(level=args.log_level, filename=args.audit_log)
    
    # Get database configuration
    db_config = get_database_config(args.database)
    if not db_config:
        print(f"❌ Unsupported database type: {args.database}")
        sys.exit(1)
    
    # Override with custom paths if provided
    if args.config:
        db_config["config_path"] = args.config
    if args.output:
        db_config["output_config"] = os.path.join(args.output, os.path.basename(db_config["output_config"]))
        db_config["output_json"] = os.path.join(args.output, os.path.basename(db_config["output_json"]))
    
    # Validate configuration if requested
    if args.validate_config:
        if not args.quiet:
            print("🔍 Validating configuration...")
        
        validation_result = validate_database_config(db_config["config_path"], args.database)
        if not validation_result.is_valid:
            print("❌ Configuration validation failed:")
            for error in validation_result.errors:
                print(f"   - {error}")
            sys.exit(1)
        
        if validation_result.warnings and not args.quiet:
            print("⚠️  Configuration warnings:")
            for warning in validation_result.warnings:
                print(f"   - {warning}")
    
    # Display banner (unless quiet mode)
    if not args.quiet:
        print(f"{db_config['icon']} {db_config['display_name']} Schema Graph Builder")
        print("=" * 50)
    
    try:
        # Step 1: Extract schema
        if not args.quiet:
            print(f"📊 Step 1: Extracting schema from {db_config['display_name']}...")
        
        schema = extract_schema(args.database, db_config["config_path"])
        
        if not args.quiet:
            print(f"   Found {len(schema['tables'])} tables")

        # Step 2: Infer relationships
        if not args.quiet:
            print("🔍 Step 2: Inferring relationships...")
        
        inferred = infer_relationships(schema)
        
        # Display relationships (unless quiet mode)
        if not args.quiet:
            display_relationships(inferred)

        # Step 3: Output inferred config to YAML
        if not args.quiet:
            print("\n💾 Step 3: Saving relationships to YAML...")
        
        os.makedirs(os.path.dirname(db_config["output_config"]), exist_ok=True)
        with open(db_config["output_config"], "w") as f:
            yaml.dump(inferred, f, default_flow_style=False)
        
        if not args.quiet:
            print(f"   ✅ Relationships saved to {db_config['output_config']}")

        # Step 4: Build graph and visualize
        if not args.quiet:
            print("\n🎨 Step 4: Building graph visualization...")
        
        build_graph(schema, db_config["output_config"], visualize=True, 
                   output_json_path=db_config["output_json"])
        
        if not args.quiet:
            print(f"   ✅ Graph data saved to {db_config['output_json']}")
            print(f"   🌐 Interactive visualization: {db_config['html_file']}")
            print(f"\n🎉 {db_config['display_name']} schema analysis completed successfully!")
        else:
            print(f"✅ {db_config['display_name']} schema analysis completed")
        
    except Exception as e:
        error_msg = f"\n❌ Error during {db_config['display_name']} schema analysis: {e}"
        logger.error(error_msg)
        print(error_msg)
        if not args.quiet:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main() 
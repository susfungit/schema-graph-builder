#!/usr/bin/env python3
"""
Command-line interface for Schema Graph Builder

This module provides the CLI functionality that can be used both as a script
and as an entry point for the installed package.
"""

import argparse
import sys
import os
import yaml
from schema_graph_builder.extractor.schema_extractor import extract_schema
from schema_graph_builder.inference.relationship_inference import infer_relationships
from schema_graph_builder.graph.graph_builder import build_graph


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
        }
    }
    
    return configs.get(db_type.lower())


def display_relationships(inferred_relationships):
    """Display inferred relationships in a formatted way."""
    print("\n📋 Inferred Relationships:")
    for table_name, relationships in inferred_relationships.items():
        pk = relationships.get("primary_key")
        fks = relationships.get("foreign_keys", [])
        print(f"   {table_name}:")
        print(f"     Primary Key: {pk}")
        if fks:
            for fk in fks:
                print(f"     Foreign Key: {fk['column']} -> {fk['references']} (confidence: {fk['confidence']})")
        else:
            print(f"     Foreign Keys: None")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description='Database Schema Graph Builder')
    parser.add_argument('database', choices=['postgres', 'mysql', 'mssql'], 
                       help='Database type (postgres, mysql, or mssql)')
    parser.add_argument('--config', type=str, 
                       help='Custom configuration file path')
    parser.add_argument('--output', type=str, 
                       help='Custom output directory')
    parser.add_argument('--quiet', action='store_true', 
                       help='Minimal output mode')
    
    args = parser.parse_args()
    
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
        print(f"\n❌ Error during {db_config['display_name']} schema analysis: {e}")
        if not args.quiet:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main() 
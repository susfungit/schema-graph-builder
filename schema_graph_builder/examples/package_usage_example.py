#!/usr/bin/env python3
"""
Example script showing how to use the Schema Graph Builder package programmatically

This script demonstrates both the high-level API and the low-level module usage.
"""

import sys
import os

# Add the parent directory to the path to import the package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from schema_graph_builder import SchemaGraphBuilder, extract_schema, infer_relationships, build_graph


def example_high_level_api():
    """Example using the high-level SchemaGraphBuilder API"""
    print("🚀 Example 1: High-level API usage")
    print("=" * 40)
    
    # Initialize the builder
    builder = SchemaGraphBuilder()
    
    try:
        # Analyze PostgreSQL database (if config exists)
        config_path = "config/db_connections.yaml"
        if os.path.exists(config_path):
            result = builder.analyze_database(
                db_type="postgres",
                config_path=config_path,
                output_dir="output/example",
                visualize=True
            )
            
            print(f"✅ Analysis complete!")
            print(f"   - Found {len(result['schema']['tables'])} tables")
            print(f"   - Generated {len(result['output_files'])} output files")
            
            # Show some details about the schema
            for table in result['schema']['tables'][:2]:  # Show first 2 tables
                print(f"   - Table: {table['name']} ({len(table['columns'])} columns)")
                
        else:
            print("⚠️  PostgreSQL config not found, skipping example")
            
    except Exception as e:
        print(f"❌ Error: {e}")


def example_low_level_api():
    """Example using the low-level module functions"""
    print("\n🔧 Example 2: Low-level API usage")
    print("=" * 40)
    
    try:
        # Extract schema only
        config_path = "config/db_connections.yaml"
        if os.path.exists(config_path):
            schema = extract_schema("postgres", config_path)
            print(f"✅ Schema extracted: {len(schema['tables'])} tables")
            
            # Infer relationships
            relationships = infer_relationships(schema)
            print(f"✅ Relationships inferred for {len(relationships)} tables")
            
            # You could save these manually or use build_graph
            print("   - Use build_graph() to create visualizations")
            
        else:
            print("⚠️  PostgreSQL config not found, skipping example")
            
    except Exception as e:
        print(f"❌ Error: {e}")


def example_mysql_usage():
    """Example using MySQL database"""
    print("\n🐬 Example 3: MySQL database usage")
    print("=" * 40)
    
    builder = SchemaGraphBuilder()
    
    try:
        config_path = "config/mysql_db_connections.yaml"
        if os.path.exists(config_path):
            result = builder.analyze_database(
                db_type="mysql",
                config_path=config_path,
                output_dir="output/mysql_example",
                visualize=True
            )
            
            print(f"✅ MySQL analysis complete!")
            print(f"   - Found {len(result['schema']['tables'])} tables")
            
        else:
            print("⚠️  MySQL config not found, skipping example")
            
    except Exception as e:
        print(f"❌ Error: {e}")


def example_extract_only():
    """Example extracting schema without inference or visualization"""
    print("\n📊 Example 4: Extract schema only")
    print("=" * 40)
    
    builder = SchemaGraphBuilder()
    
    try:
        config_path = "config/db_connections.yaml"
        if os.path.exists(config_path):
            schema = builder.extract_schema_only("postgres", config_path)
            
            print(f"✅ Schema extracted:")
            for table in schema['tables']:
                print(f"   - {table['name']}: {len(table['columns'])} columns")
                
        else:
            print("⚠️  PostgreSQL config not found, skipping example")
            
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    print("🎯 Schema Graph Builder Package Usage Examples")
    print("=" * 50)
    
    # Run all examples
    example_high_level_api()
    example_low_level_api()
    example_mysql_usage()
    example_extract_only()
    
    print("\n🎉 All examples completed!")
    print("\nNext steps:")
    print("1. Install the package: pip install -e .")
    print("2. Use the CLI: schema-graph-builder postgres")
    print("3. Import in your code: from schema_graph_builder import SchemaGraphBuilder") 
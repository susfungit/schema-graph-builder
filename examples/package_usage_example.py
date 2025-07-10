#!/usr/bin/env python3
"""
Example script showing how to use the Schema Graph Builder package

This script demonstrates the main features of the schema-graph-builder package
including database schema extraction, relationship inference, and visualization.
"""

import sys
import os
import yaml
from pathlib import Path

# Add the parent directory to the path to import the package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from schema_graph_builder import SchemaGraphBuilder, extract_schema, infer_relationships, build_graph


def create_sample_config():
    """Create a sample configuration file for demonstration"""
    config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'sample_db',
        'username': 'demo_user',
        'password': 'demo_password'
    }
    
    config_dir = Path("config")
    config_dir.mkdir(exist_ok=True)
    
    config_file = config_dir / "sample_db_connections.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    print(f"✅ Created sample config: {config_file}")
    return str(config_file)


def example_high_level_api():
    """Example using the high-level SchemaGraphBuilder API"""
    print("🚀 Example 1: High-level API usage")
    print("=" * 50)
    
    # Initialize the builder
    builder = SchemaGraphBuilder()
    
    try:
        # Create sample config if it doesn't exist
        config_path = "config/sample_db_connections.yaml"
        if not os.path.exists(config_path):
            config_path = create_sample_config()
        
        print(f"📋 Using config: {config_path}")
        
        # Analyze database (this will work even without actual database connection)
        # The builder will handle missing connections gracefully
        result = builder.analyze_database(
            db_type="postgres",
            config_path=config_path,
            output_dir="output/example",
            visualize=True,
            save_files=True
        )
        
        print(f"✅ Analysis complete!")
        print(f"   - Schema: {len(result['schema']['tables'])} tables")
        print(f"   - Relationships: {len(result['relationships'])} tables with relationships")
        print(f"   - Output files: {len(result['output_files'])} files generated")
        
        # Show output files
        for file_path in result['output_files']:
            print(f"   📄 {file_path}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        print("   (This is expected if no actual database is running)")


def example_low_level_api():
    """Example using the low-level module functions"""
    print("\n🔧 Example 2: Low-level API usage")
    print("=" * 50)
    
    try:
        # Create sample config
        config_path = create_sample_config()
        
        print(f"📋 Using config: {config_path}")
        
        # Extract schema only
        schema = extract_schema("postgres", config_path)
        print(f"✅ Schema extracted: {len(schema['tables'])} tables")
        
        # Show schema structure
        print("   📊 Schema structure:")
        for table in schema['tables'][:3]:  # Show first 3 tables
            print(f"      - {table['name']}: {len(table['columns'])} columns")
        
        # Infer relationships
        relationships = infer_relationships(schema)
        print(f"✅ Relationships inferred for {len(relationships)} tables")
        
        # Show some relationships
        relationship_count = sum(len(rel.get('foreign_keys', [])) for rel in relationships.values())
        print(f"   🔗 Total foreign key relationships: {relationship_count}")
        
        # Build graph visualization
        output_path = "output/example_relationships.yaml"
        build_graph(schema, output_path, visualize=True)
        print(f"✅ Graph visualization created: {output_path}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("   (This is expected if no actual database is running)")


def example_multiple_databases():
    """Example showing support for multiple database types"""
    print("\n🗄️  Example 3: Multiple database support")
    print("=" * 50)
    
    # List all supported database types
    supported_dbs = [
        "postgres", "mysql", "mssql", "oracle", 
        "redshift", "sybase", "db2"
    ]
    
    print("📋 Supported database types:")
    for db_type in supported_dbs:
        print(f"   ✅ {db_type}")
    
    # Show how to use different database types
    builder = SchemaGraphBuilder()
    
    for db_type in ["postgres", "mysql", "mssql"]:
        print(f"\n🔍 Example for {db_type.upper()}:")
        try:
            config_path = f"config/{db_type}_db_connections.yaml"
            
            # This would work with actual database connections
            print(f"   📋 Config: {config_path}")
            print(f"   🎯 Command: schema-graph-builder {db_type}")
            print(f"   🐍 Code: builder.analyze_database('{db_type}', '{config_path}')")
            
        except Exception as e:
            print(f"   ❌ {e}")


def example_cli_usage():
    """Example showing CLI usage"""
    print("\n💻 Example 4: Command Line Interface usage")
    print("=" * 50)
    
    print("📋 CLI Commands:")
    print("   # Basic usage")
    print("   schema-graph-builder postgres")
    print("   schema-graph-builder mysql")
    print("   schema-graph-builder oracle")
    print()
    print("   # With custom config")
    print("   schema-graph-builder postgres --config my-config.yaml")
    print()
    print("   # With custom output")
    print("   schema-graph-builder mysql --output results/")
    print()
    print("   # Quiet mode for scripts")
    print("   schema-graph-builder postgres --quiet")
    print()
    print("   # Get help")
    print("   schema-graph-builder --help")


def example_configuration():
    """Example showing configuration file formats"""
    print("\n⚙️  Example 5: Configuration file formats")
    print("=" * 50)
    
    config_examples = {
        "PostgreSQL": {
            "host": "localhost",
            "port": 5432,
            "database": "my_database",
            "username": "my_user",
            "password": "my_password"
        },
        "MySQL": {
            "host": "localhost",
            "port": 3306,
            "database": "my_database",
            "username": "my_user",
            "password": "my_password"
        },
        "SQL Server": {
            "host": "localhost",
            "port": 1433,
            "database": "my_database",
            "username": "my_user",
            "password": "my_password",
            "driver": "ODBC Driver 18 for SQL Server"
        },
        "Oracle": {
            "host": "localhost",
            "port": 1521,
            "service_name": "XEPDB1",
            "username": "my_user",
            "password": "my_password"
        }
    }
    
    for db_type, config in config_examples.items():
        print(f"\n📋 {db_type} Configuration:")
        config_file = f"config/{db_type.lower().replace(' ', '_')}_db_connections.yaml"
        print(f"   File: {config_file}")
        print("   Content:")
        yaml_content = yaml.dump(config, default_flow_style=False, indent=2)
        for line in yaml_content.split('\n'):
            print(f"      {line}")


if __name__ == "__main__":
    print("🎯 Schema Graph Builder Package Usage Examples")
    print("=" * 60)
    print("This script demonstrates the main features of the schema-graph-builder package.")
    print("Note: Some examples may show errors if no actual database is running.")
    print()
    
    # Run all examples
    example_high_level_api()
    example_low_level_api()
    example_multiple_databases()
    example_cli_usage()
    example_configuration()
    
    print("\n🎉 All examples completed!")
    print("\n📚 Next steps:")
    print("1. Install the package: pip install -e .")
    print("2. Create your database config files in config/")
    print("3. Use the CLI: schema-graph-builder postgres")
    print("4. Import in your code: from schema_graph_builder import SchemaGraphBuilder")
    print("5. Check the README.md for detailed documentation") 
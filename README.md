# 🗃️ Schema Graph Builder

**Database schema extraction and interactive graph visualization tool**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/tests-99%20passing-brightgreen.svg)](tests/)

A powerful Python tool that automatically extracts database schemas, infers relationships between tables, and creates beautiful interactive visualizations. Supports PostgreSQL, MySQL, and Microsoft SQL Server.

![Schema Graph Example](docs/images/schema-graph-preview.png)

## ✨ Features

- 🎯 **Multi-Database Support** - PostgreSQL, MySQL, MS SQL Server
- 🔍 **Automatic Relationship Detection** - Infers foreign key relationships using column name patterns
- 🎨 **Interactive Visualizations** - Beautiful HTML graphs with hover details and zoom
- 📊 **Multiple Output Formats** - YAML, JSON, HTML
- 🖥️ **Dual Interface** - Both CLI and Python API
- ⚡ **Fast & Efficient** - Optimized for large databases
- 🧪 **Thoroughly Tested** - 99 tests with comprehensive coverage
- 📦 **Production Ready** - Professional packaging and distribution

## 🚀 Quick Start

### Installation

```bash
# Install from PyPI (when published)
pip install schema-graph-builder

# Or install from source
git clone https://github.com/yourusername/schema-graph-builder
cd schema-graph-builder
pip install -e .
```

### Basic Usage

#### Command Line Interface

```bash
# Analyze a PostgreSQL database
schema-graph-builder postgres

# Analyze with custom configuration
schema-graph-builder postgres --config my-db-config.yaml --output results/

# Quiet mode for scripts
schema-graph-builder mysql --quiet
```

#### Python API

```python
from schema_graph_builder import SchemaGraphBuilder

# Create builder instance
builder = SchemaGraphBuilder()

# Analyze database and get results
result = builder.analyze_database(
    db_type="postgres",
    config_path="config/db_connections.yaml"
)

# Access extracted data
print(f"Found {len(result['schema']['tables'])} tables")
print(f"Detected {sum(len(rel['foreign_keys']) for rel in result['relationships'].values())} relationships")
```

## 📋 Requirements

- Python 3.8+
- Database drivers:
  - PostgreSQL: `psycopg2-binary`
  - MySQL: `pymysql`
  - SQL Server: `pyodbc`

## 🔧 Configuration

Create a YAML configuration file for your database:

### PostgreSQL Example
```yaml
# config/db_connections.yaml
host: localhost
port: 5432
database: your_database
username: your_username
password: your_password
```

### MySQL Example
```yaml
# config/mysql_db_connections.yaml
host: localhost
port: 3306
database: your_database
username: your_username
password: your_password
```

### SQL Server Example
```yaml
# config/mssql_db_connections.yaml
host: localhost
port: 1433
database: your_database
username: your_username
password: your_password
driver: "ODBC Driver 17 for SQL Server"
```

## 📚 Detailed Usage

### Python API Examples

#### Extract Schema Only
```python
from schema_graph_builder import extract_schema

schema = extract_schema("postgres", "config/db_connections.yaml")
print(f"Database: {schema['database']}")
for table in schema['tables']:
    print(f"  Table: {table['name']} ({len(table['columns'])} columns)")
```

#### Infer Relationships
```python
from schema_graph_builder import infer_relationships

relationships = infer_relationships(schema)
for table, rels in relationships.items():
    if rels['foreign_keys']:
        print(f"{table} references:")
        for fk in rels['foreign_keys']:
            print(f"  {fk['column']} -> {fk['references']} (confidence: {fk['confidence']})")
```

#### Create Visualization
```python
builder = SchemaGraphBuilder()
builder.last_schema = schema
builder.last_relationships = relationships

# Create interactive HTML visualization
html_path = builder.create_visualization(output_path="my_schema.html")
print(f"Visualization saved to: {html_path}")
```

#### Full Workflow
```python
builder = SchemaGraphBuilder()

# Complete analysis with all outputs
result = builder.analyze_database(
    db_type="postgres",
    config_path="config/db_connections.yaml",
    output_dir="output",
    visualize=True,
    save_files=True
)

# Files created:
# - output/postgres_inferred_relationships.yaml
# - output/postgres_schema_graph.json  
# - postgres_schema_graph.html
```

### CLI Examples

```bash
# Basic usage
schema-graph-builder postgres

# Custom configuration and output
schema-graph-builder mysql \
  --config config/production-mysql.yaml \
  --output /tmp/analysis-results/

# Multiple databases
schema-graph-builder postgres --config prod-postgres.yaml
schema-graph-builder mysql --config prod-mysql.yaml
schema-graph-builder mssql --config prod-mssql.yaml

# Automated/scripted usage
schema-graph-builder postgres --quiet && echo "Analysis complete"
```

## 📁 Output Files

The tool generates several output files:

- **`{db_type}_inferred_relationships.yaml`** - Detected foreign key relationships
- **`{db_type}_schema_graph.json`** - Graph data in JSON format
- **`{db_type}_schema_graph.html`** - Interactive visualization

### Example Relationship Output
```yaml
users:
  primary_key: user_id
  foreign_keys: []

orders:
  primary_key: order_id
  foreign_keys:
    - column: user_id
      references: users.user_id
      confidence: 0.95

order_items:
  primary_key: item_id
  foreign_keys:
    - column: order_id
      references: orders.order_id
      confidence: 0.98
    - column: product_id
      references: products.product_id
      confidence: 0.92
```

## 🛠️ Development

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/yourusername/schema-graph-builder
cd schema-graph-builder

# Install in development mode
make install-dev

# Run tests
make test

# Run specific test types
make test-unit
make test-integration
make test-performance

# Code quality checks
make lint
make format
make type-check
```

### Project Structure

```
schema-graph-builder/
├── schema_graph_builder/          # Main package
│   ├── api.py                    # High-level API
│   ├── cli.py                    # Command-line interface
│   ├── connectors/               # Database connectors
│   ├── extractor/                # Schema extraction
│   ├── inference/                # Relationship inference
│   ├── graph/                    # Graph building & visualization
│   └── utils/                    # Utility functions
├── tests/                        # Test suite (99 tests)
├── config/                       # Example configurations
├── examples/                     # Usage examples
├── setup.py                      # Package configuration
├── Makefile                      # Development commands
└── README.md                     # This file
```

### Running Tests

```bash
# Run all tests (99 tests in ~0.45s)
pytest tests/ -v

# Run with coverage
make test-coverage

# Test specific modules
pytest tests/test_api.py -v
pytest tests/test_integration.py -v

# Test specific database types
pytest tests/ -k "postgres" -v
pytest tests/ -k "mysql" -v
```

## 🤝 Contributing

We welcome contributions! Please see our contributing guidelines:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. **Make** your changes
4. **Add** tests for new functionality
5. **Run** the test suite (`make test`)
6. **Commit** your changes (`git commit -m 'Add amazing feature'`)
7. **Push** to the branch (`git push origin feature/amazing-feature`)
8. **Open** a Pull Request

### Development Guidelines

- Write comprehensive tests for new features
- Follow PEP 8 style guidelines
- Add type hints to new code
- Update documentation for API changes
- Ensure all tests pass before submitting PR

## 📊 Performance

- **Fast execution**: Complete analysis of typical databases in seconds
- **Memory efficient**: Handles large schemas without excessive memory usage
- **Optimized tests**: 99 tests run in under 0.5 seconds
- **Scalable**: Tested with databases containing 100+ tables

## 🔍 How It Works

1. **Schema Extraction**: Connects to database and extracts table/column metadata
2. **Relationship Inference**: Analyzes column names and types to detect foreign keys
3. **Graph Building**: Creates NetworkX graph with tables as nodes, relationships as edges
4. **Visualization**: Generates interactive HTML using Pyvis or vis.js fallback

### Relationship Detection Algorithm

The tool uses intelligent heuristics to detect foreign key relationships:

- **Exact matches**: `user_id` in orders table → `user_id` in users table
- **Pattern matching**: `customer_id` → `customers.id` or `customers.customer_id`
- **Type validation**: Ensures data types are compatible
- **Confidence scoring**: Assigns confidence levels to detected relationships

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [SQLAlchemy](https://www.sqlalchemy.org/) for database connectivity
- Visualizations powered by [Pyvis](https://pyvis.readthedocs.io/) and [vis.js](https://visjs.org/)
- Graph algorithms using [NetworkX](https://networkx.org/)

## 📞 Support

- 🐛 **Bug reports**: [GitHub Issues](https://github.com/yourusername/schema-graph-builder/issues)
- 💡 **Feature requests**: [GitHub Discussions](https://github.com/yourusername/schema-graph-builder/discussions)
- 📖 **Documentation**: [Project Wiki](https://github.com/yourusername/schema-graph-builder/wiki)

---

**⭐ If this project helps you, please consider giving it a star on GitHub!** 
# Test Suite Documentation

This directory contains comprehensive unit tests for the Schema Graph Builder package.

## Test Structure

The test suite is organized into the following modules:

### Core Tests
- `test_connectors.py` - Tests for database connector modules (PostgreSQL, MySQL, MS SQL Server)
- `test_extractor.py` - Tests for schema extraction functionality
- `test_inference.py` - Tests for relationship inference algorithms
- `test_graph.py` - Tests for graph building and visualization
- `test_api.py` - Tests for the high-level API interface
- `test_cli.py` - Tests for command-line interface

### Support Tests
- `test_utils.py` - Tests for utility functions and helpers
- `test_integration.py` - Integration tests for complete workflows
- `conftest.py` - Shared fixtures and test configuration

## Running Tests

### Prerequisites
```bash
pip install -r requirements-test.txt
```

### Basic Test Execution
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_inference.py

# Run specific test class
pytest tests/test_inference.py::TestRelationshipInference

# Run specific test method
pytest tests/test_inference.py::TestRelationshipInference::test_infer_relationships_basic_foreign_key
```

### Test Categories

#### Unit Tests
Tests individual functions and methods in isolation:
```bash
pytest -m "not integration and not performance"
```

#### Integration Tests
Tests complete workflows and interactions between components:
```bash
pytest -m integration
```

#### Performance Tests
Tests system behavior under load and with large datasets:
```bash
pytest -m performance
```

### Coverage Reports
```bash
# Generate coverage report
pytest --cov=schema_graph_builder --cov-report=html

# View coverage in browser
open htmlcov/index.html
```

### Parallel Testing
```bash
# Run tests in parallel using pytest-xdist
pytest -n auto
```

## Test Configuration

### pytest.ini
Configuration for pytest behavior, including:
- Test discovery patterns
- Default command-line options
- Warning filters
- Test markers

### tox.ini
Configuration for testing across multiple Python versions:
- Python 3.9, 3.10, 3.11, 3.12
- Separate environments for linting, type checking, and coverage
- Integration and performance test environments

## Test Fixtures

### Shared Fixtures (conftest.py)
- `sample_schema` - Mock database schema with customers, orders, and products
- `sample_relationships` - Mock relationship inference results
- `temp_config_file` - Temporary YAML configuration file
- `temp_output_dir` - Temporary directory for test outputs
- `mock_sqlalchemy_engine` - Mock SQLAlchemy engine and inspector
- `mock_networkx_graph` - Mock NetworkX graph object

### Usage Example
```python
def test_my_function(sample_schema, temp_config_file):
    result = my_function(sample_schema, temp_config_file)
    assert result is not None
```

## Writing Tests

### Test Organization
- Group related tests into classes
- Use descriptive test method names
- Include docstrings for complex test cases
- Use appropriate markers for categorization

### Best Practices
1. **Isolation**: Each test should be independent and not rely on other tests
2. **Mocking**: Use mocks to isolate units under test from external dependencies
3. **Data**: Use fixtures for test data to ensure consistency
4. **Assertions**: Write clear, specific assertions that test the expected behavior
5. **Edge Cases**: Test error conditions and boundary cases

### Example Test Structure
```python
class TestMyModule:
    """Tests for MyModule functionality"""
    
    def test_normal_operation(self, sample_data):
        """Test normal operation with valid input"""
        result = my_function(sample_data)
        assert result.success is True
        assert len(result.items) == 3
    
    def test_error_handling(self):
        """Test error handling with invalid input"""
        with pytest.raises(ValueError, match="Invalid input"):
            my_function(None)
    
    @pytest.mark.integration
    def test_integration_workflow(self, temp_config_file):
        """Test complete workflow integration"""
        # Integration test code here
        pass
```

## Mocking Strategy

### Database Connections
All database connections are mocked to avoid requiring actual database instances:
- SQLAlchemy engines are mocked
- Database inspectors are mocked with sample data
- Network calls are avoided entirely

### File Operations
File operations are mocked using:
- `unittest.mock.mock_open` for file reading/writing
- `tempfile` for temporary files and directories
- `patch` decorators for system calls

### External Dependencies
- NetworkX graph operations are mocked
- Pyvis visualization is mocked
- YAML file operations are controlled

## Common Test Patterns

### Testing Exception Handling
```python
def test_handles_connection_error(self, mock_connector):
    mock_connector.side_effect = ConnectionError("Database unavailable")
    
    with pytest.raises(ConnectionError, match="Database unavailable"):
        extract_schema('postgres', 'config.yaml')
```

### Testing File Operations
```python
@patch('builtins.open', new_callable=mock_open)
def test_file_writing(self, mock_file):
    write_config_file('test.yaml', {'key': 'value'})
    mock_file.assert_called_once_with('test.yaml', 'w')
```

### Testing With Fixtures
```python
def test_with_sample_data(self, sample_schema, sample_relationships):
    result = process_data(sample_schema)
    assert result.table_count == len(sample_schema['tables'])
```

## Continuous Integration

The test suite is designed to run in CI/CD environments:
- No external dependencies required
- All database connections are mocked
- Deterministic test results
- Parallel execution supported

### Make Commands
```bash
make test           # Run all tests
make test-unit      # Run unit tests only
make test-integration # Run integration tests
make test-coverage  # Run with coverage report
make lint          # Run code linting
make type-check    # Run type checking
```

## Test Metrics

The test suite aims for:
- **Coverage**: >90% code coverage
- **Speed**: Complete test suite runs in <30 seconds
- **Reliability**: Zero flaky tests
- **Maintainability**: Clear, readable test code

## Debugging Tests

### Running Single Tests
```bash
# Run with verbose output
pytest tests/test_inference.py::test_my_function -v

# Run with print statements
pytest tests/test_inference.py::test_my_function -s

# Run with debugger
pytest tests/test_inference.py::test_my_function --pdb
```

### Test Data Inspection
```python
def test_debug_data(self, sample_schema):
    import pprint
    pprint.pprint(sample_schema)  # Inspect fixture data
    assert False  # Force test failure to see output
```

## Contributing to Tests

When adding new functionality:
1. Write tests for the new feature
2. Ensure all existing tests still pass
3. Add appropriate test markers
4. Update documentation if needed
5. Maintain test coverage above 90%

### Test Checklist
- [ ] Unit tests for new functions/methods
- [ ] Integration tests for new workflows
- [ ] Error handling tests
- [ ] Edge case coverage
- [ ] Performance tests for intensive operations
- [ ] Documentation updates 
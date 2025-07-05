# Makefile for Schema Graph Builder

.PHONY: help install install-dev test test-unit test-integration test-performance test-coverage clean lint format type-check docs build publish

help:
	@echo "Available commands:"
	@echo "  install         Install production dependencies"
	@echo "  install-dev     Install development dependencies"
	@echo "  test           Run all tests"
	@echo "  test-unit      Run unit tests only"
	@echo "  test-integration Run integration tests only"
	@echo "  test-performance Run performance tests only"
	@echo "  test-coverage  Run tests with coverage report"
	@echo "  lint           Run linting checks"
	@echo "  format         Format code with black"
	@echo "  type-check     Run type checking with mypy"
	@echo "  clean          Clean up build artifacts"
	@echo "  build          Build package"
	@echo "  publish        Publish to PyPI"

install:
	pip install -e .

install-dev:
	pip install -e .
	pip install -r requirements-test.txt

test:
	pytest tests/

test-unit:
	pytest tests/ -m "not integration and not performance"

test-integration:
	pytest tests/ -m "integration"

test-performance:
	pytest tests/ -m "performance"

test-coverage:
	pytest tests/ --cov=schema_graph_builder --cov-report=html --cov-report=term-missing

lint:
	flake8 schema_graph_builder tests
	black --check schema_graph_builder tests

format:
	black schema_graph_builder tests

type-check:
	mypy schema_graph_builder

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	python setup.py sdist bdist_wheel

publish: build
	twine upload dist/*

# Quick development setup
setup: install-dev
	@echo "Development environment setup complete!"
	@echo "Run 'make test' to run the test suite"

# Continuous integration
ci: install-dev lint type-check test-coverage
	@echo "CI pipeline completed successfully!" 
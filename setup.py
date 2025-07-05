#!/usr/bin/env python3
"""
Setup script for schema-graph-builder package
"""

from setuptools import setup, find_packages
import os

# Read the README file for long description
try:
    with open("README.md", "r", encoding="utf-8") as fh:
        long_description = fh.read()
except FileNotFoundError:
    long_description = "Database Schema Graph Builder - A tool for extracting database schemas and creating interactive visualizations"

# Define requirements directly
requirements = [
    "sqlalchemy>=1.4.0",
    "psycopg2-binary>=2.9.0",
    "pymysql>=1.0.0",
    "pyodbc>=4.0.30",
    "PyYAML>=6.0",
    "networkx>=2.8.0",
    "pyvis>=0.3.0",
    "cryptography>=3.4.0",  # For password encryption
]

setup(
    name="schema-graph-builder",
    version="1.0.0",
    author="Sushant Prabhu Sawkar",
    author_email="sushantsawkar@gmail.com",
    description="Database schema extraction and graph visualization tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/susfungit/schema-graph-builder",
    packages=find_packages(exclude=["tests", "tests.*"]) + ["schema_graph_builder.examples"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": ["pytest", "black", "flake8", "mypy"],
        "test": ["pytest", "pytest-cov"],
    },
    entry_points={
        "console_scripts": [
            "schema-graph-builder=schema_graph_builder.cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "schema_graph_builder": [
            "config/*.yaml",
            "examples/*.sql",
            "examples/*.yaml",
        ],
    },
    keywords="database schema graph visualization postgresql mysql mssql",
    project_urls={
        "Bug Reports": "https://github.com/susfungit/schema-graph-builder/issues",
        "Source": "https://github.com/susfungit/schema-graph-builder",
        "Documentation": "https://github.com/susfungit/schema-graph-builder#readme",
    },
) 
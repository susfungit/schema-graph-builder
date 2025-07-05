#!/usr/bin/env python3
"""
Setup script for schema-graph-builder package
"""

from setuptools import setup, find_packages
import os

# Read the README file for long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements
with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="schema-graph-builder",
    version="1.0.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="Database schema extraction and graph visualization tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/schema-graph-builder",
    packages=find_packages(),
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
        "Bug Reports": "https://github.com/yourusername/schema-graph-builder/issues",
        "Source": "https://github.com/yourusername/schema-graph-builder",
        "Documentation": "https://github.com/yourusername/schema-graph-builder#readme",
    },
) 
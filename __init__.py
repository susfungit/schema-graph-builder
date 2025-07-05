"""
Schema Graph Builder - Database schema extraction and graph visualization tool

This package provides tools for extracting database schemas, inferring relationships,
and creating interactive visualizations for PostgreSQL, MySQL, and MS SQL Server databases.
"""

__version__ = "1.0.0"
__author__ = "Sushant Prabhu Sawkar"
__email__ = "sushantsawkar@gmail.com"

# Import main components for easy access
from schema_graph_builder.extractor.schema_extractor import extract_schema
from schema_graph_builder.inference.relationship_inference import infer_relationships  
from schema_graph_builder.graph.graph_builder import build_graph
from schema_graph_builder.api import SchemaGraphBuilder

# Define what gets imported with "from schema_graph_builder import *"
__all__ = [
    "extract_schema",
    "infer_relationships", 
    "build_graph",
    "SchemaGraphBuilder",
    "__version__",
] 
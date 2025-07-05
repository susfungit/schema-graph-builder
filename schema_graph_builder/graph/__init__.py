"""
Graph building and visualization module

This module provides functionality to build NetworkX graphs from database schemas
and create interactive HTML visualizations.
"""

from .graph_builder import build_graph

__all__ = [
    "build_graph",
] 
"""
Relationship inference module

This module provides functionality to infer relationships between database tables
based on naming conventions and column analysis.
"""

from .relationship_inference import infer_relationships

__all__ = [
    "infer_relationships",
] 
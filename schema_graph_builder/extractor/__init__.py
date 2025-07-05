"""
Schema extraction module

This module provides the main interface for extracting database schemas
from different database types.
"""

from .schema_extractor import extract_schema

__all__ = [
    "extract_schema",
] 
#!/usr/bin/env python3
"""
Legacy entry point for Schema Graph Builder

This module provides backward compatibility for the original main.py script.
For new code, use the CLI module directly or import the package.
"""

from cli import main

if __name__ == "__main__":
    main() 
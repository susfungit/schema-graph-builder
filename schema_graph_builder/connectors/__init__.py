"""
Database connectors for schema extraction

This module provides database-specific connectors for PostgreSQL, MySQL, MS SQL Server, Oracle, and Amazon Redshift.
"""

from .base_connector import DatabaseConnector
from .postgres_connector import PostgreSQLConnector, get_postgres_schema
from .mysql_connector import MySQLConnector, get_mysql_schema
from .mssql_connector import MSSQLConnector, get_mssql_schema
from .oracle_connector import OracleConnector, get_oracle_schema
from .redshift_connector import RedshiftConnector, get_redshift_schema

__all__ = [
    "DatabaseConnector",
    "PostgreSQLConnector",
    "MySQLConnector",
    "MSSQLConnector",
    "OracleConnector",
    "RedshiftConnector",
    "get_postgres_schema",
    "get_mysql_schema", 
    "get_mssql_schema",
    "get_oracle_schema",
    "get_redshift_schema",
] 
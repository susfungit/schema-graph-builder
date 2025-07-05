"""
Configuration validation utilities for Schema Graph Builder
"""

import os
import yaml
from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class ValidationResult:
    """Result of configuration validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]


def validate_database_config(config_path: str, db_type: str) -> ValidationResult:
    """
    Validate database configuration file.
    
    Args:
        config_path: Path to configuration file
        db_type: Database type (postgres, mysql, mssql)
        
    Returns:
        ValidationResult with validation status and messages
    """
    errors = []
    warnings = []
    
    # Check if file exists
    if not os.path.exists(config_path):
        errors.append(f"Configuration file not found: {config_path}")
        return ValidationResult(False, errors, warnings)
    
    try:
        # Load and parse YAML
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            
        if not isinstance(config, dict):
            errors.append("Configuration must be a dictionary")
            return ValidationResult(False, errors, warnings)
            
        # Validate required fields
        required_fields = ['host', 'port', 'database', 'username']
        for field in required_fields:
            if field not in config:
                errors.append(f"Missing required field: {field}")
                
        # Check for credentials
        if 'password' not in config and not os.getenv('DB_PASSWORD'):
            errors.append("Password must be in config file or DB_PASSWORD environment variable")
            
        # Validate field types
        if 'port' in config and not isinstance(config['port'], int):
            errors.append("Port must be an integer")
            
        if 'host' in config and not isinstance(config['host'], str):
            errors.append("Host must be a string")
            
        # Database-specific validation
        if db_type == 'postgres':
            if 'port' in config and not (1 <= config['port'] <= 65535):
                errors.append("PostgreSQL port must be between 1 and 65535")
        elif db_type == 'mysql':
            if 'port' in config and config['port'] != 3306:
                warnings.append("MySQL typically uses port 3306")
        elif db_type == 'mssql':
            if 'port' in config and config['port'] != 1433:
                warnings.append("MS SQL Server typically uses port 1433")
            if 'driver' not in config:
                warnings.append("MSSQL driver not specified, using default")
                
        # Security checks
        if 'password' in config and len(config['password']) < 8:
            warnings.append("Password is shorter than 8 characters")
            
        if 'username' in config and config['username'] in ['root', 'admin', 'sa']:
            warnings.append("Using default administrative username")
            
    except yaml.YAMLError as e:
        errors.append(f"Invalid YAML syntax: {e}")
    except Exception as e:
        errors.append(f"Unexpected error reading config: {e}")
        
    return ValidationResult(len(errors) == 0, errors, warnings)


def validate_output_directory(output_dir: str) -> ValidationResult:
    """
    Validate output directory configuration.
    
    Args:
        output_dir: Output directory path
        
    Returns:
        ValidationResult with validation status and messages
    """
    errors = []
    warnings = []
    
    try:
        # Check if directory exists or can be created
        if not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir, exist_ok=True)
                warnings.append(f"Created output directory: {output_dir}")
            except PermissionError:
                errors.append(f"Permission denied creating directory: {output_dir}")
            except Exception as e:
                errors.append(f"Error creating directory: {e}")
        
        # Check write permissions
        if os.path.exists(output_dir) and not os.access(output_dir, os.W_OK):
            errors.append(f"No write permission for directory: {output_dir}")
            
    except Exception as e:
        errors.append(f"Error validating output directory: {e}")
        
    return ValidationResult(len(errors) == 0, errors, warnings) 
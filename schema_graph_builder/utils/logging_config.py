"""
Logging configuration for Schema Graph Builder

This module provides logging utilities that gracefully handle missing dependencies.
"""

import logging
import sys
from typing import Optional

# Check if we have all required dependencies
try:
    import os
    import re
    LOGGING_AVAILABLE = True
except ImportError:
    LOGGING_AVAILABLE = False


def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    filename: Optional[str] = None
) -> None:
    """
    Configure logging for the application.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Custom log format string
        filename: Optional log file path
    """
    if format_string is None:
        format_string = (
            "%(asctime)s - %(name)s - %(levelname)s - "
            "%(filename)s:%(lineno)d - %(message)s"
        )
    
    handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
    console_handler.setFormatter(logging.Formatter(format_string))
    handlers.append(console_handler)
    
    # File handler (if specified)
    if filename:
        try:
            # Ensure directory exists
            if LOGGING_AVAILABLE:
                os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            file_handler = logging.FileHandler(filename)
            file_handler.setLevel(getattr(logging, level.upper(), logging.INFO))
            file_handler.setFormatter(logging.Formatter(format_string))
            handlers.append(file_handler)
            
        except Exception as e:
            # If file logging fails, just use console
            console_handler.setLevel(logging.WARNING)
            logging.getLogger(__name__).warning(f"Failed to setup file logging: {e}")
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    
    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add new handlers
    for handler in handlers:
        root_logger.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def mask_credentials(text: str) -> str:
    """
    Mask sensitive information in text for safe logging.
    
    Args:
        text: Text that may contain sensitive information
        
    Returns:
        Text with sensitive information masked
    """
    if not LOGGING_AVAILABLE:
        return text
    
    try:
        # Mask password patterns
        masked_text = re.sub(r'password[\'"]?\s*[:=]\s*[\'"]([^\'"\s]+)[\'"]?', 
                           r'password=***', text, flags=re.IGNORECASE)
        
        # Mask connection strings
        masked_text = re.sub(r'://([^:]+):([^@]+)@', r'://\1:***@', masked_text)
        
        # Mask API keys
        masked_text = re.sub(r'(api[_-]?key|token|secret)[\'"]?\s*[:=]\s*[\'"]([^\'"\s]+)[\'"]?', 
                           r'\1=***', masked_text, flags=re.IGNORECASE)
        
        return masked_text
        
    except Exception:
        # If masking fails, return original text
        return text 
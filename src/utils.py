"""
Utility functions for the Scholarship Watcher pipeline.

This module provides:
- Central logging configuration
- Safe JSON read/write helpers
- Shared helper utilities used across modules
"""

import json
import logging
import os
import sys
import tempfile
import shutil
from pathlib import Path
from typing import Any, Optional


def setup_logging(level: str = "INFO") -> logging.Logger:
    """
    Configure and return the root logger for the application.

    Args:
        level: Logging level as string (DEBUG, INFO, WARNING, ERROR, CRITICAL).
               Defaults to INFO.

    Returns:
        Configured logger instance.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    logger = logging.getLogger("scholarship_watcher")
    logger.setLevel(log_level)
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.

    Args:
        name: Name for the logger, typically the module name.

    Returns:
        Logger instance configured as a child of the main application logger.
    """
    return logging.getLogger(f"scholarship_watcher.{name}")


def safe_read_json(filepath: str, default: Optional[Any] = None) -> Any:
    """
    Safely read JSON data from a file.

    Args:
        filepath: Path to the JSON file.
        default: Default value to return if file doesn't exist or is invalid.
                 Defaults to None.

    Returns:
        Parsed JSON data or the default value on failure.
    """
    logger = get_logger("utils")
    
    try:
        path = Path(filepath)
        if not path.exists():
            logger.debug(f"File does not exist: {filepath}, returning default")
            return default if default is not None else []
        
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            logger.debug(f"Successfully read JSON from {filepath}")
            return data
            
    except json.JSONDecodeError as e:
        logger.warning(f"Invalid JSON in {filepath}: {e}")
        return default if default is not None else []
    except PermissionError as e:
        logger.error(f"Permission denied reading {filepath}: {e}")
        return default if default is not None else []
    except Exception as e:
        logger.error(f"Unexpected error reading {filepath}: {e}")
        return default if default is not None else []


def safe_write_json(filepath: str, data: Any, indent: int = 2) -> bool:
    """
    Safely write JSON data to a file using atomic write operation.

    Uses a temporary file and atomic rename to prevent data corruption
    if the write operation is interrupted.

    Args:
        filepath: Path to the JSON file.
        data: Data to serialize as JSON.
        indent: JSON indentation level. Defaults to 2.

    Returns:
        True if write was successful, False otherwise.
    """
    logger = get_logger("utils")
    
    try:
        path = Path(filepath)
        
        # Ensure parent directory exists
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write to temporary file first (atomic write pattern)
        fd, temp_path = tempfile.mkstemp(
            suffix=".json",
            prefix="scholarship_",
            dir=path.parent
        )
        
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=indent, ensure_ascii=False)
            
            # Atomic rename (on POSIX) or copy+delete (on Windows)
            shutil.move(temp_path, filepath)
            logger.debug(f"Successfully wrote JSON to {filepath}")
            return True
            
        except Exception:
            # Clean up temp file on failure
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise
            
    except PermissionError as e:
        logger.error(f"Permission denied writing {filepath}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error writing {filepath}: {e}")
        return False


def get_env_var(name: str, required: bool = True, default: Optional[str] = None) -> Optional[str]:
    """
    Get an environment variable with optional requirement enforcement.

    Args:
        name: Name of the environment variable.
        required: If True, raises ValueError when variable is not set.
                  Defaults to True.
        default: Default value if variable is not set and not required.

    Returns:
        Value of the environment variable or default.

    Raises:
        ValueError: If required=True and the variable is not set.
    """
    value = os.environ.get(name)
    
    if value is None or value.strip() == "":
        if required:
            raise ValueError(f"Required environment variable '{name}' is not set")
        return default
    
    return value.strip()


def normalize_url(url: str, base_url: str) -> str:
    """
    Normalize a potentially relative URL to an absolute URL.

    Args:
        url: The URL to normalize (may be relative or absolute).
        base_url: The base URL to use for resolving relative URLs.

    Returns:
        Absolute URL string.
    """
    from urllib.parse import urljoin, urlparse
    
    # If already absolute, return as-is
    parsed = urlparse(url)
    if parsed.scheme and parsed.netloc:
        return url
    
    # Resolve relative URL against base
    return urljoin(base_url, url)


def sanitize_text(text: str) -> str:
    """
    Clean and normalize text content.

    Removes extra whitespace, newlines, and normalizes spacing.

    Args:
        text: Raw text to sanitize.

    Returns:
        Cleaned text string.
    """
    if not text:
        return ""
    
    # Replace multiple whitespace with single space
    import re
    cleaned = re.sub(r"\s+", " ", text)
    return cleaned.strip()

"""
Utility functions for the Scholarship Watcher pipeline.

This module provides:
- Central logging configuration
- Safe JSON read/write helpers
- Country configuration loading and validation
- Shared helper utilities used across modules
"""

import json
import logging
import os
import sys
import tempfile
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Set


# Default configuration paths
DEFAULT_CONFIG_PATH = "config/countries.json"


class CountryConfig:
    """Represents a country configuration for scholarship filtering."""
    
    def __init__(
        self,
        code: str,
        name: str,
        keywords: List[str],
        enabled: bool = True,
        domain_patterns: Optional[List[str]] = None
    ):
        self.code = code.upper()
        self.name = name
        self.keywords = set(kw.lower() for kw in keywords)
        self.enabled = enabled
        self.domain_patterns = set(dp.lower() for dp in (domain_patterns or []))
    
    def __repr__(self) -> str:
        return f"CountryConfig(code={self.code}, name={self.name}, enabled={self.enabled})"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "code": self.code,
            "name": self.name,
            "keywords": sorted(self.keywords),
            "enabled": self.enabled,
            "domain_patterns": sorted(self.domain_patterns)
        }


def load_countries_config(
    config_path: Optional[str] = None,
    enabled_only: bool = True
) -> List[CountryConfig]:
    """
    Load country configurations from JSON file or environment.
    
    Priority:
    1. COUNTRIES_CONFIG environment variable (JSON string)
    2. COUNTRIES_CONFIG_PATH environment variable (file path)
    3. Provided config_path parameter
    4. Default config file path
    
    Args:
        config_path: Optional path to the configuration file.
        enabled_only: If True, only return enabled countries.
        
    Returns:
        List of CountryConfig objects.
    """
    logger = get_logger("utils")
    countries: List[CountryConfig] = []
    global_keywords: Set[str] = set()
    
    # Check for JSON config in environment variable
    env_config = os.environ.get("COUNTRIES_CONFIG", "").strip()
    if env_config:
        try:
            data = json.loads(env_config)
            logger.info("Loaded country config from COUNTRIES_CONFIG environment variable")
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON in COUNTRIES_CONFIG: {e}")
            data = None
    else:
        data = None
    
    # If not in env, try file path
    if data is None:
        env_path = os.environ.get("COUNTRIES_CONFIG_PATH", "").strip()
        file_path = env_path or config_path or DEFAULT_CONFIG_PATH
        
        data = safe_read_json(file_path, default=None)
        if data is not None:
            logger.info(f"Loaded country config from {file_path}")
        else:
            logger.warning(f"Could not load country config from {file_path}")
    
    # Parse configuration
    if data is not None and isinstance(data, dict):
        # Extract global keywords
        global_keywords = set(
            kw.lower() for kw in data.get("global_keywords", [])
        )
        
        # Parse country entries
        for entry in data.get("countries", []):
            try:
                country = _parse_country_entry(entry)
                if country is not None:
                    if not enabled_only or country.enabled:
                        # Add global keywords to each country
                        country.keywords.update(global_keywords)
                        countries.append(country)
                        logger.debug(f"Loaded country: {country}")
            except Exception as e:
                logger.warning(f"Skipping invalid country entry: {e}")
    
    # Fallback to default Norway config if no countries loaded
    if not countries:
        logger.warning("No countries configured, using default Norway configuration")
        countries = [_get_default_norway_config()]
    
    logger.info(f"Loaded {len(countries)} country configuration(s): {[c.code for c in countries]}")
    return countries


def _parse_country_entry(entry: Dict[str, Any]) -> Optional[CountryConfig]:
    """
    Parse a single country entry from configuration.
    
    Args:
        entry: Dictionary with country configuration.
        
    Returns:
        CountryConfig object or None if invalid.
    """
    if not isinstance(entry, dict):
        return None
    
    code = entry.get("code", "").strip()
    name = entry.get("name", "").strip()
    
    if not code or not name:
        return None
    
    keywords = entry.get("keywords", [])
    if not isinstance(keywords, list):
        keywords = []
    
    enabled = entry.get("enabled", True)
    if not isinstance(enabled, bool):
        enabled = str(enabled).lower() in ("true", "1", "yes")
    
    domain_patterns = entry.get("domain_patterns", [])
    if not isinstance(domain_patterns, list):
        domain_patterns = []
    
    return CountryConfig(
        code=code,
        name=name,
        keywords=keywords,
        enabled=enabled,
        domain_patterns=domain_patterns
    )


def _get_default_norway_config() -> CountryConfig:
    """Get default Norway configuration as fallback."""
    return CountryConfig(
        code="NO",
        name="Norway",
        keywords=[
            "norway", "norwegian", "norge", "norsk",
            "oslo", "bergen", "trondheim", "stavanger",
            "tromsø", "tromso", "ntnu", "uio", "uib",
            "nordic", "scandinavia", "scandinavian"
        ],
        enabled=True,
        domain_patterns=[".no"]
    )


def validate_countries_config(countries: List[CountryConfig]) -> List[str]:
    """
    Validate country configurations and return any warnings.
    
    Args:
        countries: List of CountryConfig objects to validate.
        
    Returns:
        List of warning messages (empty if all valid).
    """
    warnings = []
    seen_codes: Set[str] = set()
    
    for country in countries:
        # Check for duplicate codes
        if country.code in seen_codes:
            warnings.append(f"Duplicate country code: {country.code}")
        seen_codes.add(country.code)
        
        # Check for empty keywords
        if not country.keywords:
            warnings.append(f"Country {country.code} has no keywords")
        
        # Check code format (should be 2-letter ISO)
        if len(country.code) != 2:
            warnings.append(f"Country code '{country.code}' is not ISO-2 format")
    
    return warnings


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

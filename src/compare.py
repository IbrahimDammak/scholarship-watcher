"""
Compare module for the Scholarship Watcher pipeline.

This module handles comparing current scholarships with previous results
to detect new entries, and safely persisting updated results.

Supports both single-country (legacy) and multi-country modes:
- Legacy mode: flat list of scholarships
- Multi-country mode: scholarships grouped by country code
"""

import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from src.utils import get_logger, safe_read_json, safe_write_json


# Module logger
logger = get_logger("compare")

# Default path for storing results
DEFAULT_RESULTS_PATH = "data/last_results.json"


def get_scholarship_identifier(scholarship: Dict[str, str]) -> str:
    """
    Generate a unique identifier for a scholarship.

    Uses the URL as the primary identifier since it should be unique.

    Args:
        scholarship: Dictionary with 'title' and 'url' keys.

    Returns:
        Unique identifier string (the URL).
    """
    return scholarship.get("url", "")


def load_previous_results(filepath: str = DEFAULT_RESULTS_PATH) -> List[Dict[str, str]]:
    """
    Load previous scholarship results from storage.

    Args:
        filepath: Path to the JSON file containing previous results.

    Returns:
        List of scholarship dictionaries from previous run,
        or empty list if file doesn't exist or is invalid.
    """
    logger.debug(f"Loading previous results from {filepath}")
    
    data = safe_read_json(filepath, default=[])
    
    # Handle both list format and dict format with 'scholarships' key
    if isinstance(data, dict):
        scholarships = data.get("scholarships", [])
    elif isinstance(data, list):
        scholarships = data
    else:
        logger.warning(f"Unexpected data format in {filepath}, returning empty list")
        scholarships = []
    
    logger.info(f"Loaded {len(scholarships)} previous scholarship(s)")
    
    return scholarships


def save_results(
    scholarships: List[Dict[str, str]],
    filepath: str = DEFAULT_RESULTS_PATH,
    include_metadata: bool = True
) -> bool:
    """
    Save scholarship results to storage using atomic write.

    Args:
        scholarships: List of scholarship dictionaries to save.
        filepath: Path to the JSON file for storing results.
        include_metadata: If True, include timestamp and count metadata.

    Returns:
        True if save was successful, False otherwise.
    """
    logger.debug(f"Saving {len(scholarships)} scholarship(s) to {filepath}")
    
    if include_metadata:
        data = {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "count": len(scholarships),
            "scholarships": scholarships
        }
    else:
        data = scholarships
    
    success = safe_write_json(filepath, data)
    
    if success:
        logger.info(f"Successfully saved {len(scholarships)} scholarship(s) to {filepath}")
    else:
        logger.error(f"Failed to save results to {filepath}")
    
    return success


def build_url_set(scholarships: List[Dict[str, str]]) -> Set[str]:
    """
    Build a set of URLs from a list of scholarships.

    Args:
        scholarships: List of scholarship dictionaries.

    Returns:
        Set of URL strings.
    """
    return {
        get_scholarship_identifier(s) 
        for s in scholarships 
        if get_scholarship_identifier(s)
    }


def find_new_scholarships(
    current: List[Dict[str, str]],
    previous: List[Dict[str, str]]
) -> List[Dict[str, str]]:
    """
    Find scholarships that are in current but not in previous.

    Args:
        current: List of current scholarship dictionaries.
        previous: List of previous scholarship dictionaries.

    Returns:
        List of new scholarship dictionaries.
    """
    previous_urls = build_url_set(previous)
    
    new_scholarships = [
        s for s in current
        if get_scholarship_identifier(s) not in previous_urls
    ]
    
    logger.info(f"Found {len(new_scholarships)} new scholarship(s)")
    
    return new_scholarships


def find_removed_scholarships(
    current: List[Dict[str, str]],
    previous: List[Dict[str, str]]
) -> List[Dict[str, str]]:
    """
    Find scholarships that were in previous but not in current.

    Args:
        current: List of current scholarship dictionaries.
        previous: List of previous scholarship dictionaries.

    Returns:
        List of removed scholarship dictionaries.
    """
    current_urls = build_url_set(current)
    
    removed_scholarships = [
        s for s in previous
        if get_scholarship_identifier(s) not in current_urls
    ]
    
    logger.debug(f"Found {len(removed_scholarships)} removed scholarship(s)")
    
    return removed_scholarships


def merge_scholarships(
    current: List[Dict[str, str]],
    previous: List[Dict[str, str]],
    keep_removed: bool = False
) -> List[Dict[str, str]]:
    """
    Merge current and previous scholarships, preventing duplicates.

    Args:
        current: List of current scholarship dictionaries.
        previous: List of previous scholarship dictionaries.
        keep_removed: If True, keep scholarships that were in previous
                      but not in current. If False, only return current.

    Returns:
        Merged list of scholarship dictionaries without duplicates.
    """
    if not keep_removed:
        # Just deduplicate current
        seen_urls: Set[str] = set()
        merged = []
        for s in current:
            url = get_scholarship_identifier(s)
            if url and url not in seen_urls:
                seen_urls.add(url)
                merged.append(s)
        return merged
    
    # Merge current + previous (current takes precedence for duplicates)
    merged = []
    seen_urls: Set[str] = set()
    
    # Add all current scholarships first
    for s in current:
        url = get_scholarship_identifier(s)
        if url and url not in seen_urls:
            seen_urls.add(url)
            merged.append(s)
    
    # Add previous scholarships not in current
    for s in previous:
        url = get_scholarship_identifier(s)
        if url and url not in seen_urls:
            seen_urls.add(url)
            merged.append(s)
    
    return merged


def compare_and_update(
    current_scholarships: List[Dict[str, str]],
    results_filepath: str = DEFAULT_RESULTS_PATH,
    save_updated: bool = True
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    """
    Compare current scholarships with previous results and optionally update storage.

    This is the main comparison function that:
    1. Loads previous results
    2. Finds new scholarships
    3. Optionally saves updated results

    Args:
        current_scholarships: List of current scholarship dictionaries.
        results_filepath: Path to the results JSON file.
        save_updated: If True, save the updated results to file.

    Returns:
        Tuple of (new_scholarships, all_current_scholarships).
    """
    logger.info("Starting scholarship comparison")
    
    # Load previous results
    previous_scholarships = load_previous_results(results_filepath)
    
    # Find new scholarships
    new_scholarships = find_new_scholarships(current_scholarships, previous_scholarships)
    
    # Deduplicate current scholarships
    deduplicated = merge_scholarships(current_scholarships, [], keep_removed=False)
    
    # Log comparison summary
    logger.info(
        f"Comparison complete: "
        f"{len(deduplicated)} current, "
        f"{len(previous_scholarships)} previous, "
        f"{len(new_scholarships)} new"
    )
    
    # Save updated results if requested
    if save_updated and deduplicated:
        save_results(deduplicated, results_filepath)
    
    return new_scholarships, deduplicated


def get_comparison_summary(
    current: List[Dict[str, str]],
    previous: List[Dict[str, str]]
) -> Dict[str, int]:
    """
    Get a summary of the comparison between current and previous results.

    Args:
        current: List of current scholarship dictionaries.
        previous: List of previous scholarship dictionaries.

    Returns:
        Dictionary with comparison statistics.
    """
    new = find_new_scholarships(current, previous)
    removed = find_removed_scholarships(current, previous)
    
    current_urls = build_url_set(current)
    previous_urls = build_url_set(previous)
    unchanged = current_urls & previous_urls
    
    return {
        "current_count": len(current),
        "previous_count": len(previous),
        "new_count": len(new),
        "removed_count": len(removed),
        "unchanged_count": len(unchanged)
    }


# =============================================================================
# Multi-Country Comparison Functions
# =============================================================================


def load_previous_results_multi_country(
    filepath: str = DEFAULT_RESULTS_PATH
) -> Dict[str, List[Dict[str, str]]]:
    """
    Load previous scholarship results grouped by country.
    
    Handles both legacy (flat list) and multi-country (grouped) formats.
    Legacy format is migrated to multi-country format with "NO" (Norway) as default.
    
    Args:
        filepath: Path to the JSON file containing previous results.
        
    Returns:
        Dictionary mapping country codes to lists of scholarships.
    """
    logger.debug(f"Loading previous results (multi-country) from {filepath}")
    
    data = safe_read_json(filepath, default={})
    
    # Handle empty or None data
    if not data:
        logger.info("No previous results found, starting fresh")
        return {}
    
    # Check if already in multi-country format
    if isinstance(data, dict) and "scholarships_by_country" in data:
        scholarships_by_country = data.get("scholarships_by_country", {})
        if isinstance(scholarships_by_country, dict):
            total = sum(len(v) for v in scholarships_by_country.values())
            logger.info(
                f"Loaded {total} previous scholarship(s) "
                f"across {len(scholarships_by_country)} countries"
            )
            return scholarships_by_country
    
    # Handle legacy format (flat list or dict with 'scholarships' key)
    legacy_scholarships = _extract_legacy_scholarships(data)
    
    if legacy_scholarships:
        logger.info(
            f"Migrating {len(legacy_scholarships)} legacy scholarships to multi-country format"
        )
        # Migrate legacy scholarships to Norway by default
        return {"NO": legacy_scholarships}
    
    return {}


def _extract_legacy_scholarships(data: Any) -> List[Dict[str, str]]:
    """
    Extract scholarships from legacy data format.
    
    Args:
        data: Raw data from JSON file.
        
    Returns:
        List of scholarship dictionaries.
    """
    if isinstance(data, list):
        return data
    
    if isinstance(data, dict):
        scholarships = data.get("scholarships", [])
        if isinstance(scholarships, list):
            return scholarships
    
    return []


def save_results_multi_country(
    scholarships_by_country: Dict[str, List[Dict[str, str]]],
    filepath: str = DEFAULT_RESULTS_PATH,
    include_metadata: bool = True
) -> bool:
    """
    Save scholarship results grouped by country using atomic write.
    
    Args:
        scholarships_by_country: Dictionary mapping country codes to scholarship lists.
        filepath: Path to the JSON file for storing results.
        include_metadata: If True, include timestamp and count metadata.
        
    Returns:
        True if save was successful, False otherwise.
    """
    total_count = sum(len(v) for v in scholarships_by_country.values())
    logger.debug(
        f"Saving {total_count} scholarship(s) across "
        f"{len(scholarships_by_country)} countries to {filepath}"
    )
    
    if include_metadata:
        data = {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "total_count": total_count,
            "country_counts": {
                code: len(schols) 
                for code, schols in scholarships_by_country.items()
            },
            "scholarships_by_country": scholarships_by_country
        }
    else:
        data = {"scholarships_by_country": scholarships_by_country}
    
    success = safe_write_json(filepath, data)
    
    if success:
        logger.info(
            f"Successfully saved {total_count} scholarship(s) "
            f"across {len(scholarships_by_country)} countries"
        )
    else:
        logger.error(f"Failed to save multi-country results to {filepath}")
    
    return success


def find_new_scholarships_by_country(
    current_by_country: Dict[str, List[Dict[str, str]]],
    previous_by_country: Dict[str, List[Dict[str, str]]]
) -> Dict[str, List[Dict[str, str]]]:
    """
    Find new scholarships for each country.
    
    Args:
        current_by_country: Current scholarships grouped by country.
        previous_by_country: Previous scholarships grouped by country.
        
    Returns:
        Dictionary mapping country codes to lists of new scholarships.
    """
    new_by_country: Dict[str, List[Dict[str, str]]] = {}
    
    # Get all country codes from both current and previous
    all_countries = set(current_by_country.keys()) | set(previous_by_country.keys())
    
    for country_code in all_countries:
        current = current_by_country.get(country_code, [])
        previous = previous_by_country.get(country_code, [])
        
        new_scholarships = find_new_scholarships(current, previous)
        
        if new_scholarships:
            new_by_country[country_code] = new_scholarships
            logger.debug(
                f"Found {len(new_scholarships)} new scholarship(s) for {country_code}"
            )
    
    total_new = sum(len(v) for v in new_by_country.values())
    logger.info(
        f"Found {total_new} total new scholarship(s) "
        f"across {len(new_by_country)} countries"
    )
    
    return new_by_country


def compare_and_update_multi_country(
    current_by_country: Dict[str, List[Dict[str, str]]],
    results_filepath: str = DEFAULT_RESULTS_PATH,
    save_updated: bool = True
) -> Tuple[Dict[str, List[Dict[str, str]]], Dict[str, List[Dict[str, str]]]]:
    """
    Compare current scholarships with previous results by country.
    
    This is the main comparison function for multi-country mode that:
    1. Loads previous results (handles legacy migration)
    2. Finds new scholarships per country
    3. Optionally saves updated results
    
    Args:
        current_by_country: Current scholarships grouped by country.
        results_filepath: Path to the results JSON file.
        save_updated: If True, save the updated results to file.
        
    Returns:
        Tuple of (new_by_country, all_current_by_country).
    """
    logger.info("Starting multi-country scholarship comparison")
    
    # Load previous results
    previous_by_country = load_previous_results_multi_country(results_filepath)
    
    # Find new scholarships
    new_by_country = find_new_scholarships_by_country(
        current_by_country, previous_by_country
    )
    
    # Merge current with previous for each country (deduplicate)
    merged_by_country: Dict[str, List[Dict[str, str]]] = {}
    
    for country_code in set(current_by_country.keys()) | set(previous_by_country.keys()):
        current = current_by_country.get(country_code, [])
        merged = merge_scholarships(current, [], keep_removed=False)
        if merged:
            merged_by_country[country_code] = merged
    
    # Log comparison summary
    total_current = sum(len(v) for v in merged_by_country.values())
    total_previous = sum(len(v) for v in previous_by_country.values())
    total_new = sum(len(v) for v in new_by_country.values())
    
    logger.info(
        f"Multi-country comparison complete: "
        f"{total_current} current, {total_previous} previous, {total_new} new"
    )
    
    # Save updated results if requested
    if save_updated and merged_by_country:
        save_results_multi_country(merged_by_country, results_filepath)
    
    return new_by_country, merged_by_country


def get_comparison_summary_multi_country(
    current_by_country: Dict[str, List[Dict[str, str]]],
    previous_by_country: Dict[str, List[Dict[str, str]]]
) -> Dict[str, Any]:
    """
    Get a summary of the multi-country comparison.
    
    Args:
        current_by_country: Current scholarships by country.
        previous_by_country: Previous scholarships by country.
        
    Returns:
        Dictionary with comparison statistics by country and totals.
    """
    new_by_country = find_new_scholarships_by_country(
        current_by_country, previous_by_country
    )
    
    by_country: Dict[str, Dict[str, int]] = {}
    all_countries = set(current_by_country.keys()) | set(previous_by_country.keys())
    
    for country_code in all_countries:
        current = current_by_country.get(country_code, [])
        previous = previous_by_country.get(country_code, [])
        new = new_by_country.get(country_code, [])
        
        by_country[country_code] = {
            "current": len(current),
            "previous": len(previous),
            "new": len(new)
        }
    
    return {
        "by_country": by_country,
        "total_current": sum(len(v) for v in current_by_country.values()),
        "total_previous": sum(len(v) for v in previous_by_country.values()),
        "total_new": sum(len(v) for v in new_by_country.values())
    }

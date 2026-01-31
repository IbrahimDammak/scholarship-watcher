"""
Compare module for the Scholarship Watcher pipeline.

This module handles comparing current scholarships with previous results
to detect new entries, and safely persisting updated results.
"""

import os
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

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

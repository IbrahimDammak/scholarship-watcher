"""
Filter module for the Scholarship Watcher pipeline.

This module handles filtering scholarships based on relevance criteria:
- Country-specific content (configurable, supports multiple countries)
- Cloud / IT / Computer Science / Engineering keywords
"""

import re
from typing import Dict, List, Optional, Set, TYPE_CHECKING

from src.utils import get_logger, CountryConfig, load_countries_config

if TYPE_CHECKING:
    from src.utils import CountryConfig


# Module logger
logger = get_logger("filter")


# Keywords indicating Norway relevance (case-insensitive)
NORWAY_KEYWORDS: Set[str] = {
    "norway",
    "norwegian",
    "norge",
    "norsk",
    "oslo",
    "bergen",
    "trondheim",
    "stavanger",
    "tromsø",
    "tromso",
    "ntnu",
    "uio",
    "uib",
    "nordic",
    "scandinavia",
    "scandinavian",
}

# Keywords indicating Cloud/IT/Computer Science/Engineering relevance (case-insensitive)
TECH_KEYWORDS: Set[str] = {
    # Computer Science
    "computer science",
    "computer engineering",
    "computing",
    "informatics",
    "computational",
    "software engineering",
    "software development",
    # IT general
    "information technology",
    "information systems",
    "it ",  # space to avoid matching "with" etc
    "ict",
    "tech",
    "technology",
    "digital",
    # Cloud and infrastructure
    "cloud",
    "cloud computing",
    "aws",
    "azure",
    "gcp",
    "devops",
    "infrastructure",
    "kubernetes",
    "docker",
    # Data and AI
    "data science",
    "data engineering",
    "machine learning",
    "artificial intelligence",
    "ai ",  # space to avoid false positives
    "ml ",
    "deep learning",
    "big data",
    "analytics",
    # Cybersecurity
    "cybersecurity",
    "cyber security",
    "information security",
    "network security",
    # Engineering
    "engineering",
    "electrical engineering",
    "electronics",
    # Programming
    "programming",
    "developer",
    "coding",
    "python",
    "java",
    "javascript",
    # Networks
    "networking",
    "telecommunications",
    # STEM general
    "stem",
    "science",
    "mathematics",
    "physics",
}

# Keywords that indicate false positives (non-scholarship content)
FALSE_POSITIVE_KEYWORDS: Set[str] = {
    "login",
    "sign in",
    "sign up",
    "register",
    "subscribe",
    "newsletter",
    "cookie",
    "privacy policy",
    "terms of service",
    "contact us",
    "about us",
    "faq",
    "help center",
    "support",
    "advertisement",
    "sponsored",
    "cart",
    "checkout",
    "add to cart",
}


def normalize_text_for_matching(text: Optional[str]) -> str:
    """
    Normalize text for case-insensitive keyword matching.

    Args:
        text: Text to normalize. Can be None or empty string.

    Returns:
        Lowercase text with normalized whitespace, or empty string if input is None/empty.
    """
    if not text:
        return ""
    # Convert to lowercase and normalize whitespace
    normalized = re.sub(r"\s+", " ", text.lower())
    return normalized


def contains_any_keyword(text: str, keywords: Set[str]) -> bool:
    """
    Check if text contains any of the specified keywords.

    Performs case-insensitive matching.

    Args:
        text: Text to search in.
        keywords: Set of keywords to look for.

    Returns:
        True if any keyword is found, False otherwise.
    """
    if not text:
        return False
    
    normalized = normalize_text_for_matching(text)
    
    for keyword in keywords:
        # Use word boundary for short keywords to avoid false matches
        if len(keyword) <= 3:
            pattern = rf"\b{re.escape(keyword.lower())}\b"
            if re.search(pattern, normalized):
                return True
        else:
            if keyword.lower() in normalized:
                return True
    
    return False


def is_likely_false_positive(scholarship: Dict[str, str]) -> bool:
    """
    Check if a scholarship entry is likely a false positive.

    Args:
        scholarship: Dictionary with 'title' and 'url' keys.

    Returns:
        True if the entry appears to be a false positive, False otherwise.
    """
    title = scholarship.get("title", "")
    url = scholarship.get("url", "")
    combined = f"{title} {url}"
    
    return contains_any_keyword(combined, FALSE_POSITIVE_KEYWORDS)


def is_norway_relevant(scholarship: Dict[str, str]) -> bool:
    """
    Check if a scholarship is relevant to Norway.

    Args:
        scholarship: Dictionary with 'title' and 'url' keys.

    Returns:
        True if Norway-related, False otherwise.
    """
    title = scholarship.get("title", "")
    url = scholarship.get("url", "")
    combined = f"{title} {url}"
    
    return contains_any_keyword(combined, NORWAY_KEYWORDS)


def is_tech_relevant(scholarship: Dict[str, str]) -> bool:
    """
    Check if a scholarship is relevant to Cloud/IT/Computer Science/Engineering.

    Args:
        scholarship: Dictionary with 'title' and 'url' keys.

    Returns:
        True if tech-related, False otherwise.
    """
    title = scholarship.get("title", "")
    url = scholarship.get("url", "")
    combined = f"{title} {url}"
    
    return contains_any_keyword(combined, TECH_KEYWORDS)


def calculate_relevance_score(scholarship: Dict[str, str]) -> int:
    """
    Calculate a relevance score for a scholarship.

    Higher scores indicate more relevant scholarships.

    Args:
        scholarship: Dictionary with 'title' and 'url' keys.

    Returns:
        Integer relevance score (0-100).
    """
    score = 0
    title = scholarship.get("title", "")
    url = scholarship.get("url", "")
    combined = f"{title} {url}"
    normalized = normalize_text_for_matching(combined)
    
    # Count Norway keyword matches
    norway_matches = sum(1 for kw in NORWAY_KEYWORDS if kw.lower() in normalized)
    score += min(norway_matches * 15, 45)  # Max 45 points for Norway
    
    # Count tech keyword matches
    tech_matches = sum(1 for kw in TECH_KEYWORDS if kw.lower() in normalized)
    score += min(tech_matches * 10, 45)  # Max 45 points for tech
    
    # Bonus for title containing key terms
    title_normalized = normalize_text_for_matching(title)
    if "scholarship" in title_normalized:
        score += 5
    if "phd" in title_normalized or "master" in title_normalized:
        score += 5
    
    return min(score, 100)


def filter_scholarships(
    scholarships: List[Dict[str, str]],
    require_norway: bool = True,
    require_tech: bool = True,
    min_relevance_score: int = 0,
    exclude_false_positives: bool = True
) -> List[Dict[str, str]]:
    """
    Filter scholarships based on relevance criteria.

    Args:
        scholarships: List of dictionaries with 'title' and 'url' keys.
        require_norway: If True, only include Norway-related scholarships.
        require_tech: If True, only include tech-related scholarships.
        min_relevance_score: Minimum relevance score to include (0-100).
        exclude_false_positives: If True, exclude likely false positives.

    Returns:
        Filtered list of scholarships meeting all criteria.
    """
    if not scholarships:
        logger.info("No scholarships to filter")
        return []
    
    logger.info(f"Filtering {len(scholarships)} scholarship(s)")
    
    filtered = []
    stats = {
        "total": len(scholarships),
        "false_positives": 0,
        "not_norway": 0,
        "not_tech": 0,
        "low_score": 0,
        "passed": 0
    }
    
    for scholarship in scholarships:
        # Skip false positives
        if exclude_false_positives and is_likely_false_positive(scholarship):
            stats["false_positives"] += 1
            logger.debug(f"Filtered (false positive): {scholarship.get('title', 'N/A')}")
            continue
        
        # Check Norway relevance
        norway_match = is_norway_relevant(scholarship)
        if require_norway and not norway_match:
            stats["not_norway"] += 1
            logger.debug(f"Filtered (not Norway): {scholarship.get('title', 'N/A')}")
            continue
        
        # Check tech relevance
        tech_match = is_tech_relevant(scholarship)
        if require_tech and not tech_match:
            stats["not_tech"] += 1
            logger.debug(f"Filtered (not tech): {scholarship.get('title', 'N/A')}")
            continue
        
        # Check relevance score
        score = calculate_relevance_score(scholarship)
        if score < min_relevance_score:
            stats["low_score"] += 1
            logger.debug(f"Filtered (low score {score}): {scholarship.get('title', 'N/A')}")
            continue
        
        # Scholarship passed all filters
        stats["passed"] += 1
        filtered.append(scholarship)
        logger.debug(f"Passed (score {score}): {scholarship.get('title', 'N/A')}")
    
    # Log filtering summary
    logger.info(
        f"Filter results: {stats['passed']}/{stats['total']} passed "
        f"(false_positives={stats['false_positives']}, "
        f"not_norway={stats['not_norway']}, "
        f"not_tech={stats['not_tech']}, "
        f"low_score={stats['low_score']})"
    )
    
    return filtered


def filter_scholarships_flexible(
    scholarships: List[Dict[str, str]],
    require_both: bool = False
) -> List[Dict[str, str]]:
    """
    Filter scholarships with flexible relevance matching.

    If require_both is False, scholarships matching either Norway OR tech
    criteria will be included. This is useful when sources are already
    focused on a specific region or field.

    Args:
        scholarships: List of dictionaries with 'title' and 'url' keys.
        require_both: If True, require both Norway AND tech relevance.
                      If False, require Norway OR tech relevance.

    Returns:
        Filtered list of scholarships.
    """
    if not scholarships:
        return []
    
    filtered = []
    
    for scholarship in scholarships:
        # Always exclude false positives
        if is_likely_false_positive(scholarship):
            continue
        
        norway_match = is_norway_relevant(scholarship)
        tech_match = is_tech_relevant(scholarship)
        
        if require_both:
            if norway_match and tech_match:
                filtered.append(scholarship)
        else:
            if norway_match or tech_match:
                filtered.append(scholarship)
    
    logger.info(
        f"Flexible filter ({'AND' if require_both else 'OR'}): "
        f"{len(filtered)}/{len(scholarships)} passed"
    )
    
    return filtered


# =============================================================================
# Multi-Country Filtering Functions
# =============================================================================


def is_country_relevant(
    scholarship: Dict[str, str],
    country: "CountryConfig"
) -> bool:
    """
    Check if a scholarship is relevant to a specific country.
    
    Args:
        scholarship: Dictionary with 'title', 'url', and optionally 'description' keys.
        country: CountryConfig object with keywords and domain patterns.
        
    Returns:
        True if scholarship matches country criteria, False otherwise.
    """
    title = scholarship.get("title", "")
    url = scholarship.get("url", "").lower()
    description = scholarship.get("description", "")
    
    # Combine all text fields for keyword search
    combined = f"{title} {url} {description}"
    
    # Check keywords
    if contains_any_keyword(combined, country.keywords):
        return True
    
    # Check domain patterns in URL
    for pattern in country.domain_patterns:
        if pattern in url:
            return True
    
    return False


def get_matching_countries(
    scholarship: Dict[str, str],
    countries: List["CountryConfig"]
) -> List["CountryConfig"]:
    """
    Get all countries that a scholarship matches.
    
    A scholarship may match multiple countries if it contains
    keywords or domain patterns from multiple countries.
    
    Args:
        scholarship: Dictionary with 'title' and 'url' keys.
        countries: List of CountryConfig objects to check against.
        
    Returns:
        List of matching CountryConfig objects.
    """
    return [
        country for country in countries
        if is_country_relevant(scholarship, country)
    ]


def filter_scholarships_by_country(
    scholarships: List[Dict[str, str]],
    country: "CountryConfig",
    require_tech: bool = True,
    exclude_false_positives: bool = True
) -> List[Dict[str, str]]:
    """
    Filter scholarships for a specific country.
    
    Args:
        scholarships: List of scholarship dictionaries.
        country: CountryConfig object defining country criteria.
        require_tech: If True, also require tech relevance.
        exclude_false_positives: If True, exclude false positives.
        
    Returns:
        List of scholarships matching the country criteria.
    """
    if not scholarships:
        return []
    
    filtered = []
    
    for scholarship in scholarships:
        if exclude_false_positives and is_likely_false_positive(scholarship):
            continue
        
        if not is_country_relevant(scholarship, country):
            continue
        
        if require_tech and not is_tech_relevant(scholarship):
            continue
        
        filtered.append(scholarship)
    
    logger.debug(
        f"Country filter [{country.code}]: {len(filtered)}/{len(scholarships)} passed"
    )
    
    return filtered


def filter_scholarships_multi_country(
    scholarships: List[Dict[str, str]],
    countries: Optional[List["CountryConfig"]] = None,
    require_tech: bool = True,
    exclude_false_positives: bool = True
) -> Dict[str, List[Dict[str, str]]]:
    """
    Filter scholarships and group by country.
    
    Each scholarship is assigned to all matching countries.
    A scholarship may appear under multiple countries if it matches
    multiple country criteria.
    
    Args:
        scholarships: List of scholarship dictionaries.
        countries: List of CountryConfig objects. If None, loads from config.
        require_tech: If True, also require tech relevance.
        exclude_false_positives: If True, exclude false positives.
        
    Returns:
        Dictionary mapping country codes to lists of scholarships.
    """
    if countries is None:
        countries = load_countries_config(enabled_only=True)
    
    if not scholarships:
        return {country.code: [] for country in countries}
    
    logger.info(
        f"Multi-country filtering {len(scholarships)} scholarship(s) "
        f"for {len(countries)} countries"
    )
    
    results: Dict[str, List[Dict[str, str]]] = {
        country.code: [] for country in countries
    }
    
    stats = {
        "total": len(scholarships),
        "false_positives": 0,
        "not_tech": 0,
        "no_country": 0,
        "matched": 0
    }
    
    for scholarship in scholarships:
        if exclude_false_positives and is_likely_false_positive(scholarship):
            stats["false_positives"] += 1
            continue
        
        if require_tech and not is_tech_relevant(scholarship):
            stats["not_tech"] += 1
            continue
        
        matching_countries = get_matching_countries(scholarship, countries)
        
        if not matching_countries:
            stats["no_country"] += 1
            continue
        
        stats["matched"] += 1
        
        for country in matching_countries:
            # Add country info to scholarship
            enriched = scholarship.copy()
            enriched["country_code"] = country.code
            enriched["country_name"] = country.name
            results[country.code].append(enriched)
    
    # Log summary
    country_counts = {code: len(schols) for code, schols in results.items() if schols}
    logger.info(
        f"Multi-country filter results: {stats['matched']}/{stats['total']} matched, "
        f"by country: {country_counts}"
    )
    logger.debug(
        f"Filter stats: false_positives={stats['false_positives']}, "
        f"not_tech={stats['not_tech']}, no_country={stats['no_country']}"
    )
    
    return results


def get_all_filtered_scholarships(
    scholarships_by_country: Dict[str, List[Dict[str, str]]]
) -> List[Dict[str, str]]:
    """
    Flatten country-grouped scholarships into a single deduplicated list.
    
    Args:
        scholarships_by_country: Dictionary mapping country codes to scholarship lists.
        
    Returns:
        Deduplicated list of all scholarships.
    """
    seen_urls: Set[str] = set()
    all_scholarships: List[Dict[str, str]] = []
    
    for scholarships in scholarships_by_country.values():
        for scholarship in scholarships:
            url = scholarship.get("url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_scholarships.append(scholarship)
    
    return all_scholarships


def count_scholarships_by_country(
    scholarships_by_country: Dict[str, List[Dict[str, str]]]
) -> Dict[str, int]:
    """
    Get scholarship counts by country.
    
    Args:
        scholarships_by_country: Dictionary mapping country codes to scholarship lists.
        
    Returns:
        Dictionary mapping country codes to counts.
    """
    return {code: len(schols) for code, schols in scholarships_by_country.items()}

"""
Parse module for the Scholarship Watcher pipeline.

This module handles parsing HTML content from scholarship pages
and extracting scholarship information (title, URL).
"""

import re
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from src.fetch import FetchResult
from src.utils import get_logger, normalize_url, sanitize_text


# Module logger
logger = get_logger("parse")


# Common selectors for scholarship listings across different websites
# These are patterns commonly used by scholarship websites
SCHOLARSHIP_SELECTORS = [
    # Generic article/list item patterns
    "article",
    ".scholarship",
    ".scholarship-item",
    ".scholarship-listing",
    ".post",
    ".entry",
    ".listing-item",
    ".result-item",
    # Specific patterns for known scholarship sites
    ".scholarship-card",
    ".program-card",
    ".opportunity",
    ".funding-item",
    # Table-based listings
    "table.scholarships tr",
    ".scholarship-table tr",
    # List-based patterns
    "ul.scholarships li",
    "ol.scholarships li",
    ".scholarship-list li",
]

# Patterns for finding scholarship links within containers
LINK_SELECTORS = [
    "h1 a",
    "h2 a",
    "h3 a",
    "h4 a",
    ".title a",
    ".scholarship-title a",
    ".entry-title a",
    ".post-title a",
    "a.scholarship-link",
    "a.title-link",
    "a[href*='scholarship']",
]


def extract_title_from_element(element: Tag) -> Optional[str]:
    """
    Extract a meaningful title from an HTML element.

    Tries multiple strategies to find the most appropriate title text.

    Args:
        element: BeautifulSoup Tag element to extract title from.

    Returns:
        Extracted title string or None if no suitable title found.
    """
    # Try heading elements first
    for heading_tag in ["h1", "h2", "h3", "h4", "h5", "h6"]:
        heading = element.find(heading_tag)
        if heading:
            text = sanitize_text(heading.get_text())
            if text and len(text) > 5:  # Minimum title length
                return text
    
    # Try title-like class elements
    for selector in [".title", ".scholarship-title", ".entry-title", ".post-title"]:
        title_elem = element.select_one(selector)
        if title_elem:
            text = sanitize_text(title_elem.get_text())
            if text and len(text) > 5:
                return text
    
    # Try first link with substantial text
    first_link = element.find("a")
    if first_link:
        text = sanitize_text(first_link.get_text())
        if text and len(text) > 10:
            return text
    
    return None


def extract_url_from_element(element: Tag, base_url: str) -> Optional[str]:
    """
    Extract the most relevant URL from an HTML element.

    Args:
        element: BeautifulSoup Tag element to extract URL from.
        base_url: Base URL for resolving relative URLs.

    Returns:
        Absolute URL string or None if no suitable URL found.
    """
    # Try specific link selectors first
    for selector in LINK_SELECTORS:
        link = element.select_one(selector)
        if link and link.get("href"):
            href = str(link["href"])
            # Skip anchor-only links and javascript
            if href.startswith("#") or href.startswith("javascript:"):
                continue
            return normalize_url(href, base_url)
    
    # Fallback: find any link with href
    all_links = element.find_all("a", href=True)
    for link in all_links:
        href = str(link["href"])
        if href.startswith("#") or href.startswith("javascript:"):
            continue
        # Prefer links that look like scholarship pages
        if any(kw in href.lower() for kw in ["scholarship", "program", "grant", "funding", "apply"]):
            return normalize_url(href, base_url)
    
    # Last resort: return first valid link
    for link in all_links:
        href = str(link["href"])
        if not href.startswith("#") and not href.startswith("javascript:"):
            return normalize_url(href, base_url)
    
    return None


def parse_with_selectors(soup: BeautifulSoup, base_url: str) -> List[Dict[str, str]]:
    """
    Parse scholarship information using predefined CSS selectors.

    Args:
        soup: BeautifulSoup object with parsed HTML.
        base_url: Base URL for resolving relative URLs.

    Returns:
        List of dictionaries with 'title' and 'url' keys.
    """
    scholarships = []
    seen_urls = set()
    
    for selector in SCHOLARSHIP_SELECTORS:
        try:
            elements = soup.select(selector)
            for element in elements:
                title = extract_title_from_element(element)
                url = extract_url_from_element(element, base_url)
                
                # Skip if missing required fields
                if not title or not url:
                    continue
                
                # Skip duplicates
                if url in seen_urls:
                    continue
                
                seen_urls.add(url)
                scholarships.append({
                    "title": title,
                    "url": url
                })
        except Exception as e:
            logger.debug(f"Error with selector '{selector}': {e}")
            continue
    
    return scholarships


def parse_links_with_keywords(soup: BeautifulSoup, base_url: str) -> List[Dict[str, str]]:
    """
    Parse scholarship links by finding links with scholarship-related keywords.

    This is a fallback method when structured selectors don't find results.

    Args:
        soup: BeautifulSoup object with parsed HTML.
        base_url: Base URL for resolving relative URLs.

    Returns:
        List of dictionaries with 'title' and 'url' keys.
    """
    scholarships = []
    seen_urls = set()
    
    # Keywords that indicate a scholarship link
    scholarship_keywords = [
        "scholarship", "scholarships", "grant", "grants",
        "fellowship", "fellowships", "funding", "bursary",
        "award", "stipend", "financial aid"
    ]
    
    # Find all links
    all_links = soup.find_all("a", href=True)
    
    for link in all_links:
        href = str(link["href"])
        text = sanitize_text(link.get_text())
        
        # Skip invalid links
        if href.startswith("#") or href.startswith("javascript:"):
            continue
        
        # Skip if text is too short
        if not text or len(text) < 10:
            continue
        
        # Check if link text or URL contains scholarship keywords
        combined = f"{text} {href}".lower()
        if any(kw in combined for kw in scholarship_keywords):
            url = normalize_url(href, base_url)
            
            # Skip duplicates
            if url in seen_urls:
                continue
            
            seen_urls.add(url)
            scholarships.append({
                "title": text,
                "url": url
            })
    
    return scholarships


def parse_html_content(html: str, source_url: str) -> List[Dict[str, str]]:
    """
    Parse HTML content to extract scholarship information.

    Uses multiple strategies:
    1. Structured CSS selectors for common scholarship page layouts
    2. Keyword-based link detection as fallback

    Args:
        html: Raw HTML content string.
        source_url: Source URL for resolving relative URLs.

    Returns:
        List of dictionaries with 'title' and 'url' keys.
    """
    if not html:
        logger.warning(f"Empty HTML content for {source_url}")
        return []
    
    logger.debug(f"Parsing HTML from {source_url} ({len(html)} bytes)")
    
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as e:
        logger.error(f"Failed to parse HTML from {source_url}: {e}")
        return []
    
    # Try structured selectors first
    scholarships = parse_with_selectors(soup, source_url)
    
    # If no results, try keyword-based parsing
    if not scholarships:
        logger.debug(f"No results from selectors, trying keyword parsing for {source_url}")
        scholarships = parse_links_with_keywords(soup, source_url)
    
    logger.info(f"Extracted {len(scholarships)} scholarship(s) from {source_url}")
    
    return scholarships


def parse_fetch_results(fetch_results: List[FetchResult]) -> List[Dict[str, str]]:
    """
    Parse scholarship information from a list of fetch results.

    Aggregates scholarships from all successfully fetched pages,
    removing duplicates based on URL.

    Args:
        fetch_results: List of FetchResult objects from fetch operation.

    Returns:
        Deduplicated list of dictionaries with 'title' and 'url' keys.
    """
    all_scholarships = []
    seen_urls = set()
    
    for result in fetch_results:
        if not result.success or not result.html_content:
            continue
        
        scholarships = parse_html_content(result.html_content, result.source_url)
        
        for scholarship in scholarships:
            url = scholarship["url"]
            if url not in seen_urls:
                seen_urls.add(url)
                all_scholarships.append(scholarship)
    
    logger.info(f"Total unique scholarships parsed: {len(all_scholarships)}")
    
    return all_scholarships

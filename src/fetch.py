"""
Fetch module for the Scholarship Watcher pipeline.

This module handles fetching scholarship pages from configured URLs with
proper error handling, retries, and exponential backoff.
"""

import time
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils import get_logger


# Module logger
logger = get_logger("fetch")

# Default configuration
DEFAULT_TIMEOUT = 30  # seconds
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_FACTOR = 1.0  # exponential backoff multiplier
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Default scholarship source URLs (Norway, Cloud, IT, Computer Science focus)
DEFAULT_SCHOLARSHIP_URLS = [
    "https://www.scholarshipportal.com/scholarships/norway",
    "https://www.studyinnorway.no/scholarships",
    "https://www.scholars4dev.com/tag/norway-scholarships/",
    "https://www.afterschoolafrica.com/tag/norway-scholarships/",
]


@dataclass
class FetchResult:
    """
    Represents the result of fetching a single URL.

    Attributes:
        source_url: The original URL that was fetched.
        html_content: Raw HTML content if successful, None otherwise.
        success: Whether the fetch was successful.
        error_message: Error description if fetch failed, None otherwise.
        status_code: HTTP status code if request was made, None otherwise.
    """
    source_url: str
    html_content: Optional[str]
    success: bool
    error_message: Optional[str] = None
    status_code: Optional[int] = None


def create_session(
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR
) -> requests.Session:
    """
    Create a requests session with retry configuration.

    Configures automatic retries with exponential backoff for
    transient failures (5xx errors, connection errors).

    Args:
        max_retries: Maximum number of retry attempts.
        backoff_factor: Multiplier for exponential backoff between retries.
                       Sleep time = backoff_factor * (2 ** retry_number)

    Returns:
        Configured requests.Session instance.
    """
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=int(backoff_factor),
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False
    )
    
    # Mount adapter for both HTTP and HTTPS
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set default headers
    session.headers.update({
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    })
    
    return session


def validate_url(url: str) -> bool:
    """
    Validate that a URL is well-formed and uses HTTP/HTTPS.

    Args:
        url: URL string to validate.

    Returns:
        True if URL is valid, False otherwise.
    """
    try:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False


def fetch_single_url(
    url: str,
    session: requests.Session,
    timeout: int = DEFAULT_TIMEOUT
) -> FetchResult:
    """
    Fetch a single URL and return the result.

    Args:
        url: URL to fetch.
        session: Configured requests session.
        timeout: Request timeout in seconds.

    Returns:
        FetchResult containing the fetch outcome.
    """
    logger.debug(f"Fetching URL: {url}")
    
    # Validate URL format
    if not validate_url(url):
        logger.warning(f"Invalid URL format: {url}")
        return FetchResult(
            source_url=url,
            html_content=None,
            success=False,
            error_message="Invalid URL format"
        )
    
    try:
        response = session.get(url, timeout=timeout)
        
        # Check for successful response
        if response.status_code == 200:
            logger.info(f"Successfully fetched {url} ({len(response.text)} bytes)")
            return FetchResult(
                source_url=url,
                html_content=response.text,
                success=True,
                status_code=response.status_code
            )
        else:
            logger.warning(f"HTTP {response.status_code} for {url}")
            return FetchResult(
                source_url=url,
                html_content=None,
                success=False,
                error_message=f"HTTP {response.status_code}",
                status_code=response.status_code
            )
            
    except requests.exceptions.Timeout:
        logger.warning(f"Timeout fetching {url}")
        return FetchResult(
            source_url=url,
            html_content=None,
            success=False,
            error_message="Request timeout"
        )
        
    except requests.exceptions.ConnectionError as e:
        logger.warning(f"Connection error for {url}: {e}")
        return FetchResult(
            source_url=url,
            html_content=None,
            success=False,
            error_message=f"Connection error: {str(e)}"
        )
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception for {url}: {e}")
        return FetchResult(
            source_url=url,
            html_content=None,
            success=False,
            error_message=f"Request failed: {str(e)}"
        )


def fetch_scholarship_pages(
    urls: Optional[List[str]] = None,
    timeout: int = DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    delay_between_requests: float = 1.0
) -> List[FetchResult]:
    """
    Fetch scholarship pages from a list of URLs.

    Fetches each URL sequentially with configurable retry behavior
    and delays between requests to be respectful to servers.

    Args:
        urls: List of URLs to fetch. Uses DEFAULT_SCHOLARSHIP_URLS if None.
        timeout: Request timeout in seconds per request.
        max_retries: Maximum retry attempts per URL.
        backoff_factor: Exponential backoff multiplier for retries.
        delay_between_requests: Seconds to wait between requests.

    Returns:
        List of FetchResult objects, one per URL.
    """
    if urls is None:
        urls = DEFAULT_SCHOLARSHIP_URLS
    
    if not urls:
        logger.warning("No URLs provided to fetch")
        return []
    
    logger.info(f"Starting to fetch {len(urls)} scholarship source(s)")
    
    # Create session with retry configuration
    session = create_session(
        max_retries=max_retries,
        backoff_factor=backoff_factor
    )
    
    results: List[FetchResult] = []
    
    try:
        for i, url in enumerate(urls):
            # Fetch the URL
            result = fetch_single_url(url, session, timeout)
            results.append(result)
            
            # Add delay between requests (except after the last one)
            if i < len(urls) - 1 and delay_between_requests > 0:
                time.sleep(delay_between_requests)
    
    finally:
        session.close()
    
    # Log summary
    successful = sum(1 for r in results if r.success)
    logger.info(f"Fetch complete: {successful}/{len(results)} successful")
    
    return results


def get_successful_fetches(results: List[FetchResult]) -> List[FetchResult]:
    """
    Filter fetch results to only include successful fetches.

    Args:
        results: List of FetchResult objects.

    Returns:
        List of FetchResult objects where success is True.
    """
    return [r for r in results if r.success]

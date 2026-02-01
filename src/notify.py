"""
Notify module for the Scholarship Watcher pipeline.

This module handles creating notifications when new scholarships are detected.
Supports two notification channels:
- GitHub Issues (primary)
- Email via SMTP with TLS (optional, for daily digest)

Uses the GitHub REST API with proper error handling for rate limits and
validation errors. Email notifications use smtplib with TLS encryption.
"""

import json
import os
import smtplib
import ssl
import time
from datetime import datetime
from email.message import EmailMessage
from typing import Any, Dict, List, Optional, Tuple

import requests

from src.utils import get_env_var, get_logger


# Module logger
logger = get_logger("notify")

# GitHub API configuration
GITHUB_API_BASE = "https://api.github.com"
GITHUB_API_VERSION = "2022-11-28"

# Rate limit retry configuration
MAX_RATE_LIMIT_RETRIES = 3
RATE_LIMIT_WAIT_SECONDS = 60


class GitHubAPIError(Exception):
    """Custom exception for GitHub API errors."""
    
    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[Dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


def get_github_credentials() -> Tuple[str, str]:
    """
    Get GitHub credentials from environment variables.

    Returns:
        Tuple of (github_token, github_repository).

    Raises:
        ValueError: If required environment variables are not set.
    """
    token = get_env_var("GITHUB_TOKEN", required=True)
    repository = get_env_var("GITHUB_REPOSITORY", required=True)
    
    # Type assertion - get_env_var with required=True raises ValueError if None
    assert token is not None
    assert repository is not None
    
    return token, repository


def parse_repository(repository: str) -> Tuple[str, str]:
    """
    Parse repository string into owner and repo name.

    Args:
        repository: Repository string in format "owner/repo".

    Returns:
        Tuple of (owner, repo_name).

    Raises:
        ValueError: If repository format is invalid.
    """
    if "/" not in repository:
        raise ValueError(f"Invalid repository format: {repository}. Expected 'owner/repo'")
    
    parts = repository.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid repository format: {repository}. Expected 'owner/repo'")
    
    return parts[0], parts[1]


def create_github_session(token: str) -> requests.Session:
    """
    Create a requests session configured for GitHub API.

    Args:
        token: GitHub personal access token.

    Returns:
        Configured requests.Session instance.
    """
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
        "User-Agent": "ScholarshipWatcher/1.0"
    })
    return session


def check_rate_limit(response: requests.Response) -> Tuple[bool, int]:
    """
    Check if response indicates rate limiting.

    Args:
        response: Response object from GitHub API.

    Returns:
        Tuple of (is_rate_limited, seconds_until_reset).
    """
    if response.status_code != 403:
        return False, 0
    
    # Check for rate limit headers
    remaining = response.headers.get("X-RateLimit-Remaining", "1")
    reset_time = response.headers.get("X-RateLimit-Reset", "0")
    
    if remaining == "0":
        try:
            reset_timestamp = int(reset_time)
            wait_seconds = max(0, reset_timestamp - int(time.time()))
            return True, wait_seconds
        except ValueError:
            return True, RATE_LIMIT_WAIT_SECONDS
    
    # Check response body for rate limit message
    try:
        data = response.json()
        if "rate limit" in data.get("message", "").lower():
            return True, RATE_LIMIT_WAIT_SECONDS
    except Exception:
        pass
    
    return False, 0


def format_issue_body(scholarships: List[Dict[str, str]]) -> str:
    """
    Format the GitHub Issue body with scholarship information.

    Args:
        scholarships: List of scholarship dictionaries with 'title' and 'url'.

    Returns:
        Formatted markdown string for issue body.
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    
    lines = [
        "## 🎓 New Scholarships Detected!",
        "",
        f"**Detection Time:** {timestamp}",
        f"**Number of New Scholarships:** {len(scholarships)}",
        "",
        "---",
        "",
        "### Scholarships Found:",
        ""
    ]
    
    for i, scholarship in enumerate(scholarships, 1):
        title = scholarship.get("title", "Unknown Title")
        url = scholarship.get("url", "#")
        
        # Escape any markdown special characters in title
        escaped_title = title.replace("[", "\\[").replace("]", "\\]")
        
        lines.append(f"{i}. [{escaped_title}]({url})")
    
    lines.extend([
        "",
        "---",
        "",
        "*This issue was automatically created by the Scholarship Watcher pipeline.*",
        "*Please review each scholarship for eligibility and deadlines.*"
    ])
    
    return "\n".join(lines)


def format_issue_title(scholarship_count: int) -> str:
    """
    Format the GitHub Issue title.

    Args:
        scholarship_count: Number of new scholarships found.

    Returns:
        Issue title string.
    """
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    plural = "s" if scholarship_count != 1 else ""
    return f"🎓 {scholarship_count} New Scholarship{plural} Found - {date_str}"


def format_issue_title_multi_country(
    scholarships_by_country: Dict[str, List[Dict[str, str]]],
    country_names: Dict[str, str]
) -> str:
    """
    Format GitHub Issue title for multi-country results.
    
    Args:
        scholarships_by_country: Scholarships grouped by country code.
        country_names: Mapping of country codes to names.
        
    Returns:
        Issue title string.
    """
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    total_count = sum(len(v) for v in scholarships_by_country.values())
    country_count = len([c for c, s in scholarships_by_country.items() if s])
    
    plural_s = "s" if total_count != 1 else ""
    plural_c = "ies" if country_count != 1 else "y"
    
    return f"🎓 {total_count} New Scholarship{plural_s} ({country_count} Countr{plural_c}) - {date_str}"


def format_issue_body_multi_country(
    scholarships_by_country: Dict[str, List[Dict[str, str]]],
    country_names: Dict[str, str]
) -> str:
    """
    Format GitHub Issue body with scholarships grouped by country.
    
    Args:
        scholarships_by_country: Scholarships grouped by country code.
        country_names: Mapping of country codes to names.
        
    Returns:
        Formatted markdown string for issue body.
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    total_count = sum(len(v) for v in scholarships_by_country.values())
    
    lines = [
        "## 🎓 New Scholarships Detected!",
        "",
        f"**Detection Time:** {timestamp}",
        f"**Total New Scholarships:** {total_count}",
        f"**Countries:** {len([c for c, s in scholarships_by_country.items() if s])}",
        "",
        "---",
        ""
    ]
    
    # Sort countries by name for consistent output
    sorted_countries = sorted(
        [(code, schols) for code, schols in scholarships_by_country.items() if schols],
        key=lambda x: country_names.get(x[0], x[0])
    )
    
    for country_code, scholarships in sorted_countries:
        country_name = country_names.get(country_code, country_code)
        flag = _get_country_flag(country_code)
        
        lines.extend([
            f"### {flag} {country_name} ({len(scholarships)})",
            ""
        ])
        
        for i, scholarship in enumerate(scholarships, 1):
            title = scholarship.get("title", "Unknown Title")
            url = scholarship.get("url", "#")
            
            escaped_title = title.replace("[", "\\[").replace("]", "\\]")
            lines.append(f"{i}. [{escaped_title}]({url})")
        
        lines.append("")
    
    lines.extend([
        "---",
        "",
        "*This issue was automatically created by the Scholarship Watcher pipeline.*",
        "*Please review each scholarship for eligibility and deadlines.*"
    ])
    
    return "\n".join(lines)


def _get_country_flag(country_code: str) -> str:
    """
    Get emoji flag for a country code.
    
    Args:
        country_code: ISO-2 country code.
        
    Returns:
        Flag emoji or globe emoji if not found.
    """
    flags = {
        # Nordic
        "NO": "🇳🇴",
        "SE": "🇸🇪",
        "DK": "🇩🇰",
        "FI": "🇫🇮",
        # Western Europe
        "DE": "🇩🇪",
        "NL": "🇳🇱",
        "BE": "🇧🇪",
        "LU": "🇱🇺",
        "FR": "🇫🇷",
        "CH": "🇨🇭",
        "AT": "🇦🇹",
        # Southern Europe
        "IT": "🇮🇹",
        "ES": "🇪🇸",
        "PT": "🇵🇹",
        "GR": "🇬🇷",
        "MT": "🇲🇹",
        "CY": "🇨🇾",
        # Central Europe
        "PL": "🇵🇱",
        "CZ": "🇨🇿",
        "HU": "🇭🇺",
        "SK": "🇸🇰",
        "SI": "🇸🇮",
        # Eastern Europe
        "EE": "🇪🇪",
        "LV": "🇱🇻",
        "LT": "🇱🇹",
        "RO": "🇷🇴",
        "BG": "🇧🇬",
        "HR": "🇭🇷",
        # British Isles
        "IE": "🇮🇪",
        "UK": "🇬🇧",
        "GB": "🇬🇧",
        # EU
        "EU": "🇪🇺",
        # Other
        "US": "🇺🇸",
        "CA": "🇨🇦",
        "AU": "🇦🇺",
        "JP": "🇯🇵",
        "KR": "🇰🇷",
        "SG": "🇸🇬",
    }
    return flags.get(country_code.upper(), "🌍")


def create_issue(
    session: requests.Session,
    owner: str,
    repo: str,
    title: str,
    body: str,
    labels: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Create a GitHub Issue.

    Args:
        session: Configured requests session.
        owner: Repository owner.
        repo: Repository name.
        title: Issue title.
        body: Issue body (markdown).
        labels: Optional list of labels to apply.

    Returns:
        Created issue data from GitHub API.

    Raises:
        GitHubAPIError: If issue creation fails.
    """
    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/issues"
    
    payload: Dict[str, Any] = {
        "title": title,
        "body": body
    }
    
    if labels:
        payload["labels"] = labels
    
    logger.debug(f"Creating issue in {owner}/{repo}")
    
    for attempt in range(MAX_RATE_LIMIT_RETRIES):
        try:
            response = session.post(url, json=payload, timeout=30)
            
            # Check for rate limiting
            is_limited, wait_time = check_rate_limit(response)
            if is_limited:
                if attempt < MAX_RATE_LIMIT_RETRIES - 1:
                    logger.warning(f"Rate limited, waiting {wait_time}s (attempt {attempt + 1})")
                    time.sleep(min(wait_time, RATE_LIMIT_WAIT_SECONDS))
                    continue
                else:
                    raise GitHubAPIError(
                        "GitHub API rate limit exceeded",
                        status_code=403
                    )
            
            # Check for success
            if response.status_code == 201:
                data = response.json()
                logger.info(f"Successfully created issue #{data.get('number')}")
                return data
            
            # Handle specific error cases
            if response.status_code == 401:
                raise GitHubAPIError(
                    "GitHub authentication failed. Check GITHUB_TOKEN.",
                    status_code=401
                )
            
            if response.status_code == 403:
                raise GitHubAPIError(
                    "GitHub permission denied. Token may lack 'issues' scope.",
                    status_code=403
                )
            
            if response.status_code == 404:
                raise GitHubAPIError(
                    f"Repository {owner}/{repo} not found or not accessible.",
                    status_code=404
                )
            
            if response.status_code == 422:
                try:
                    error_data = response.json()
                    message = error_data.get("message", "Validation failed")
                    errors = error_data.get("errors", [])
                    error_details = "; ".join(
                        e.get("message", str(e)) for e in errors
                    ) if errors else ""
                    raise GitHubAPIError(
                        f"GitHub validation error: {message}. {error_details}",
                        status_code=422,
                        response=error_data
                    )
                except (json.JSONDecodeError, KeyError):
                    raise GitHubAPIError(
                        "GitHub validation error",
                        status_code=422
                    )
            
            # Generic error
            raise GitHubAPIError(
                f"GitHub API error: HTTP {response.status_code}",
                status_code=response.status_code
            )
            
        except requests.exceptions.Timeout:
            if attempt < MAX_RATE_LIMIT_RETRIES - 1:
                logger.warning(f"Request timeout, retrying (attempt {attempt + 1})")
                time.sleep(5)
                continue
            raise GitHubAPIError("GitHub API request timeout")
            
        except requests.exceptions.RequestException as e:
            raise GitHubAPIError(f"GitHub API request failed: {e}")
    
    raise GitHubAPIError("Failed to create issue after retries")


def notify_new_scholarships(
    scholarships: List[Dict[str, str]],
    labels: Optional[List[str]] = None,
    dry_run: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Create a GitHub Issue to notify about new scholarships.

    Args:
        scholarships: List of new scholarship dictionaries.
        labels: Optional list of labels for the issue.
        dry_run: If True, don't actually create the issue, just log.

    Returns:
        Created issue data if successful, None if no scholarships or dry_run.

    Raises:
        ValueError: If required environment variables are missing.
        GitHubAPIError: If issue creation fails.
    """
    if not scholarships:
        logger.info("No new scholarships to notify about")
        return None
    
    logger.info(f"Notifying about {len(scholarships)} new scholarship(s)")
    
    # Get credentials
    try:
        token, repository = get_github_credentials()
        owner, repo = parse_repository(repository)
    except ValueError as e:
        logger.error(f"Failed to get GitHub credentials: {e}")
        raise
    
    # Format issue content
    title = format_issue_title(len(scholarships))
    body = format_issue_body(scholarships)
    
    if dry_run:
        logger.info(f"[DRY RUN] Would create issue: {title}")
        logger.debug(f"[DRY RUN] Issue body:\n{body}")
        return None
    
    # Create session and issue
    session = create_github_session(token)
    
    try:
        issue_data = create_issue(
            session=session,
            owner=owner,
            repo=repo,
            title=title,
            body=body,
            labels=labels or ["scholarship", "automated"]
        )
        
        issue_url = issue_data.get("html_url", "")
        logger.info(f"Issue created successfully: {issue_url}")
        
        return issue_data
        
    except GitHubAPIError as e:
        logger.error(f"Failed to create GitHub issue: {e}")
        raise
    finally:
        session.close()


def notify_new_scholarships_multi_country(
    scholarships_by_country: Dict[str, List[Dict[str, str]]],
    country_names: Dict[str, str],
    labels: Optional[List[str]] = None,
    dry_run: bool = False
) -> Optional[Dict[str, Any]]:
    """
    Create a GitHub Issue with scholarships grouped by country.
    
    Args:
        scholarships_by_country: Scholarships grouped by country code.
        country_names: Mapping of country codes to human-readable names.
        labels: Optional list of labels for the issue.
        dry_run: If True, don't actually create the issue, just log.
        
    Returns:
        Created issue data if successful, None if no scholarships or dry_run.
        
    Raises:
        ValueError: If required environment variables are missing.
        GitHubAPIError: If issue creation fails.
    """
    # Filter out empty countries
    non_empty = {c: s for c, s in scholarships_by_country.items() if s}
    
    if not non_empty:
        logger.info("No new scholarships to notify about (multi-country)")
        return None
    
    total_count = sum(len(v) for v in non_empty.values())
    logger.info(
        f"Notifying about {total_count} new scholarship(s) "
        f"across {len(non_empty)} countries"
    )
    
    # Get credentials
    try:
        token, repository = get_github_credentials()
        owner, repo = parse_repository(repository)
    except ValueError as e:
        logger.error(f"Failed to get GitHub credentials: {e}")
        raise
    
    # Format issue content
    title = format_issue_title_multi_country(non_empty, country_names)
    body = format_issue_body_multi_country(non_empty, country_names)
    
    if dry_run:
        logger.info(f"[DRY RUN] Would create issue: {title}")
        logger.debug(f"[DRY RUN] Issue body:\n{body}")
        return None
    
    # Create session and issue
    session = create_github_session(token)
    
    # Build labels including country codes
    all_labels = list(labels or ["scholarship", "automated"])
    for country_code in non_empty.keys():
        country_label = f"country:{country_code.lower()}"
        if country_label not in all_labels:
            all_labels.append(country_label)
    
    try:
        issue_data = create_issue(
            session=session,
            owner=owner,
            repo=repo,
            title=title,
            body=body,
            labels=all_labels
        )
        
        issue_url = issue_data.get("html_url", "")
        logger.info(f"Multi-country issue created successfully: {issue_url}")
        
        return issue_data
        
    except GitHubAPIError as e:
        logger.error(f"Failed to create GitHub issue: {e}")
        raise
    finally:
        session.close()


def check_github_connection() -> bool:
    """
    Verify GitHub API connection and credentials.

    Returns:
        True if connection is successful, False otherwise.
    """
    try:
        token, _ = get_github_credentials()
        session = create_github_session(token)
        
        response = session.get(f"{GITHUB_API_BASE}/user", timeout=10)
        session.close()
        
        if response.status_code == 200:
            user_data = response.json()
            logger.debug(f"GitHub connection OK, authenticated as {user_data.get('login')}")
            return True
        else:
            logger.warning(f"GitHub authentication failed: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        logger.warning(f"GitHub connection check failed: {e}")
        return False


# =============================================================================
# Email Notification Functions
# =============================================================================


class EmailNotificationError(Exception):
    """Custom exception for email notification errors."""
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message)
        self.original_error = original_error


def get_email_credentials() -> Tuple[str, int, str, str, str, str]:
    """
    Get email credentials from environment variables.
    
    Returns:
        Tuple of (smtp_host, smtp_port, smtp_user, smtp_password, email_from, email_to).
        
    Raises:
        ValueError: If any required environment variable is not set.
    """
    smtp_host = get_env_var("SMTP_HOST", required=True)
    smtp_port_str = get_env_var("SMTP_PORT", required=True)
    smtp_user = get_env_var("SMTP_USER", required=True)
    smtp_password = get_env_var("SMTP_PASSWORD", required=True)
    email_from = get_env_var("EMAIL_FROM", required=True)
    email_to = get_env_var("EMAIL_TO", required=True)
    
    # Type assertions - get_env_var with required=True raises ValueError if None
    assert smtp_host is not None
    assert smtp_port_str is not None
    assert smtp_user is not None
    assert smtp_password is not None
    assert email_from is not None
    assert email_to is not None
    
    # Parse port as integer
    try:
        smtp_port = int(smtp_port_str)
    except ValueError:
        raise ValueError(f"SMTP_PORT must be a valid integer, got: {smtp_port_str}")
    
    return smtp_host, smtp_port, smtp_user, smtp_password, email_from, email_to


def is_email_configured() -> bool:
    """
    Check if email notification is configured.
    
    Returns:
        True if all email environment variables are set, False otherwise.
    """
    required_vars = [
        "SMTP_HOST", "SMTP_PORT", "SMTP_USER", 
        "SMTP_PASSWORD", "EMAIL_FROM", "EMAIL_TO"
    ]
    
    for var in required_vars:
        value = os.environ.get(var)
        if not value or value.strip() == "":
            return False
    
    return True


def format_email_body_html(scholarships: List[Dict[str, str]]) -> str:
    """
    Format the email body as HTML with scholarship information.
    
    Args:
        scholarships: List of scholarship dictionaries with 'title' and 'url'.
        
    Returns:
        Formatted HTML string for email body.
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    
    html_lines = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        '  <meta charset="utf-8">',
        "  <style>",
        "    body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }",
        "    .header { background-color: #4a90d9; color: white; padding: 20px; border-radius: 5px 5px 0 0; }",
        "    .content { padding: 20px; background-color: #f9f9f9; }",
        "    .scholarship { background-color: white; padding: 15px; margin: 10px 0; border-radius: 5px; border-left: 4px solid #4a90d9; }",
        "    .scholarship a { color: #4a90d9; text-decoration: none; font-weight: bold; }",
        "    .scholarship a:hover { text-decoration: underline; }",
        "    .footer { padding: 15px; font-size: 12px; color: #666; text-align: center; border-top: 1px solid #ddd; }",
        "    .count { font-size: 24px; font-weight: bold; }",
        "  </style>",
        "</head>",
        "<body>",
        '  <div class="header">',
        "    <h1>🎓 New Scholarships Detected!</h1>",
        f"    <p>Detection Time: {timestamp}</p>",
        "  </div>",
        '  <div class="content">',
        f'    <p class="count">{len(scholarships)} new scholarship{"s" if len(scholarships) != 1 else ""} found!</p>',
        "    <p>The following scholarships related to Cloud, IT, and Computer Science in Norway have been detected:</p>",
    ]
    
    for i, scholarship in enumerate(scholarships, 1):
        title = scholarship.get("title", "Unknown Title")
        url = scholarship.get("url", "#")
        
        # Escape HTML special characters in title
        escaped_title = (
            title
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )
        
        html_lines.extend([
            f'    <div class="scholarship">',
            f'      <strong>{i}.</strong> <a href="{url}">{escaped_title}</a>',
            "    </div>",
        ])
    
    html_lines.extend([
        "  </div>",
        '  <div class="footer">',
        "    <p>This email was automatically sent by the Scholarship Watcher pipeline.</p>",
        "    <p>Please review each scholarship for eligibility and deadlines.</p>",
        "  </div>",
        "</body>",
        "</html>",
    ])
    
    return "\n".join(html_lines)


def format_email_body_plain(scholarships: List[Dict[str, str]]) -> str:
    """
    Format the email body as plain text with scholarship information.
    
    Args:
        scholarships: List of scholarship dictionaries with 'title' and 'url'.
        
    Returns:
        Formatted plain text string for email body.
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    
    lines = [
        "🎓 NEW SCHOLARSHIPS DETECTED!",
        "=" * 50,
        "",
        f"Detection Time: {timestamp}",
        f"Number of New Scholarships: {len(scholarships)}",
        "",
        "-" * 50,
        "",
        "SCHOLARSHIPS FOUND:",
        "",
    ]
    
    for i, scholarship in enumerate(scholarships, 1):
        title = scholarship.get("title", "Unknown Title")
        url = scholarship.get("url", "#")
        
        lines.append(f"{i}. {title}")
        lines.append(f"   URL: {url}")
        lines.append("")
    
    lines.extend([
        "-" * 50,
        "",
        "This email was automatically sent by the Scholarship Watcher pipeline.",
        "Please review each scholarship for eligibility and deadlines.",
    ])
    
    return "\n".join(lines)


def format_email_body_html_multi_country(
    scholarships_by_country: Dict[str, List[Dict[str, str]]],
    country_names: Dict[str, str]
) -> str:
    """
    Format email body as HTML with scholarships grouped by country.
    
    Args:
        scholarships_by_country: Scholarships grouped by country code.
        country_names: Mapping of country codes to names.
        
    Returns:
        Formatted HTML string for email body.
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    total_count = sum(len(v) for v in scholarships_by_country.values())
    country_count = len([c for c, s in scholarships_by_country.items() if s])
    
    html_lines = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        '  <meta charset="utf-8">',
        "  <style>",
        "    body { font-family: Arial, sans-serif; line-height: 1.6; color: #333; }",
        "    .header { background-color: #4a90d9; color: white; padding: 20px; border-radius: 5px 5px 0 0; }",
        "    .content { padding: 20px; background-color: #f9f9f9; }",
        "    .country-section { margin-bottom: 25px; }",
        "    .country-header { background-color: #2c5282; color: white; padding: 10px 15px; border-radius: 5px 5px 0 0; margin-bottom: 0; }",
        "    .country-header h3 { margin: 0; font-size: 18px; }",
        "    .scholarship { background-color: white; padding: 12px 15px; margin: 0; border-left: 4px solid #4a90d9; border-bottom: 1px solid #eee; }",
        "    .scholarship:last-child { border-radius: 0 0 5px 5px; border-bottom: none; }",
        "    .scholarship a { color: #4a90d9; text-decoration: none; font-weight: bold; }",
        "    .scholarship a:hover { text-decoration: underline; }",
        "    .footer { padding: 15px; font-size: 12px; color: #666; text-align: center; border-top: 1px solid #ddd; }",
        "    .count { font-size: 24px; font-weight: bold; }",
        "    .summary { background-color: #e8f0fe; padding: 15px; border-radius: 5px; margin-bottom: 20px; }",
        "  </style>",
        "</head>",
        "<body>",
        '  <div class="header">',
        "    <h1>🎓 New Cloud & IT Scholarships – Multi-Country Daily Update</h1>",
        f"    <p>Detection Time: {timestamp}</p>",
        "  </div>",
        '  <div class="content">',
        '    <div class="summary">',
        f'      <p class="count">{total_count} new scholarship{"s" if total_count != 1 else ""} found across {country_count} {"countries" if country_count != 1 else "country"}!</p>',
        "    </div>",
    ]
    
    # Sort countries by name
    sorted_countries = sorted(
        [(code, schols) for code, schols in scholarships_by_country.items() if schols],
        key=lambda x: country_names.get(x[0], x[0])
    )
    
    for country_code, scholarships in sorted_countries:
        country_name = country_names.get(country_code, country_code)
        flag = _get_country_flag(country_code)
        
        html_lines.extend([
            '    <div class="country-section">',
            '      <div class="country-header">',
            f'        <h3>{flag} {country_name} ({len(scholarships)})</h3>',
            "      </div>",
        ])
        
        for i, scholarship in enumerate(scholarships, 1):
            title = scholarship.get("title", "Unknown Title")
            url = scholarship.get("url", "#")
            
            escaped_title = (
                title
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
            )
            
            html_lines.extend([
                f'      <div class="scholarship">',
                f'        <strong>{i}.</strong> <a href="{url}">{escaped_title}</a>',
                "      </div>",
            ])
        
        html_lines.append("    </div>")
    
    html_lines.extend([
        "  </div>",
        '  <div class="footer">',
        "    <p>This email was automatically sent by the Scholarship Watcher pipeline.</p>",
        "    <p>Please review each scholarship for eligibility and deadlines.</p>",
        "  </div>",
        "</body>",
        "</html>",
    ])
    
    return "\n".join(html_lines)


def format_email_body_plain_multi_country(
    scholarships_by_country: Dict[str, List[Dict[str, str]]],
    country_names: Dict[str, str]
) -> str:
    """
    Format email body as plain text with scholarships grouped by country.
    
    Args:
        scholarships_by_country: Scholarships grouped by country code.
        country_names: Mapping of country codes to names.
        
    Returns:
        Formatted plain text string for email body.
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    total_count = sum(len(v) for v in scholarships_by_country.values())
    country_count = len([c for c, s in scholarships_by_country.items() if s])
    
    lines = [
        "🎓 NEW CLOUD & IT SCHOLARSHIPS – MULTI-COUNTRY DAILY UPDATE",
        "=" * 60,
        "",
        f"Detection Time: {timestamp}",
        f"Total New Scholarships: {total_count}",
        f"Countries: {country_count}",
        "",
    ]
    
    # Sort countries by name
    sorted_countries = sorted(
        [(code, schols) for code, schols in scholarships_by_country.items() if schols],
        key=lambda x: country_names.get(x[0], x[0])
    )
    
    for country_code, scholarships in sorted_countries:
        country_name = country_names.get(country_code, country_code)
        flag = _get_country_flag(country_code)
        
        lines.extend([
            "-" * 60,
            f"{flag} {country_name.upper()} ({len(scholarships)} scholarship{'s' if len(scholarships) != 1 else ''})",
            "-" * 60,
            "",
        ])
        
        for i, scholarship in enumerate(scholarships, 1):
            title = scholarship.get("title", "Unknown Title")
            url = scholarship.get("url", "#")
            
            lines.append(f"{i}. {title}")
            lines.append(f"   URL: {url}")
            lines.append("")
    
    lines.extend([
        "=" * 60,
        "",
        "This email was automatically sent by the Scholarship Watcher pipeline.",
        "Please review each scholarship for eligibility and deadlines.",
    ])
    
    return "\n".join(lines)


def send_email_notification(
    scholarships: List[Dict[str, str]],
    dry_run: bool = False
) -> bool:
    """
    Send an email notification about new scholarships via SMTP with TLS.
    
    Supports both:
    - Port 465: SMTP_SSL (implicit TLS)
    - Port 587: SMTP with STARTTLS (explicit TLS)
    
    Args:
        scholarships: List of new scholarship dictionaries.
        dry_run: If True, don't actually send the email, just log.
        
    Returns:
        True if email was sent successfully, False otherwise.
        
    Note:
        This function fails gracefully - it logs errors but does not raise
        exceptions to avoid crashing the pipeline.
    """
    if not scholarships:
        logger.info("No new scholarships to send email about")
        return True
    
    # Check if email is configured
    if not is_email_configured():
        logger.info("Email notifications not configured (missing environment variables)")
        return True  # Not an error - email is optional
    
    logger.info(f"Sending email notification about {len(scholarships)} new scholarship(s)")
    
    try:
        # Get credentials
        smtp_host, smtp_port, smtp_user, smtp_password, email_from, email_to = get_email_credentials()
        
        logger.info(f"Email config: host={smtp_host}, port={smtp_port}, from={email_from}, to={email_to}")
        
        # Format email content
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        subject = "🎓 New Cloud & IT Scholarships in Norway – Daily Update"
        
        html_body = format_email_body_html(scholarships)
        plain_body = format_email_body_plain(scholarships)
        
        if dry_run:
            logger.info(f"[DRY RUN] Would send email to: {email_to}")
            logger.info(f"[DRY RUN] Subject: {subject}")
            logger.debug(f"[DRY RUN] Plain body:\n{plain_body}")
            return True
        
        # Create email message
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = email_from
        msg["To"] = email_to
        msg["Date"] = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")
        
        # Set plain text content
        msg.set_content(plain_body)
        
        # Add HTML alternative
        msg.add_alternative(html_body, subtype="html")
        
        # Create SSL context for TLS
        ssl_context = ssl.create_default_context()
        
        # Send email - use different method based on port
        logger.info(f"Connecting to SMTP server: {smtp_host}:{smtp_port}")
        
        if smtp_port == 465:
            # Port 465 uses implicit SSL (SMTP_SSL)
            logger.debug("Using SMTP_SSL (implicit TLS) for port 465")
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30, context=ssl_context) as server:
                logger.debug("SSL connection established, authenticating...")
                server.login(smtp_user, smtp_password)
                logger.debug("Authentication successful, sending message...")
                server.send_message(msg)
        else:
            # Port 587 (and others) use STARTTLS
            logger.debug(f"Using SMTP with STARTTLS for port {smtp_port}")
            with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
                logger.debug("Connection established, starting TLS...")
                server.starttls(context=ssl_context)
                logger.debug("TLS established, authenticating...")
                server.login(smtp_user, smtp_password)
                logger.debug("Authentication successful, sending message...")
                server.send_message(msg)
        
        logger.info(f"Email notification sent successfully to {email_to}")
        return True
        
    except ValueError as e:
        logger.error(f"Email configuration error: {e}")
        return False
        
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"SMTP authentication failed: {e}")
        return False
        
    except smtplib.SMTPConnectError as e:
        logger.error(f"Failed to connect to SMTP server: {e}")
        return False
        
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error while sending email: {e}")
        return False
        
    except ssl.SSLError as e:
        logger.error(f"SSL/TLS error while sending email: {e}")
        return False
        
    except TimeoutError as e:
        logger.error(f"Timeout while sending email: {e}")
        return False
        
    except Exception as e:
        logger.error(f"Unexpected error sending email notification: {e}")
        return False


def send_email_notification_multi_country(
    scholarships_by_country: Dict[str, List[Dict[str, str]]],
    country_names: Dict[str, str],
    dry_run: bool = False
) -> bool:
    """
    Send email notification with scholarships grouped by country.
    
    Supports both:
    - Port 465: SMTP_SSL (implicit TLS)
    - Port 587: SMTP with STARTTLS (explicit TLS)
    
    Args:
        scholarships_by_country: Scholarships grouped by country code.
        country_names: Mapping of country codes to names.
        dry_run: If True, don't actually send the email, just log.
        
    Returns:
        True if email was sent successfully, False otherwise.
        
    Note:
        This function fails gracefully - it logs errors but does not raise
        exceptions to avoid crashing the pipeline.
    """
    # Filter out empty countries
    non_empty = {c: s for c, s in scholarships_by_country.items() if s}
    
    if not non_empty:
        logger.info("No new scholarships to send email about (multi-country)")
        return True
    
    # Check if email is configured
    if not is_email_configured():
        logger.info("Email notifications not configured (missing environment variables)")
        return True
    
    total_count = sum(len(v) for v in non_empty.values())
    logger.info(
        f"Sending multi-country email notification about {total_count} scholarship(s) "
        f"across {len(non_empty)} countries"
    )
    
    try:
        # Get credentials
        smtp_host, smtp_port, smtp_user, smtp_password, email_from, email_to = get_email_credentials()
        
        logger.info(f"Email config: host={smtp_host}, port={smtp_port}, from={email_from}, to={email_to}")
        
        # Format email content
        subject = "🎓 New Cloud & IT Scholarships – Multi-Country Daily Update"
        
        html_body = format_email_body_html_multi_country(non_empty, country_names)
        plain_body = format_email_body_plain_multi_country(non_empty, country_names)
        
        if dry_run:
            logger.info(f"[DRY RUN] Would send multi-country email to: {email_to}")
            logger.info(f"[DRY RUN] Subject: {subject}")
            logger.debug(f"[DRY RUN] Plain body:\n{plain_body}")
            return True
        
        # Create email message
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = email_from
        msg["To"] = email_to
        msg["Date"] = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")
        
        # Set plain text content
        msg.set_content(plain_body)
        
        # Add HTML alternative
        msg.add_alternative(html_body, subtype="html")
        
        # Create SSL context for TLS
        ssl_context = ssl.create_default_context()
        
        # Send email - use different method based on port
        logger.info(f"Connecting to SMTP server: {smtp_host}:{smtp_port}")
        
        if smtp_port == 465:
            logger.debug("Using SMTP_SSL (implicit TLS) for port 465")
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30, context=ssl_context) as server:
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
        else:
            logger.debug(f"Using SMTP with STARTTLS for port {smtp_port}")
            with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
                server.starttls(context=ssl_context)
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
        
        logger.info(f"Multi-country email notification sent successfully to {email_to}")
        return True
        
    except ValueError as e:
        logger.error(f"Email configuration error: {e}")
        return False
        
    except smtplib.SMTPAuthenticationError as e:
        logger.error(f"SMTP authentication failed: {e}")
        return False
        
    except smtplib.SMTPConnectError as e:
        logger.error(f"Failed to connect to SMTP server: {e}")
        return False
        
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error while sending email: {e}")
        return False
        
    except ssl.SSLError as e:
        logger.error(f"SSL/TLS error while sending email: {e}")
        return False
        
    except TimeoutError as e:
        logger.error(f"Timeout while sending email: {e}")
        return False
        
    except Exception as e:
        logger.error(f"Unexpected error sending multi-country email: {e}")
        return False


def check_email_connection() -> bool:
    """
    Verify SMTP connection and credentials.
    
    Returns:
        True if connection is successful, False otherwise.
    """
    if not is_email_configured():
        logger.debug("Email not configured, skipping connection check")
        return False
    
    try:
        smtp_host, smtp_port, smtp_user, smtp_password, _, _ = get_email_credentials()
        
        ssl_context = ssl.create_default_context()
        
        logger.debug(f"Testing email connection to {smtp_host}:{smtp_port}")
        
        if smtp_port == 465:
            # Port 465 uses implicit SSL
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=10, context=ssl_context) as server:
                server.login(smtp_user, smtp_password)
        else:
            # Port 587 and others use STARTTLS
            with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
                server.starttls(context=ssl_context)
                server.login(smtp_user, smtp_password)
        
        logger.debug(f"Email connection OK, authenticated with {smtp_user}")
        return True
        
    except Exception as e:
        logger.warning(f"Email connection check failed: {e}")
        return False

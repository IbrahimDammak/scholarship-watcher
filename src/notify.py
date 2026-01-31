"""
Notify module for the Scholarship Watcher pipeline.

This module handles creating GitHub Issues when new scholarships are detected.
Uses the GitHub REST API with proper error handling for rate limits and
validation errors.
"""

import json
import os
import time
from datetime import datetime
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

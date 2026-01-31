#!/usr/bin/env python3
"""
Main orchestration module for the Scholarship Watcher pipeline.

This module coordinates the complete pipeline:
fetch → parse → filter → compare → notify

It handles environment validation, logging setup, and error handling
for the entire workflow.
"""

import sys
import os
from typing import List, Dict, Optional

from src.utils import setup_logging, get_logger, get_env_var
from src.fetch import fetch_scholarship_pages, get_successful_fetches, DEFAULT_SCHOLARSHIP_URLS
from src.parse import parse_fetch_results
from src.filter import filter_scholarships
from src.compare import compare_and_update, DEFAULT_RESULTS_PATH
from src.notify import (
    notify_new_scholarships,
    check_github_connection,
    GitHubAPIError,
    send_email_notification,
    check_email_connection,
    is_email_configured
)


# Exit codes
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_ENV_ERROR = 2


def validate_environment() -> bool:
    """
    Validate that required environment variables are set.

    Checks for GITHUB_TOKEN and GITHUB_REPOSITORY which are
    required for the notification functionality.

    Returns:
        True if all required variables are set, False otherwise.
    """
    logger = get_logger("main")
    
    required_vars = ["GITHUB_TOKEN", "GITHUB_REPOSITORY"]
    missing_vars = []
    
    for var in required_vars:
        value = os.environ.get(var)
        if not value or value.strip() == "":
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    logger.debug("Environment validation passed")
    return True


def get_scholarship_urls() -> List[str]:
    """
    Get the list of scholarship URLs to fetch.

    First checks for SCHOLARSHIP_URLS environment variable,
    falls back to default URLs if not set.

    Returns:
        List of URLs to fetch.
    """
    logger = get_logger("main")
    
    # Check for custom URLs from environment
    custom_urls = os.environ.get("SCHOLARSHIP_URLS", "")
    
    if custom_urls.strip():
        # Parse comma-separated URLs
        urls = [url.strip() for url in custom_urls.split(",") if url.strip()]
        if urls:
            logger.info(f"Using {len(urls)} custom URL(s) from environment")
            return urls
    
    logger.info(f"Using {len(DEFAULT_SCHOLARSHIP_URLS)} default URL(s)")
    return DEFAULT_SCHOLARSHIP_URLS


def get_results_filepath() -> str:
    """
    Get the filepath for storing results.

    Checks for DATA_PATH environment variable,
    falls back to default path if not set.

    Returns:
        Path to the results JSON file.
    """
    custom_path = os.environ.get("DATA_PATH", "")
    
    if custom_path.strip():
        return custom_path.strip()
    
    return DEFAULT_RESULTS_PATH


def run_pipeline(dry_run: bool = False) -> int:
    """
    Execute the complete scholarship watcher pipeline.

    Pipeline stages:
    1. Validate environment
    2. Fetch scholarship pages
    3. Parse HTML content
    4. Filter for relevant scholarships
    5. Compare with previous results
    6. Notify about new scholarships

    Args:
        dry_run: If True, skip actual notification (for testing).

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    logger = get_logger("main")
    
    logger.info("=" * 60)
    logger.info("Scholarship Watcher Pipeline - Starting")
    logger.info("=" * 60)
    
    # Stage 1: Validate environment
    logger.info("[Stage 1/6] Validating environment...")
    if not validate_environment():
        logger.error("Environment validation failed")
        return EXIT_ENV_ERROR
    
    # Verify GitHub connection
    if not dry_run:
        logger.info("Verifying GitHub connection...")
        if not check_github_connection():
            logger.warning("GitHub connection check failed, notifications may fail")
        
        # Check email connection if configured
        if is_email_configured():
            logger.info("Verifying email connection...")
            if not check_email_connection():
                logger.warning("Email connection check failed, email notifications may fail")
    
    # Stage 2: Fetch scholarship pages
    logger.info("[Stage 2/6] Fetching scholarship pages...")
    urls = get_scholarship_urls()
    
    fetch_results = fetch_scholarship_pages(urls)
    successful_fetches = get_successful_fetches(fetch_results)
    
    if not successful_fetches:
        logger.error("No scholarship pages could be fetched")
        # This is not necessarily fatal - might be temporary network issues
        # Continue to compare phase in case we have previous data
        logger.warning("Continuing with empty fetch results")
    
    logger.info(f"Successfully fetched {len(successful_fetches)}/{len(fetch_results)} page(s)")
    
    # Stage 3: Parse HTML content
    logger.info("[Stage 3/6] Parsing scholarship information...")
    parsed_scholarships = parse_fetch_results(fetch_results)
    
    if not parsed_scholarships:
        logger.warning("No scholarships parsed from fetched pages")
    
    logger.info(f"Parsed {len(parsed_scholarships)} scholarship(s)")
    
    # Stage 4: Filter for relevant scholarships
    logger.info("[Stage 4/6] Filtering scholarships...")
    
    # Use flexible filtering - require either Norway OR tech relevance
    # since some sources are already Norway-focused
    filtered_scholarships = filter_scholarships(
        parsed_scholarships,
        require_norway=True,
        require_tech=True,
        exclude_false_positives=True
    )
    
    # If strict filtering yields no results, try flexible filtering
    if not filtered_scholarships and parsed_scholarships:
        logger.info("Strict filtering found no results, trying flexible filter...")
        from src.filter import filter_scholarships_flexible
        filtered_scholarships = filter_scholarships_flexible(
            parsed_scholarships,
            require_both=False  # Norway OR tech
        )
    
    logger.info(f"Filtered to {len(filtered_scholarships)} relevant scholarship(s)")
    
    # Stage 5: Compare with previous results
    logger.info("[Stage 5/6] Comparing with previous results...")
    results_path = get_results_filepath()
    
    new_scholarships, all_scholarships = compare_and_update(
        filtered_scholarships,
        results_filepath=results_path,
        save_updated=True
    )
    
    logger.info(f"Found {len(new_scholarships)} new scholarship(s)")
    
    # Stage 6: Notify about new scholarships
    logger.info("[Stage 6/6] Creating notifications...")
    
    if new_scholarships:
        try:
            issue_data = notify_new_scholarships(
                new_scholarships,
                labels=["scholarship", "automated", "norway", "tech"],
                dry_run=dry_run
            )
            
            if issue_data:
                logger.info(f"Created GitHub Issue: {issue_data.get('html_url', 'N/A')}")
            elif dry_run:
                logger.info("[DRY RUN] Notification would have been sent")
                
        except GitHubAPIError as e:
            logger.error(f"Failed to create notification: {e}")
            # Don't fail the pipeline for notification errors
            # The data has been saved, so next run will have correct state
            logger.warning("Pipeline data saved, notification will be retried on next run")
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            return EXIT_ENV_ERROR
        
        # Send email notification (if configured)
        # Email fails gracefully - won't crash pipeline
        if is_email_configured():
            logger.info("Sending email notification...")
            email_sent = send_email_notification(
                new_scholarships,
                dry_run=dry_run
            )
            if email_sent:
                logger.info("Email notification sent successfully")
            else:
                logger.warning("Email notification failed (see logs above)")
        else:
            logger.debug("Email notifications not configured, skipping")
    else:
        logger.info("No new scholarships to notify about")
    
    # Pipeline complete
    logger.info("=" * 60)
    logger.info("Scholarship Watcher Pipeline - Complete")
    logger.info(f"Summary: {len(all_scholarships)} total, {len(new_scholarships)} new")
    logger.info("=" * 60)
    
    return EXIT_SUCCESS


def main() -> int:
    """
    Main entry point for the Scholarship Watcher pipeline.

    Sets up logging and runs the pipeline with proper error handling.

    Returns:
        Exit code for the process.
    """
    # Determine log level from environment
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    
    # Set up logging
    setup_logging(log_level)
    logger = get_logger("main")
    
    # Check for dry run mode
    dry_run = os.environ.get("DRY_RUN", "").lower() in ("true", "1", "yes")
    
    if dry_run:
        logger.info("Running in DRY RUN mode - notifications will be skipped")
    
    try:
        exit_code = run_pipeline(dry_run=dry_run)
        return exit_code
        
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        return EXIT_FAILURE
        
    except Exception as e:
        logger.exception(f"Unexpected error in pipeline: {e}")
        return EXIT_FAILURE


if __name__ == "__main__":
    sys.exit(main())

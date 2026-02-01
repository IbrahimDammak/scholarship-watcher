#!/usr/bin/env python3
"""
Main orchestration module for the Scholarship Watcher pipeline.

This module coordinates the complete pipeline:
fetch → parse → filter → compare → notify

Supports both single-country (legacy) and multi-country modes.
Multi-country mode is enabled automatically when countries are configured.

It handles environment validation, logging setup, and error handling
for the entire workflow.
"""

import sys
import os
from typing import List, Dict, Optional

from src.utils import (
    setup_logging,
    get_logger,
    get_env_var,
    load_countries_config,
    validate_countries_config,
    CountryConfig
)
from src.fetch import fetch_scholarship_pages, get_successful_fetches, DEFAULT_SCHOLARSHIP_URLS
from src.parse import parse_fetch_results
from src.filter import (
    filter_scholarships,
    filter_scholarships_multi_country,
    get_all_filtered_scholarships
)
from src.compare import (
    compare_and_update,
    compare_and_update_multi_country,
    DEFAULT_RESULTS_PATH
)
from src.notify import (
    notify_new_scholarships,
    notify_new_scholarships_multi_country,
    check_github_connection,
    GitHubAPIError,
    send_email_notification,
    send_email_notification_multi_country,
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


def is_multi_country_mode() -> bool:
    """
    Check if multi-country mode should be used.
    
    Multi-country mode is enabled when:
    - MULTI_COUNTRY_MODE environment variable is set to "true"
    - OR countries configuration file exists with multiple enabled countries
    
    Returns:
        True if multi-country mode should be used.
    """
    # Check explicit environment variable
    env_mode = os.environ.get("MULTI_COUNTRY_MODE", "").lower()
    if env_mode in ("true", "1", "yes"):
        return True
    if env_mode in ("false", "0", "no"):
        return False
    
    # Auto-detect based on configuration
    try:
        countries = load_countries_config(enabled_only=True)
        # If more than one country is configured, use multi-country mode
        return len(countries) > 1
    except Exception:
        return False


def run_pipeline(dry_run: bool = False) -> int:
    """
    Execute the complete scholarship watcher pipeline.

    Pipeline stages:
    1. Validate environment and load configuration
    2. Fetch scholarship pages
    3. Parse HTML content
    4. Filter for relevant scholarships (single or multi-country)
    5. Compare with previous results
    6. Notify about new scholarships

    Automatically uses multi-country mode when configured.

    Args:
        dry_run: If True, skip actual notification (for testing).

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    logger = get_logger("main")
    
    logger.info("=" * 60)
    logger.info("Scholarship Watcher Pipeline - Starting")
    logger.info("=" * 60)
    
    # Stage 1: Validate environment and load configuration
    logger.info("[Stage 1/6] Validating environment...")
    if not validate_environment():
        logger.error("Environment validation failed")
        return EXIT_ENV_ERROR
    
    # Load country configuration
    countries = load_countries_config(enabled_only=True)
    multi_country = is_multi_country_mode()
    
    # Build country name mapping for notifications
    country_names = {c.code: c.name for c in countries}
    
    if multi_country:
        logger.info(f"Multi-country mode enabled with {len(countries)} countries: {list(country_names.keys())}")
    else:
        logger.info(f"Single-country mode (primary: {countries[0].name if countries else 'Norway'})")
    
    # Validate country configuration
    config_warnings = validate_countries_config(countries)
    for warning in config_warnings:
        logger.warning(f"Country config warning: {warning}")
    
    # Verify connections
    if not dry_run:
        logger.info("Verifying GitHub connection...")
        if not check_github_connection():
            logger.warning("GitHub connection check failed, notifications may fail")
        
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
    
    results_path = get_results_filepath()
    
    if multi_country:
        # Multi-country filtering and comparison
        return _run_multi_country_pipeline(
            parsed_scholarships=parsed_scholarships,
            countries=countries,
            country_names=country_names,
            results_path=results_path,
            dry_run=dry_run,
            logger=logger
        )
    else:
        # Single-country (legacy) filtering and comparison
        return _run_single_country_pipeline(
            parsed_scholarships=parsed_scholarships,
            results_path=results_path,
            dry_run=dry_run,
            logger=logger
        )


def _run_single_country_pipeline(
    parsed_scholarships: List[Dict[str, str]],
    results_path: str,
    dry_run: bool,
    logger
) -> int:
    """Run the single-country (legacy) pipeline."""
    from src.filter import filter_scholarships_flexible
    
    # Use flexible filtering - require either Norway OR tech relevance
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


def _run_multi_country_pipeline(
    parsed_scholarships: List[Dict[str, str]],
    countries: List[CountryConfig],
    country_names: Dict[str, str],
    results_path: str,
    dry_run: bool,
    logger
) -> int:
    """
    Run the multi-country pipeline.
    
    Filters scholarships by country, tracks results per country,
    and sends grouped notifications.
    
    Args:
        parsed_scholarships: List of parsed scholarship dictionaries.
        countries: List of enabled CountryConfig objects.
        country_names: Mapping of country codes to names.
        results_path: Path to the results JSON file.
        dry_run: If True, skip actual notifications.
        logger: Logger instance.
    
    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    # Filter scholarships by country
    scholarships_by_country = filter_scholarships_multi_country(
        parsed_scholarships,
        countries
    )
    
    # Get all filtered scholarships (union across countries)
    all_filtered = get_all_filtered_scholarships(scholarships_by_country)
    
    # Log filtering results per country
    for country_code, scholarships in scholarships_by_country.items():
        country_name = country_names.get(country_code, country_code)
        logger.info(f"  {country_name}: {len(scholarships)} scholarship(s)")
    
    logger.info(f"Filtered to {len(all_filtered)} unique relevant scholarship(s) across {len(scholarships_by_country)} countries")
    
    # Stage 5: Compare with previous results (per country)
    logger.info("[Stage 5/6] Comparing with previous results (per country)...")
    
    new_by_country, all_by_country = compare_and_update_multi_country(
        scholarships_by_country,
        results_filepath=results_path,
        save_updated=True
    )
    
    # Log comparison results
    total_new = sum(len(s) for s in new_by_country.values())
    total_all = sum(len(s) for s in all_by_country.values())
    
    logger.info(f"Comparison summary:")
    for country_code, scholarships in new_by_country.items():
        if scholarships:
            country_name = country_names.get(country_code, country_code)
            logger.info(f"  {country_name}: {len(scholarships)} new scholarship(s)")
    
    logger.info(f"Total: {total_new} new scholarship(s) across all countries")
    
    # Stage 6: Notify about new scholarships (grouped by country)
    logger.info("[Stage 6/6] Creating notifications...")
    
    if total_new > 0:
        try:
            # Create GitHub Issue with grouped scholarships
            issue_data = notify_new_scholarships_multi_country(
                new_by_country,
                country_names=country_names,
                labels=["scholarship", "automated", "multi-country"],
                dry_run=dry_run
            )
            
            if issue_data:
                logger.info(f"Created GitHub Issue: {issue_data.get('html_url', 'N/A')}")
            elif dry_run:
                logger.info("[DRY RUN] GitHub Issue notification would have been sent")
                
        except GitHubAPIError as e:
            logger.error(f"Failed to create GitHub notification: {e}")
            logger.warning("Pipeline data saved, notification will be retried on next run")
        except ValueError as e:
            logger.error(f"Configuration error: {e}")
            return EXIT_ENV_ERROR
        
        # Send email notification (if configured)
        if is_email_configured():
            logger.info("Sending email notification...")
            email_sent = send_email_notification_multi_country(
                new_by_country,
                country_names=country_names,
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
    logger.info("Scholarship Watcher Pipeline - Complete (Multi-Country)")
    logger.info(f"Summary: {total_all} total, {total_new} new across {len(countries)} countries")
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

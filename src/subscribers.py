"""
Subscriber management module for the Scholarship Watcher pipeline.

This module handles:
- Loading subscriber configuration
- Validating subscriber data
- Grouping subscribers by country
- Managing subscription state
"""

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Set, Any

from src.utils import get_logger


logger = get_logger("subscribers")


DEFAULT_SUBSCRIBERS_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "data",
    "subscribers.json"
)


@dataclass
class Subscriber:
    """Represents a subscription to scholarship alerts."""
    email: str
    countries: List[str]
    created_at: str
    active: bool = True
    
    def __post_init__(self):
        """Normalize data after initialization."""
        self.email = self.email.lower().strip()
        self.countries = [c.upper().strip() for c in self.countries]


def load_subscribers(
    filepath: str = DEFAULT_SUBSCRIBERS_PATH,
    active_only: bool = True
) -> List[Subscriber]:
    """
    Load subscribers from configuration file.
    
    Args:
        filepath: Path to subscribers JSON file.
        active_only: If True, only return active subscribers.
        
    Returns:
        List of Subscriber objects.
    """
    if not os.path.exists(filepath):
        logger.info(f"Subscribers file not found: {filepath}")
        return []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in subscribers file: {e}")
        return []
    except IOError as e:
        logger.error(f"Failed to read subscribers file: {e}")
        return []
    
    subscribers_data = data.get('subscribers', [])
    
    if not isinstance(subscribers_data, list):
        logger.error("Invalid subscribers data format: expected list")
        return []
    
    subscribers = []
    
    for entry in subscribers_data:
        try:
            subscriber = _parse_subscriber_entry(entry)
            if subscriber and (not active_only or subscriber.active):
                subscribers.append(subscriber)
        except Exception as e:
            logger.warning(f"Failed to parse subscriber entry: {e}")
            continue
    
    logger.info(f"Loaded {len(subscribers)} subscriber(s)")
    return subscribers


def _parse_subscriber_entry(entry: Dict[str, Any]) -> Optional[Subscriber]:
    """
    Parse a subscriber entry from configuration.
    
    Args:
        entry: Dictionary with subscriber data.
        
    Returns:
        Subscriber object or None if invalid.
    """
    if not isinstance(entry, dict):
        return None
    
    email = entry.get('email', '').strip()
    
    if not email or not _is_valid_email(email):
        logger.warning(f"Invalid or missing email in subscriber entry")
        return None
    
    countries = entry.get('countries', [])
    
    if isinstance(countries, str):
        try:
            countries = json.loads(countries)
        except json.JSONDecodeError:
            countries = [c.strip() for c in countries.split(',') if c.strip()]
    
    if not countries:
        logger.warning(f"No countries specified for subscriber: {email}")
        return None
    
    created_at = entry.get('created_at', datetime.utcnow().isoformat())
    active = entry.get('active', True)
    
    return Subscriber(
        email=email,
        countries=countries,
        created_at=created_at,
        active=active
    )


def _is_valid_email(email: str) -> bool:
    """
    Basic email validation.
    
    Args:
        email: Email address to validate.
        
    Returns:
        True if email appears valid.
    """
    if not email or '@' not in email:
        return False
    
    parts = email.split('@')
    if len(parts) != 2:
        return False
    
    local, domain = parts
    if not local or not domain or '.' not in domain:
        return False
    
    return True


def group_subscribers_by_country(
    subscribers: List[Subscriber]
) -> Dict[str, List[Subscriber]]:
    """
    Group subscribers by their subscribed countries.
    
    Args:
        subscribers: List of Subscriber objects.
        
    Returns:
        Dictionary mapping country codes to subscriber lists.
    """
    by_country: Dict[str, List[Subscriber]] = {}
    
    for subscriber in subscribers:
        for country_code in subscriber.countries:
            if country_code not in by_country:
                by_country[country_code] = []
            by_country[country_code].append(subscriber)
    
    return by_country


def get_subscribers_for_countries(
    subscribers: List[Subscriber],
    country_codes: List[str]
) -> List[Subscriber]:
    """
    Get subscribers who are interested in any of the given countries.
    
    Args:
        subscribers: List of all subscribers.
        country_codes: List of country codes to match.
        
    Returns:
        List of subscribers interested in at least one of the countries.
    """
    country_set = set(c.upper() for c in country_codes)
    
    matching = []
    for subscriber in subscribers:
        subscriber_countries = set(subscriber.countries)
        if subscriber_countries & country_set:
            matching.append(subscriber)
    
    return matching


def get_countries_for_subscriber(
    subscriber: Subscriber,
    available_countries: List[str]
) -> List[str]:
    """
    Get the intersection of subscriber's countries and available countries.
    
    Args:
        subscriber: Subscriber object.
        available_countries: List of country codes with new scholarships.
        
    Returns:
        List of country codes the subscriber cares about.
    """
    subscriber_set = set(subscriber.countries)
    available_set = set(c.upper() for c in available_countries)
    
    return list(subscriber_set & available_set)


def save_subscribers(
    subscribers: List[Subscriber],
    filepath: str = DEFAULT_SUBSCRIBERS_PATH
) -> bool:
    """
    Save subscribers to configuration file.
    
    Args:
        subscribers: List of Subscriber objects.
        filepath: Path to save file.
        
    Returns:
        True if saved successfully.
    """
    data = {
        'subscribers': [
            {
                'email': s.email,
                'countries': s.countries,
                'created_at': s.created_at,
                'active': s.active
            }
            for s in subscribers
        ],
        'last_updated': datetime.utcnow().isoformat() + 'Z',
        'version': '1.0'
    }
    
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(subscribers)} subscriber(s) to {filepath}")
        return True
        
    except IOError as e:
        logger.error(f"Failed to save subscribers: {e}")
        return False


def add_subscriber(
    email: str,
    countries: List[str],
    filepath: str = DEFAULT_SUBSCRIBERS_PATH
) -> bool:
    """
    Add a new subscriber or update existing one.
    
    Args:
        email: Subscriber email.
        countries: List of country codes.
        filepath: Path to subscribers file.
        
    Returns:
        True if added/updated successfully.
    """
    subscribers = load_subscribers(filepath, active_only=False)
    
    email_lower = email.lower().strip()
    existing = next((s for s in subscribers if s.email == email_lower), None)
    
    if existing:
        existing.countries = list(set(existing.countries) | set(countries))
        existing.active = True
        logger.info(f"Updated subscriber: {email_lower}")
    else:
        new_subscriber = Subscriber(
            email=email_lower,
            countries=countries,
            created_at=datetime.utcnow().isoformat() + 'Z',
            active=True
        )
        subscribers.append(new_subscriber)
        logger.info(f"Added new subscriber: {email_lower}")
    
    return save_subscribers(subscribers, filepath)


def remove_subscriber(
    email: str,
    filepath: str = DEFAULT_SUBSCRIBERS_PATH,
    hard_delete: bool = False
) -> bool:
    """
    Remove or deactivate a subscriber.
    
    Args:
        email: Subscriber email.
        filepath: Path to subscribers file.
        hard_delete: If True, completely remove. If False, just deactivate.
        
    Returns:
        True if removed/deactivated successfully.
    """
    subscribers = load_subscribers(filepath, active_only=False)
    
    email_lower = email.lower().strip()
    
    if hard_delete:
        subscribers = [s for s in subscribers if s.email != email_lower]
    else:
        for s in subscribers:
            if s.email == email_lower:
                s.active = False
    
    return save_subscribers(subscribers, filepath)


def validate_subscribers(subscribers: List[Subscriber]) -> List[str]:
    """
    Validate subscriber list and return warnings.
    
    Args:
        subscribers: List of subscribers to validate.
        
    Returns:
        List of warning messages.
    """
    warnings = []
    emails_seen: Set[str] = set()
    
    for subscriber in subscribers:
        if subscriber.email in emails_seen:
            warnings.append(f"Duplicate email: {subscriber.email}")
        emails_seen.add(subscriber.email)
        
        if not subscriber.countries:
            warnings.append(f"No countries for: {subscriber.email}")
        
        for country in subscriber.countries:
            if len(country) != 2:
                warnings.append(f"Invalid country code '{country}' for: {subscriber.email}")
    
    return warnings


def get_subscriber_summary(subscribers: List[Subscriber]) -> Dict[str, Any]:
    """
    Get summary statistics for subscribers.
    
    Args:
        subscribers: List of subscribers.
        
    Returns:
        Dictionary with summary statistics.
    """
    by_country = group_subscribers_by_country(subscribers)
    
    return {
        'total_subscribers': len(subscribers),
        'active_subscribers': len([s for s in subscribers if s.active]),
        'countries_with_subscribers': len(by_country),
        'subscribers_per_country': {
            code: len(subs) for code, subs in by_country.items()
        }
    }

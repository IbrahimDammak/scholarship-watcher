"""
Tests for the subscribers module.

Tests cover:
- Loading subscribers from file
- Validating subscriber data
- Grouping subscribers by country
- Adding and removing subscribers
"""

import json
import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock

from src.subscribers import (
    Subscriber,
    load_subscribers,
    save_subscribers,
    add_subscriber,
    remove_subscriber,
    group_subscribers_by_country,
    get_subscribers_for_countries,
    get_countries_for_subscriber,
    validate_subscribers,
    get_subscriber_summary,
    _is_valid_email,
    _parse_subscriber_entry,
)


class TestSubscriber:
    """Tests for Subscriber dataclass."""

    def test_create_subscriber(self):
        """Test creating a subscriber."""
        sub = Subscriber(
            email="Test@Example.com",
            countries=["no", "se"],
            created_at="2026-01-01T00:00:00Z"
        )
        
        assert sub.email == "test@example.com"
        assert sub.countries == ["NO", "SE"]
        assert sub.active is True

    def test_subscriber_normalizes_email(self):
        """Test that email is normalized to lowercase."""
        sub = Subscriber(
            email="  USER@DOMAIN.COM  ",
            countries=["de"],
            created_at="2026-01-01T00:00:00Z"
        )
        
        assert sub.email == "user@domain.com"

    def test_subscriber_normalizes_countries(self):
        """Test that country codes are normalized to uppercase."""
        sub = Subscriber(
            email="user@example.com",
            countries=["no", "Se", "DE"],
            created_at="2026-01-01T00:00:00Z"
        )
        
        assert sub.countries == ["NO", "SE", "DE"]


class TestIsValidEmail:
    """Tests for email validation."""

    def test_valid_email(self):
        """Test valid email addresses."""
        assert _is_valid_email("user@example.com") is True
        assert _is_valid_email("user.name@domain.co.uk") is True
        assert _is_valid_email("user+tag@example.org") is True

    def test_invalid_email(self):
        """Test invalid email addresses."""
        assert _is_valid_email("") is False
        assert _is_valid_email("not-an-email") is False
        assert _is_valid_email("@domain.com") is False
        assert _is_valid_email("user@") is False
        assert _is_valid_email("user@domain") is False
        assert _is_valid_email(None) is False


class TestParseSubscriberEntry:
    """Tests for parsing subscriber entries."""

    def test_parse_valid_entry(self):
        """Test parsing a valid entry."""
        entry = {
            "email": "user@example.com",
            "countries": ["NO", "SE"],
            "created_at": "2026-01-01T00:00:00Z",
            "active": True
        }
        
        sub = _parse_subscriber_entry(entry)
        
        assert sub is not None
        assert sub.email == "user@example.com"
        assert sub.countries == ["NO", "SE"]

    def test_parse_entry_with_string_countries(self):
        """Test parsing entry with countries as JSON string."""
        entry = {
            "email": "user@example.com",
            "countries": '["NO", "SE"]',
            "created_at": "2026-01-01T00:00:00Z"
        }
        
        sub = _parse_subscriber_entry(entry)
        
        assert sub is not None
        assert sub.countries == ["NO", "SE"]

    def test_parse_entry_with_comma_separated_countries(self):
        """Test parsing entry with comma-separated countries."""
        entry = {
            "email": "user@example.com",
            "countries": "NO, SE, DE",
            "created_at": "2026-01-01T00:00:00Z"
        }
        
        sub = _parse_subscriber_entry(entry)
        
        assert sub is not None
        assert sub.countries == ["NO", "SE", "DE"]

    def test_parse_entry_invalid_email(self):
        """Test parsing entry with invalid email."""
        entry = {
            "email": "not-valid",
            "countries": ["NO"]
        }
        
        sub = _parse_subscriber_entry(entry)
        
        assert sub is None

    def test_parse_entry_no_countries(self):
        """Test parsing entry with no countries."""
        entry = {
            "email": "user@example.com",
            "countries": []
        }
        
        sub = _parse_subscriber_entry(entry)
        
        assert sub is None


class TestLoadSubscribers:
    """Tests for loading subscribers from file."""

    def test_load_from_file(self, tmp_path):
        """Test loading subscribers from JSON file."""
        file_path = tmp_path / "subscribers.json"
        data = {
            "subscribers": [
                {
                    "email": "user1@example.com",
                    "countries": ["NO", "SE"],
                    "created_at": "2026-01-01T00:00:00Z",
                    "active": True
                },
                {
                    "email": "user2@example.com",
                    "countries": ["DE"],
                    "created_at": "2026-01-01T00:00:00Z",
                    "active": True
                }
            ]
        }
        file_path.write_text(json.dumps(data))
        
        subscribers = load_subscribers(str(file_path))
        
        assert len(subscribers) == 2
        assert subscribers[0].email == "user1@example.com"

    def test_load_nonexistent_file(self, tmp_path):
        """Test loading from nonexistent file."""
        file_path = tmp_path / "nonexistent.json"
        
        subscribers = load_subscribers(str(file_path))
        
        assert subscribers == []

    def test_load_active_only(self, tmp_path):
        """Test loading only active subscribers."""
        file_path = tmp_path / "subscribers.json"
        data = {
            "subscribers": [
                {
                    "email": "active@example.com",
                    "countries": ["NO"],
                    "created_at": "2026-01-01T00:00:00Z",
                    "active": True
                },
                {
                    "email": "inactive@example.com",
                    "countries": ["SE"],
                    "created_at": "2026-01-01T00:00:00Z",
                    "active": False
                }
            ]
        }
        file_path.write_text(json.dumps(data))
        
        active_subs = load_subscribers(str(file_path), active_only=True)
        all_subs = load_subscribers(str(file_path), active_only=False)
        
        assert len(active_subs) == 1
        assert len(all_subs) == 2


class TestSaveSubscribers:
    """Tests for saving subscribers to file."""

    def test_save_to_file(self, tmp_path):
        """Test saving subscribers to JSON file."""
        file_path = tmp_path / "subscribers.json"
        subscribers = [
            Subscriber(
                email="user@example.com",
                countries=["NO", "SE"],
                created_at="2026-01-01T00:00:00Z"
            )
        ]
        
        result = save_subscribers(subscribers, str(file_path))
        
        assert result is True
        assert file_path.exists()
        
        with open(file_path) as f:
            data = json.load(f)
        
        assert len(data["subscribers"]) == 1
        assert data["subscribers"][0]["email"] == "user@example.com"


class TestAddSubscriber:
    """Tests for adding subscribers."""

    def test_add_new_subscriber(self, tmp_path):
        """Test adding a new subscriber."""
        file_path = tmp_path / "subscribers.json"
        file_path.write_text(json.dumps({"subscribers": []}))
        
        result = add_subscriber(
            email="new@example.com",
            countries=["NO", "SE"],
            filepath=str(file_path)
        )
        
        assert result is True
        
        subscribers = load_subscribers(str(file_path))
        assert len(subscribers) == 1
        assert subscribers[0].email == "new@example.com"

    def test_add_updates_existing(self, tmp_path):
        """Test that adding existing email updates countries."""
        file_path = tmp_path / "subscribers.json"
        data = {
            "subscribers": [{
                "email": "user@example.com",
                "countries": ["NO"],
                "created_at": "2026-01-01T00:00:00Z",
                "active": True
            }]
        }
        file_path.write_text(json.dumps(data))
        
        result = add_subscriber(
            email="user@example.com",
            countries=["SE", "DE"],
            filepath=str(file_path)
        )
        
        assert result is True
        
        subscribers = load_subscribers(str(file_path))
        assert len(subscribers) == 1
        assert set(subscribers[0].countries) == {"NO", "SE", "DE"}


class TestRemoveSubscriber:
    """Tests for removing subscribers."""

    def test_deactivate_subscriber(self, tmp_path):
        """Test deactivating a subscriber (soft delete)."""
        file_path = tmp_path / "subscribers.json"
        data = {
            "subscribers": [{
                "email": "user@example.com",
                "countries": ["NO"],
                "created_at": "2026-01-01T00:00:00Z",
                "active": True
            }]
        }
        file_path.write_text(json.dumps(data))
        
        result = remove_subscriber(
            email="user@example.com",
            filepath=str(file_path),
            hard_delete=False
        )
        
        assert result is True
        
        # Active only should return empty
        active_subs = load_subscribers(str(file_path), active_only=True)
        assert len(active_subs) == 0
        
        # All should return the deactivated one
        all_subs = load_subscribers(str(file_path), active_only=False)
        assert len(all_subs) == 1
        assert all_subs[0].active is False

    def test_hard_delete_subscriber(self, tmp_path):
        """Test completely removing a subscriber."""
        file_path = tmp_path / "subscribers.json"
        data = {
            "subscribers": [{
                "email": "user@example.com",
                "countries": ["NO"],
                "created_at": "2026-01-01T00:00:00Z",
                "active": True
            }]
        }
        file_path.write_text(json.dumps(data))
        
        result = remove_subscriber(
            email="user@example.com",
            filepath=str(file_path),
            hard_delete=True
        )
        
        assert result is True
        
        all_subs = load_subscribers(str(file_path), active_only=False)
        assert len(all_subs) == 0


class TestGroupSubscribersByCountry:
    """Tests for grouping subscribers by country."""

    def test_group_by_country(self):
        """Test grouping subscribers by their countries."""
        subscribers = [
            Subscriber("user1@example.com", ["NO", "SE"], "2026-01-01T00:00:00Z"),
            Subscriber("user2@example.com", ["NO", "DE"], "2026-01-01T00:00:00Z"),
            Subscriber("user3@example.com", ["SE"], "2026-01-01T00:00:00Z"),
        ]
        
        by_country = group_subscribers_by_country(subscribers)
        
        assert len(by_country["NO"]) == 2
        assert len(by_country["SE"]) == 2
        assert len(by_country["DE"]) == 1


class TestGetSubscribersForCountries:
    """Tests for filtering subscribers by countries."""

    def test_get_subscribers_for_countries(self):
        """Test getting subscribers interested in specific countries."""
        subscribers = [
            Subscriber("user1@example.com", ["NO", "SE"], "2026-01-01T00:00:00Z"),
            Subscriber("user2@example.com", ["DE"], "2026-01-01T00:00:00Z"),
            Subscriber("user3@example.com", ["FR"], "2026-01-01T00:00:00Z"),
        ]
        
        matching = get_subscribers_for_countries(subscribers, ["NO", "DE"])
        
        assert len(matching) == 2
        emails = [s.email for s in matching]
        assert "user1@example.com" in emails
        assert "user2@example.com" in emails


class TestGetCountriesForSubscriber:
    """Tests for getting relevant countries for a subscriber."""

    def test_get_intersection(self):
        """Test getting intersection of subscriber and available countries."""
        subscriber = Subscriber("user@example.com", ["NO", "SE", "DE"], "2026-01-01T00:00:00Z")
        
        result = get_countries_for_subscriber(subscriber, ["NO", "FR", "IT"])
        
        assert result == ["NO"]


class TestValidateSubscribers:
    """Tests for subscriber validation."""

    def test_valid_subscribers(self):
        """Test validation passes for valid subscribers."""
        subscribers = [
            Subscriber("user1@example.com", ["NO"], "2026-01-01T00:00:00Z"),
            Subscriber("user2@example.com", ["SE"], "2026-01-01T00:00:00Z"),
        ]
        
        warnings = validate_subscribers(subscribers)
        
        assert len(warnings) == 0

    def test_duplicate_emails_warning(self):
        """Test warning for duplicate emails."""
        subscribers = [
            Subscriber("user@example.com", ["NO"], "2026-01-01T00:00:00Z"),
            Subscriber("user@example.com", ["SE"], "2026-01-01T00:00:00Z"),
        ]
        
        warnings = validate_subscribers(subscribers)
        
        assert any("duplicate" in w.lower() for w in warnings)


class TestGetSubscriberSummary:
    """Tests for subscriber summary."""

    def test_summary(self):
        """Test getting subscriber summary."""
        subscribers = [
            Subscriber("user1@example.com", ["NO", "SE"], "2026-01-01T00:00:00Z", active=True),
            Subscriber("user2@example.com", ["NO"], "2026-01-01T00:00:00Z", active=True),
            Subscriber("user3@example.com", ["DE"], "2026-01-01T00:00:00Z", active=False),
        ]
        
        summary = get_subscriber_summary(subscribers)
        
        assert summary["total_subscribers"] == 3
        assert summary["active_subscribers"] == 2
        assert summary["subscribers_per_country"]["NO"] == 2

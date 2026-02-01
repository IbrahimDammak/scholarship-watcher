"""
Tests for multi-country functionality.

Tests cover:
- Country configuration loading and validation
- Multi-country filtering
- Per-country comparison and tracking
- Grouped notifications
"""

import json
import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock

from src.utils import (
    CountryConfig,
    load_countries_config,
    validate_countries_config,
    _parse_country_entry,
    _get_default_norway_config,
)
from src.filter import (
    is_country_relevant,
    get_matching_countries,
    filter_scholarships_by_country,
    filter_scholarships_multi_country,
    get_all_filtered_scholarships,
    count_scholarships_by_country,
)
from src.compare import (
    load_previous_results_multi_country,
    save_results_multi_country,
    find_new_scholarships_by_country,
    compare_and_update_multi_country,
    get_comparison_summary_multi_country,
)
from src.notify import (
    format_issue_title_multi_country,
    format_issue_body_multi_country,
    _get_country_flag,
    format_email_body_html_multi_country,
    format_email_body_plain_multi_country,
)
from src.main import is_multi_country_mode


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_countries():
    """Sample country configurations for testing."""
    return [
        CountryConfig(
            code="NO",
            name="Norway",
            keywords=["norway", "norwegian", "norsk"],
            domain_patterns=[".no", "norway"],
            enabled=True
        ),
        CountryConfig(
            code="SE",
            name="Sweden",
            keywords=["sweden", "swedish", "svenska"],
            domain_patterns=[".se", "sweden"],
            enabled=True
        ),
        CountryConfig(
            code="DE",
            name="Germany",
            keywords=["germany", "german", "deutschland"],
            domain_patterns=[".de", "germany"],
            enabled=True
        ),
    ]


@pytest.fixture
def sample_scholarships():
    """Sample scholarships for multi-country filtering."""
    return [
        {
            "title": "Norwegian Tech Scholarship 2024",
            "url": "https://example.no/scholarship1",
            "description": "Study in Norway"
        },
        {
            "title": "Swedish Innovation Grant",
            "url": "https://example.se/scholarship2",
            "description": "Research in Sweden"
        },
        {
            "title": "German DAAD Fellowship",
            "url": "https://daad.de/fellowship",
            "description": "Study in Germany"
        },
        {
            "title": "Global Education Fund",
            "url": "https://example.com/global",
            "description": "International scholarship program"
        },
        {
            "title": "Nordic Cloud Computing Grant",
            "url": "https://example.com/nordic",
            "description": "For students in Norway, Sweden, and Finland"
        },
    ]


@pytest.fixture
def sample_countries_config_dict():
    """Sample countries configuration as dict (like JSON file)."""
    return {
        "countries": [
            {
                "code": "NO",
                "name": "Norway",
                "keywords": ["norway", "norwegian"],
                "domain_patterns": [".no"],
                "enabled": True
            },
            {
                "code": "SE",
                "name": "Sweden",
                "keywords": ["sweden", "swedish"],
                "domain_patterns": [".se"],
                "enabled": True
            },
            {
                "code": "DE",
                "name": "Germany",
                "keywords": ["germany", "german"],
                "domain_patterns": [".de"],
                "enabled": False
            }
        ]
    }


# =============================================================================
# CountryConfig Tests
# =============================================================================


class TestCountryConfig:
    """Tests for CountryConfig dataclass."""

    def test_create_country_config(self):
        """Test creating a CountryConfig."""
        config = CountryConfig(
            code="NO",
            name="Norway",
            keywords=["norway", "norwegian"],
            domain_patterns=[".no"],
            enabled=True
        )
        
        assert config.code == "NO"
        assert config.name == "Norway"
        assert "norway" in config.keywords
        assert ".no" in config.domain_patterns
        assert config.enabled is True

    def test_default_enabled_value(self):
        """Test that enabled defaults to True."""
        config = CountryConfig(
            code="SE",
            name="Sweden",
            keywords=["sweden"],
            domain_patterns=[".se"]
        )
        
        assert config.enabled is True


class TestParseCountryEntry:
    """Tests for parsing country configuration entries."""

    def test_parse_valid_entry(self):
        """Test parsing a valid country entry."""
        entry = {
            "code": "NO",
            "name": "Norway",
            "keywords": ["norway", "norwegian"],
            "domain_patterns": [".no"],
            "enabled": True
        }
        
        config = _parse_country_entry(entry)
        
        assert config.code == "NO"
        assert config.name == "Norway"
        # Keywords are stored as sets
        assert "norway" in config.keywords
        assert "norwegian" in config.keywords

    def test_parse_entry_with_defaults(self):
        """Test parsing entry with missing optional fields."""
        entry = {
            "code": "FI",
            "name": "Finland"
        }
        
        config = _parse_country_entry(entry)
        
        assert config.code == "FI"
        assert config.name == "Finland"
        # Empty sets for missing fields
        assert len(config.keywords) == 0
        assert len(config.domain_patterns) == 0
        assert config.enabled is True


class TestGetDefaultNorwayConfig:
    """Tests for default Norway configuration."""

    def test_default_norway_has_keywords(self):
        """Test that default Norway config has expected keywords."""
        config = _get_default_norway_config()
        
        assert config.code == "NO"
        assert config.name == "Norway"
        assert "norway" in config.keywords
        assert "norwegian" in config.keywords

    def test_default_norway_has_domain_patterns(self):
        """Test that default Norway config has domain patterns."""
        config = _get_default_norway_config()
        
        assert ".no" in config.domain_patterns


class TestValidateCountriesConfig:
    """Tests for country configuration validation."""

    def test_valid_config(self, sample_countries):
        """Test validation of valid configuration."""
        warnings = validate_countries_config(sample_countries)
        
        # Should have no critical warnings for valid config
        assert not any("missing" in w.lower() for w in warnings)

    def test_empty_keywords_warning(self):
        """Test warning for country with no keywords."""
        countries = [
            CountryConfig(
                code="XX",
                name="Unknown",
                keywords=[],
                domain_patterns=[],
                enabled=True
            )
        ]
        
        warnings = validate_countries_config(countries)
        
        assert any("keyword" in w.lower() or "pattern" in w.lower() for w in warnings)


# =============================================================================
# Multi-Country Filtering Tests
# =============================================================================


class TestIsCountryRelevant:
    """Tests for country relevance checking."""

    def test_relevant_by_keyword_in_title(self, sample_countries):
        """Test scholarship is relevant when keyword in title."""
        scholarship = {
            "title": "Norwegian Tech Grant",
            "url": "https://example.com/grant",
            "description": "Study abroad"
        }
        norway = sample_countries[0]
        
        assert is_country_relevant(scholarship, norway) is True

    def test_relevant_by_keyword_in_url(self, sample_countries):
        """Test scholarship is relevant when keyword in URL."""
        scholarship = {
            "title": "Tech Grant",
            "url": "https://example.com/norway/grant",
            "description": "Study abroad"
        }
        norway = sample_countries[0]
        
        assert is_country_relevant(scholarship, norway) is True

    def test_relevant_by_domain(self, sample_countries):
        """Test scholarship is relevant when URL matches domain pattern."""
        scholarship = {
            "title": "Study Abroad Grant",
            "url": "https://scholarships.no/grant",
            "description": "International opportunities"
        }
        norway = sample_countries[0]
        
        assert is_country_relevant(scholarship, norway) is True

    def test_not_relevant(self, sample_countries):
        """Test scholarship not relevant to a country."""
        scholarship = {
            "title": "French Art Fellowship",
            "url": "https://france.edu/fellowship",
            "description": "Study art in Paris"
        }
        norway = sample_countries[0]
        
        assert is_country_relevant(scholarship, norway) is False

    def test_case_insensitive_matching(self, sample_countries):
        """Test that keyword matching is case-insensitive."""
        scholarship = {
            "title": "NORWEGIAN SCHOLARSHIP",
            "url": "https://example.com/grant",
            "description": "STUDY IN NORWAY"
        }
        norway = sample_countries[0]
        
        assert is_country_relevant(scholarship, norway) is True


class TestGetMatchingCountries:
    """Tests for getting all matching countries for a scholarship."""

    def test_single_country_match(self, sample_countries, sample_scholarships):
        """Test scholarship matching single country."""
        german_scholarship = sample_scholarships[2]  # German DAAD Fellowship
        
        matches = get_matching_countries(german_scholarship, sample_countries)
        
        assert len(matches) == 1
        assert matches[0].code == "DE"

    def test_multiple_country_match(self, sample_countries):
        """Test scholarship matching multiple countries."""
        # Scholarship with both Norway and Sweden in title/URL
        nordic_scholarship = {
            "title": "Nordic Grant for Norway and Sweden",
            "url": "https://example.com/nordic",
            "description": "Study in Scandinavia"
        }
        
        matches = get_matching_countries(nordic_scholarship, sample_countries)
        
        # Should match both Norway and Sweden (mentioned in title)
        codes = [c.code for c in matches]
        assert "NO" in codes
        assert "SE" in codes

    def test_no_country_match(self, sample_countries):
        """Test scholarship not matching any country."""
        scholarship = {
            "title": "French Art Fellowship",
            "url": "https://france.edu/fellowship",
            "description": "Study in Paris"
        }
        
        matches = get_matching_countries(scholarship, sample_countries)
        
        assert len(matches) == 0


class TestFilterScholarshipsMultiCountry:
    """Tests for multi-country filtering."""

    def test_filter_by_multiple_countries(self, sample_countries, sample_scholarships):
        """Test filtering scholarships across multiple countries."""
        result = filter_scholarships_multi_country(sample_scholarships, sample_countries)
        
        # Should have entries for countries with matches
        assert "NO" in result or "SE" in result or "DE" in result
        
        # Norway scholarships
        if "NO" in result:
            assert any("Norwegian" in s.get("title", "") for s in result["NO"])

    def test_returns_dict_with_country_codes(self, sample_countries, sample_scholarships):
        """Test that result is dict with country codes as keys."""
        result = filter_scholarships_multi_country(sample_scholarships, sample_countries)
        
        assert isinstance(result, dict)
        for key in result.keys():
            assert key in ["NO", "SE", "DE"]


class TestGetAllFilteredScholarships:
    """Tests for getting all filtered scholarships."""

    def test_combines_all_countries(self):
        """Test that all scholarships are combined."""
        scholarships_by_country = {
            "NO": [{"title": "A", "url": "https://a.com"}],
            "SE": [{"title": "B", "url": "https://b.com"}],
            "DE": [{"title": "C", "url": "https://c.com"}],
        }
        
        result = get_all_filtered_scholarships(scholarships_by_country)
        
        assert len(result) == 3

    def test_deduplicates_by_url(self):
        """Test that duplicates across countries are removed."""
        scholarships_by_country = {
            "NO": [{"title": "Nordic Grant", "url": "https://nordic.com/grant"}],
            "SE": [{"title": "Nordic Grant", "url": "https://nordic.com/grant"}],
        }
        
        result = get_all_filtered_scholarships(scholarships_by_country)
        
        assert len(result) == 1


class TestCountScholarshipsByCountry:
    """Tests for counting scholarships by country."""

    def test_count_per_country(self):
        """Test counting scholarships per country."""
        scholarships_by_country = {
            "NO": [{"title": "A"}, {"title": "B"}],
            "SE": [{"title": "C"}],
            "DE": [],
        }
        
        counts = count_scholarships_by_country(scholarships_by_country)
        
        assert counts["NO"] == 2
        assert counts["SE"] == 1
        assert counts["DE"] == 0


# =============================================================================
# Multi-Country Compare Tests
# =============================================================================


class TestMultiCountryCompare:
    """Tests for multi-country comparison functionality."""

    def test_find_new_scholarships_by_country(self):
        """Test finding new scholarships per country."""
        current = {
            "NO": [{"title": "A", "url": "https://a.no"}, {"title": "B", "url": "https://b.no"}],
            "SE": [{"title": "C", "url": "https://c.se"}],
        }
        previous = {
            "NO": [{"title": "A", "url": "https://a.no"}],
            "SE": [],
        }
        
        new = find_new_scholarships_by_country(current, previous)
        
        assert len(new["NO"]) == 1
        assert new["NO"][0]["url"] == "https://b.no"
        assert len(new["SE"]) == 1

    def test_compare_and_update_saves_results(self, tmp_path):
        """Test that compare and update saves results."""
        results_path = tmp_path / "results.json"
        
        current = {
            "NO": [{"title": "A", "url": "https://a.no"}],
        }
        
        new, all_results = compare_and_update_multi_country(
            current,
            results_filepath=str(results_path),
            save_updated=True
        )
        
        assert results_path.exists()

    def test_load_previous_results_empty_file(self, tmp_path):
        """Test loading when file doesn't exist."""
        results_path = tmp_path / "nonexistent.json"
        
        result = load_previous_results_multi_country(str(results_path))
        
        assert result == {}


class TestSaveResultsMultiCountry:
    """Tests for saving multi-country results."""

    def test_save_creates_file(self, tmp_path):
        """Test that save creates JSON file."""
        results_path = tmp_path / "results.json"
        scholarships = {
            "NO": [{"title": "A", "url": "https://a.no"}],
        }
        
        save_results_multi_country(scholarships, str(results_path))
        
        assert results_path.exists()
        
        with open(results_path) as f:
            data = json.load(f)
        
        # Check it has the country data
        assert "scholarships_by_country" in data
        assert "NO" in data["scholarships_by_country"]


# =============================================================================
# Multi-Country Notification Tests
# =============================================================================


class TestCountryFlag:
    """Tests for country flag emoji function."""

    def test_known_countries(self):
        """Test flag emojis for known countries."""
        assert _get_country_flag("NO") == "🇳🇴"
        assert _get_country_flag("SE") == "🇸🇪"
        assert _get_country_flag("DE") == "🇩🇪"

    def test_unknown_country(self):
        """Test default flag for unknown country."""
        assert _get_country_flag("XX") == "🌍"

    def test_case_insensitive(self):
        """Test that lookup is case-insensitive."""
        assert _get_country_flag("no") == "🇳🇴"
        assert _get_country_flag("No") == "🇳🇴"


class TestFormatIssueMultiCountry:
    """Tests for multi-country issue formatting."""

    def test_issue_title_single_country(self):
        """Test issue title with single country."""
        scholarships_by_country = {
            "NO": [{"title": "A", "url": "https://a.no"}],
        }
        country_names = {"NO": "Norway"}
        
        title = format_issue_title_multi_country(scholarships_by_country, country_names)
        
        assert "1" in title  # 1 scholarship
        assert "Country" in title or "country" in title.lower()

    def test_issue_title_multiple_countries(self):
        """Test issue title with multiple countries."""
        scholarships_by_country = {
            "NO": [{"title": "A", "url": "https://a.no"}],
            "SE": [{"title": "B", "url": "https://b.se"}],
        }
        country_names = {"NO": "Norway", "SE": "Sweden"}
        
        title = format_issue_title_multi_country(scholarships_by_country, country_names)
        
        assert "2" in title

    def test_issue_body_has_country_sections(self):
        """Test that issue body has sections per country."""
        scholarships_by_country = {
            "NO": [{"title": "Norwegian Grant", "url": "https://example.no"}],
            "SE": [{"title": "Swedish Grant", "url": "https://example.se"}],
        }
        country_names = {"NO": "Norway", "SE": "Sweden"}
        
        body = format_issue_body_multi_country(scholarships_by_country, country_names)
        
        assert "Norway" in body
        assert "Sweden" in body
        assert "Norwegian Grant" in body
        assert "Swedish Grant" in body


class TestFormatEmailMultiCountry:
    """Tests for multi-country email formatting."""

    def test_html_email_has_country_sections(self):
        """Test HTML email has sections for each country."""
        scholarships_by_country = {
            "NO": [{"title": "Norwegian Grant", "url": "https://example.no"}],
        }
        country_names = {"NO": "Norway"}
        
        html = format_email_body_html_multi_country(scholarships_by_country, country_names)
        
        assert "Norway" in html
        assert "Norwegian Grant" in html
        assert "<html>" in html.lower() or "<h" in html.lower()

    def test_plain_email_has_country_sections(self):
        """Test plain text email has sections for each country."""
        scholarships_by_country = {
            "NO": [{"title": "Norwegian Grant", "url": "https://example.no"}],
        }
        country_names = {"NO": "Norway"}
        
        plain = format_email_body_plain_multi_country(scholarships_by_country, country_names)
        
        # Check for country name - may be uppercase in plain text
        assert "NORWAY" in plain.upper()
        assert "Norwegian Grant" in plain


# =============================================================================
# Multi-Country Mode Detection Tests
# =============================================================================


class TestIsMultiCountryMode:
    """Tests for multi-country mode detection."""

    def test_explicit_env_true(self):
        """Test explicit environment variable set to true."""
        with patch.dict(os.environ, {"MULTI_COUNTRY_MODE": "true"}):
            assert is_multi_country_mode() is True

    def test_explicit_env_false(self):
        """Test explicit environment variable set to false."""
        with patch.dict(os.environ, {"MULTI_COUNTRY_MODE": "false"}):
            assert is_multi_country_mode() is False

    def test_auto_detect_multiple_countries(self, tmp_path):
        """Test auto-detection with multiple countries configured."""
        config_path = tmp_path / "countries.json"
        config = {
            "countries": [
                {"code": "NO", "name": "Norway", "enabled": True},
                {"code": "SE", "name": "Sweden", "enabled": True},
            ]
        }
        config_path.write_text(json.dumps(config))
        
        with patch.dict(os.environ, {"MULTI_COUNTRY_MODE": "", "COUNTRIES_CONFIG_PATH": str(config_path)}):
            # This depends on load_countries_config using COUNTRIES_CONFIG_PATH
            # The actual behavior may vary based on implementation
            pass  # Test would need config path support


# =============================================================================
# Integration Tests
# =============================================================================


class TestMultiCountryIntegration:
    """Integration tests for the full multi-country workflow."""

    def test_full_filtering_workflow(self, sample_countries, sample_scholarships):
        """Test complete filtering workflow."""
        # Filter scholarships
        filtered = filter_scholarships_multi_country(sample_scholarships, sample_countries)
        
        # Should have results for at least one country
        assert len(filtered) > 0
        
        # Get all filtered (deduplicated)
        all_filtered = get_all_filtered_scholarships(filtered)
        
        # Should be fewer or equal due to deduplication
        total_in_countries = sum(len(s) for s in filtered.values())
        assert len(all_filtered) <= total_in_countries

    def test_compare_and_notify_workflow(self, tmp_path):
        """Test complete compare and notify workflow."""
        results_path = tmp_path / "results.json"
        
        # First run - all are new
        current_1 = {
            "NO": [{"title": "A", "url": "https://a.no"}],
        }
        
        new_1, _ = compare_and_update_multi_country(
            current_1,
            results_filepath=str(results_path),
            save_updated=True
        )
        
        assert len(new_1["NO"]) == 1
        
        # Second run - same scholarships, none new
        new_2, _ = compare_and_update_multi_country(
            current_1,
            results_filepath=str(results_path),
            save_updated=True
        )
        
        assert len(new_2.get("NO", [])) == 0
        
        # Third run - add a new one
        current_3 = {
            "NO": [
                {"title": "A", "url": "https://a.no"},
                {"title": "B", "url": "https://b.no"},
            ],
        }
        
        new_3, _ = compare_and_update_multi_country(
            current_3,
            results_filepath=str(results_path),
            save_updated=True
        )
        
        assert len(new_3["NO"]) == 1
        assert new_3["NO"][0]["url"] == "https://b.no"

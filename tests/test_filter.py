"""
Tests for the filter module.

Tests cover:
- Norway keyword matching
- Tech/IT keyword matching
- Case-insensitive matching
- False positive detection
- Relevance scoring
- Various filtering configurations
"""

import pytest

from src.filter import (
    filter_scholarships,
    filter_scholarships_flexible,
    is_norway_relevant,
    is_tech_relevant,
    is_likely_false_positive,
    contains_any_keyword,
    calculate_relevance_score,
    normalize_text_for_matching,
    NORWAY_KEYWORDS,
    TECH_KEYWORDS,
)


class TestNormalizeTextForMatching:
    """Tests for text normalization."""
    
    def test_lowercase_conversion(self):
        """Test that text is converted to lowercase."""
        assert normalize_text_for_matching("NORWAY") == "norway"
        assert normalize_text_for_matching("Computer Science") == "computer science"
    
    def test_whitespace_normalization(self):
        """Test that whitespace is normalized."""
        # Multiple spaces are reduced to single space
        result = normalize_text_for_matching("  multiple   spaces  ")
        assert "multiple spaces" in result
        assert "   " not in result  # No triple spaces
        # Tabs and newlines become spaces
        result2 = normalize_text_for_matching("tabs\tand\nnewlines")
        assert "tabs and newlines" in result2
    
    def test_empty_string(self):
        """Test handling of empty string."""
        assert normalize_text_for_matching("") == ""
    
    def test_none_input(self):
        """Test handling of None input."""
        assert normalize_text_for_matching(None) == ""


class TestContainsAnyKeyword:
    """Tests for keyword matching function."""
    
    def test_single_keyword_match(self):
        """Test matching a single keyword."""
        text = "Study in Norway for free"
        keywords = {"norway", "sweden", "finland"}
        
        assert contains_any_keyword(text, keywords) is True
    
    def test_case_insensitive_match(self):
        """Test that matching is case-insensitive."""
        text = "NORWAY scholarship program"
        keywords = {"norway"}
        
        assert contains_any_keyword(text, keywords) is True
    
    def test_no_match(self):
        """Test when no keywords match."""
        text = "Study in Germany"
        keywords = {"norway", "sweden", "finland"}
        
        assert contains_any_keyword(text, keywords) is False
    
    def test_empty_text(self):
        """Test with empty text."""
        assert contains_any_keyword("", {"norway"}) is False
    
    def test_empty_keywords(self):
        """Test with empty keywords set."""
        assert contains_any_keyword("Norway", set()) is False
    
    def test_word_boundary_for_short_keywords(self):
        """Test word boundary matching for short keywords."""
        # "it " should match standalone "IT" but not "with"
        text = "IT scholarship program"
        keywords = {"it "}
        
        result = contains_any_keyword(text.lower() + " ", keywords)
        assert result is True
        
        text2 = "Work with computers"
        result2 = contains_any_keyword(text2, {"it "})
        # "it " won't match inside "with"
        assert result2 is False


class TestIsNorwayRelevant:
    """Tests for Norway relevance detection."""
    
    def test_norway_in_title(self):
        """Test detection of Norway in title."""
        scholarship = {
            "title": "Norway Technology Scholarship 2024",
            "url": "https://example.com/apply"
        }
        assert is_norway_relevant(scholarship) is True
    
    def test_norwegian_keyword(self):
        """Test detection of 'Norwegian' keyword."""
        scholarship = {
            "title": "Norwegian Research Council Grant",
            "url": "https://example.com/grant"
        }
        assert is_norway_relevant(scholarship) is True
    
    def test_norway_in_url(self):
        """Test detection of Norway in URL."""
        scholarship = {
            "title": "Tech Scholarship Program",
            "url": "https://scholarships.norway.no/program"
        }
        assert is_norway_relevant(scholarship) is True
    
    def test_oslo_keyword(self):
        """Test detection of Norwegian city names."""
        scholarship = {
            "title": "University of Oslo Fellowship",
            "url": "https://example.com"
        }
        assert is_norway_relevant(scholarship) is True
    
    def test_ntnu_keyword(self):
        """Test detection of Norwegian university abbreviations."""
        scholarship = {
            "title": "NTNU PhD Position in Computer Science",
            "url": "https://example.com"
        }
        assert is_norway_relevant(scholarship) is True
    
    def test_nordic_keyword(self):
        """Test detection of 'Nordic' keyword."""
        scholarship = {
            "title": "Nordic Masters in Cloud Computing",
            "url": "https://example.com"
        }
        assert is_norway_relevant(scholarship) is True
    
    def test_not_norway_relevant(self):
        """Test scholarship not related to Norway."""
        scholarship = {
            "title": "German Engineering Scholarship",
            "url": "https://germany.edu/scholarship"
        }
        assert is_norway_relevant(scholarship) is False


class TestIsTechRelevant:
    """Tests for tech/IT relevance detection."""
    
    def test_computer_science_match(self):
        """Test detection of computer science keywords."""
        scholarship = {
            "title": "Computer Science PhD Fellowship",
            "url": "https://example.com"
        }
        assert is_tech_relevant(scholarship) is True
    
    def test_cloud_computing_match(self):
        """Test detection of cloud computing keywords."""
        scholarship = {
            "title": "Cloud Infrastructure Scholarship",
            "url": "https://example.com/cloud"
        }
        assert is_tech_relevant(scholarship) is True
    
    def test_data_science_match(self):
        """Test detection of data science keywords."""
        scholarship = {
            "title": "Data Science Research Grant",
            "url": "https://example.com"
        }
        assert is_tech_relevant(scholarship) is True
    
    def test_engineering_match(self):
        """Test detection of engineering keywords."""
        scholarship = {
            "title": "Software Engineering Masters Scholarship",
            "url": "https://example.com"
        }
        assert is_tech_relevant(scholarship) is True
    
    def test_ai_ml_match(self):
        """Test detection of AI/ML keywords."""
        scholarship = {
            "title": "Machine Learning Research Position",
            "url": "https://example.com/ai"
        }
        assert is_tech_relevant(scholarship) is True
    
    def test_cybersecurity_match(self):
        """Test detection of cybersecurity keywords."""
        scholarship = {
            "title": "Cybersecurity Scholarship Program",
            "url": "https://example.com"
        }
        assert is_tech_relevant(scholarship) is True
    
    def test_stem_match(self):
        """Test detection of STEM keywords."""
        scholarship = {
            "title": "STEM Excellence Award",
            "url": "https://example.com"
        }
        assert is_tech_relevant(scholarship) is True
    
    def test_not_tech_relevant(self):
        """Test scholarship not related to tech."""
        scholarship = {
            "title": "Art History Fellowship",
            "url": "https://example.com/arts"
        }
        assert is_tech_relevant(scholarship) is False


class TestIsLikelyFalsePositive:
    """Tests for false positive detection."""
    
    def test_login_link(self):
        """Test detection of login links as false positives."""
        scholarship = {
            "title": "Login to your account",
            "url": "https://example.com/login"
        }
        assert is_likely_false_positive(scholarship) is True
    
    def test_newsletter_link(self):
        """Test detection of newsletter links as false positives."""
        scholarship = {
            "title": "Subscribe to our newsletter",
            "url": "https://example.com/subscribe"
        }
        assert is_likely_false_positive(scholarship) is True
    
    def test_privacy_policy(self):
        """Test detection of privacy policy as false positive."""
        scholarship = {
            "title": "Privacy Policy",
            "url": "https://example.com/privacy-policy"
        }
        assert is_likely_false_positive(scholarship) is True
    
    def test_valid_scholarship(self):
        """Test that valid scholarships are not flagged."""
        scholarship = {
            "title": "Norway Tech Scholarship 2024",
            "url": "https://example.com/scholarships/apply"
        }
        assert is_likely_false_positive(scholarship) is False


class TestCalculateRelevanceScore:
    """Tests for relevance score calculation."""
    
    def test_high_relevance_scholarship(self):
        """Test high score for highly relevant scholarship."""
        scholarship = {
            "title": "Norway Computer Science PhD Scholarship",
            "url": "https://oslo.edu/cloud-computing-fellowship"
        }
        score = calculate_relevance_score(scholarship)
        
        # Should have high score due to multiple keyword matches
        assert score >= 30
    
    def test_low_relevance_scholarship(self):
        """Test low score for less relevant scholarship."""
        scholarship = {
            "title": "General Study Abroad Program",
            "url": "https://example.com/program"
        }
        score = calculate_relevance_score(scholarship)
        
        assert score < 30
    
    def test_score_capped_at_100(self):
        """Test that score is capped at 100."""
        # Create scholarship with many keywords
        scholarship = {
            "title": "Norway Norwegian Oslo NTNU Computer Science Cloud AI ML Engineering Data Science",
            "url": "https://norway.edu/scholarship/cloud/data-science/ai"
        }
        score = calculate_relevance_score(scholarship)
        
        assert score <= 100


class TestFilterScholarships:
    """Tests for the main filter function."""
    
    def test_filter_norway_and_tech(self):
        """Test filtering requiring both Norway and tech relevance."""
        scholarships = [
            {"title": "Norway Computer Science Scholarship", "url": "https://a.com"},
            {"title": "Norway Art Scholarship", "url": "https://b.com"},
            {"title": "German CS Scholarship", "url": "https://c.com"},
            {"title": "Oslo Data Science Fellowship", "url": "https://d.com"},
        ]
        
        filtered = filter_scholarships(
            scholarships,
            require_norway=True,
            require_tech=True
        )
        
        # Should only include scholarships matching BOTH criteria
        assert len(filtered) == 2
        urls = [s["url"] for s in filtered]
        assert "https://a.com" in urls  # Norway + CS
        assert "https://d.com" in urls  # Oslo + Data Science
    
    def test_filter_norway_only(self):
        """Test filtering for Norway relevance only."""
        scholarships = [
            {"title": "Norway Art Scholarship", "url": "https://a.com"},
            {"title": "German Tech Scholarship", "url": "https://b.com"},
        ]
        
        filtered = filter_scholarships(
            scholarships,
            require_norway=True,
            require_tech=False
        )
        
        assert len(filtered) == 1
        assert filtered[0]["url"] == "https://a.com"
    
    def test_filter_tech_only(self):
        """Test filtering for tech relevance only."""
        scholarships = [
            {"title": "Norway Art Scholarship", "url": "https://a.com"},
            {"title": "German Computer Science Scholarship", "url": "https://b.com"},
        ]
        
        filtered = filter_scholarships(
            scholarships,
            require_norway=False,
            require_tech=True
        )
        
        assert len(filtered) == 1
        assert filtered[0]["url"] == "https://b.com"
    
    def test_exclude_false_positives(self):
        """Test exclusion of false positives."""
        scholarships = [
            {"title": "Norway Computer Science Scholarship", "url": "https://a.com"},
            {"title": "Login to Norway Tech Portal", "url": "https://b.com/login"},
        ]
        
        filtered = filter_scholarships(
            scholarships,
            require_norway=True,
            require_tech=True,
            exclude_false_positives=True
        )
        
        assert len(filtered) == 1
        assert filtered[0]["url"] == "https://a.com"
    
    def test_empty_input(self):
        """Test handling of empty input list."""
        filtered = filter_scholarships([])
        assert filtered == []
    
    def test_all_filtered_out(self):
        """Test when all scholarships are filtered out."""
        scholarships = [
            {"title": "French Literature Grant", "url": "https://a.com"},
            {"title": "Japanese History Fellowship", "url": "https://b.com"},
        ]
        
        filtered = filter_scholarships(
            scholarships,
            require_norway=True,
            require_tech=True
        )
        
        assert filtered == []


class TestFilterScholarshipsFlexible:
    """Tests for flexible filtering (OR logic)."""
    
    def test_flexible_or_logic(self):
        """Test flexible filtering with OR logic."""
        scholarships = [
            {"title": "Norway Art Scholarship", "url": "https://a.com"},  # Norway only
            {"title": "German Computer Science Scholarship", "url": "https://b.com"},  # Tech only
            {"title": "French Literature Grant", "url": "https://c.com"},  # Neither
        ]
        
        filtered = filter_scholarships_flexible(
            scholarships,
            require_both=False  # OR logic
        )
        
        # Should include Norway OR tech
        assert len(filtered) == 2
        urls = [s["url"] for s in filtered]
        assert "https://a.com" in urls
        assert "https://b.com" in urls
    
    def test_flexible_and_logic(self):
        """Test flexible filtering with AND logic."""
        scholarships = [
            {"title": "Norway Computer Science Scholarship", "url": "https://a.com"},  # Both
            {"title": "Norway Art Scholarship", "url": "https://b.com"},  # Norway only
            {"title": "German Computer Science Scholarship", "url": "https://c.com"},  # Tech only
        ]
        
        filtered = filter_scholarships_flexible(
            scholarships,
            require_both=True  # AND logic
        )
        
        # Should only include scholarships matching BOTH
        assert len(filtered) == 1
        assert filtered[0]["url"] == "https://a.com"
    
    def test_excludes_false_positives(self):
        """Test that false positives are always excluded."""
        scholarships = [
            {"title": "Norway Computer Science Scholarship", "url": "https://a.com"},
            {"title": "Subscribe to Norway Tech Newsletter", "url": "https://b.com"},
        ]
        
        filtered = filter_scholarships_flexible(scholarships, require_both=False)
        
        assert len(filtered) == 1
        assert filtered[0]["url"] == "https://a.com"

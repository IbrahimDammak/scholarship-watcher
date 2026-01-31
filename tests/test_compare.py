"""
Tests for the compare module.

Tests cover:
- Detection of new scholarships
- Duplicate prevention
- Loading and saving results
- Merging scholarships
- Comparison summary
"""

import json
import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock

from src.compare import (
    compare_and_update,
    find_new_scholarships,
    find_removed_scholarships,
    merge_scholarships,
    load_previous_results,
    save_results,
    get_scholarship_identifier,
    build_url_set,
    get_comparison_summary,
)


class TestGetScholarshipIdentifier:
    """Tests for scholarship identifier extraction."""
    
    def test_url_as_identifier(self):
        """Test that URL is used as identifier."""
        scholarship = {
            "title": "Test Scholarship",
            "url": "https://example.com/scholarship/123"
        }
        
        identifier = get_scholarship_identifier(scholarship)
        
        assert identifier == "https://example.com/scholarship/123"
    
    def test_missing_url(self):
        """Test handling of missing URL."""
        scholarship = {"title": "No URL Scholarship"}
        
        identifier = get_scholarship_identifier(scholarship)
        
        assert identifier == ""
    
    def test_empty_url(self):
        """Test handling of empty URL."""
        scholarship = {"title": "Empty URL", "url": ""}
        
        identifier = get_scholarship_identifier(scholarship)
        
        assert identifier == ""


class TestBuildUrlSet:
    """Tests for URL set building."""
    
    def test_build_set_from_scholarships(self):
        """Test building URL set from scholarship list."""
        scholarships = [
            {"title": "A", "url": "https://a.com"},
            {"title": "B", "url": "https://b.com"},
            {"title": "C", "url": "https://c.com"},
        ]
        
        url_set = build_url_set(scholarships)
        
        assert url_set == {"https://a.com", "https://b.com", "https://c.com"}
    
    def test_skip_empty_urls(self):
        """Test that empty URLs are skipped."""
        scholarships = [
            {"title": "A", "url": "https://a.com"},
            {"title": "B", "url": ""},
            {"title": "C"},  # No URL key
        ]
        
        url_set = build_url_set(scholarships)
        
        assert url_set == {"https://a.com"}
    
    def test_empty_list(self):
        """Test with empty scholarship list."""
        url_set = build_url_set([])
        
        assert url_set == set()


class TestFindNewScholarships:
    """Tests for detecting new scholarships."""
    
    def test_find_new_entries(self):
        """Test detection of new scholarship entries."""
        current = [
            {"title": "Existing", "url": "https://existing.com"},
            {"title": "New One", "url": "https://new.com"},
        ]
        previous = [
            {"title": "Existing", "url": "https://existing.com"},
        ]
        
        new = find_new_scholarships(current, previous)
        
        assert len(new) == 1
        assert new[0]["url"] == "https://new.com"
    
    def test_no_new_entries(self):
        """Test when there are no new entries."""
        current = [
            {"title": "A", "url": "https://a.com"},
        ]
        previous = [
            {"title": "A", "url": "https://a.com"},
        ]
        
        new = find_new_scholarships(current, previous)
        
        assert len(new) == 0
    
    def test_all_new_entries(self):
        """Test when all entries are new."""
        current = [
            {"title": "A", "url": "https://a.com"},
            {"title": "B", "url": "https://b.com"},
        ]
        previous = []
        
        new = find_new_scholarships(current, previous)
        
        assert len(new) == 2
    
    def test_empty_current(self):
        """Test with empty current list."""
        current = []
        previous = [
            {"title": "A", "url": "https://a.com"},
        ]
        
        new = find_new_scholarships(current, previous)
        
        assert len(new) == 0
    
    def test_matching_by_url_not_title(self):
        """Test that matching is by URL, not title."""
        current = [
            {"title": "Updated Title", "url": "https://same.com"},
        ]
        previous = [
            {"title": "Original Title", "url": "https://same.com"},
        ]
        
        new = find_new_scholarships(current, previous)
        
        # Same URL means not new, even if title differs
        assert len(new) == 0


class TestFindRemovedScholarships:
    """Tests for detecting removed scholarships."""
    
    def test_find_removed_entries(self):
        """Test detection of removed scholarship entries."""
        current = [
            {"title": "Still Here", "url": "https://still.com"},
        ]
        previous = [
            {"title": "Still Here", "url": "https://still.com"},
            {"title": "Gone Now", "url": "https://gone.com"},
        ]
        
        removed = find_removed_scholarships(current, previous)
        
        assert len(removed) == 1
        assert removed[0]["url"] == "https://gone.com"
    
    def test_no_removed_entries(self):
        """Test when nothing is removed."""
        current = [
            {"title": "A", "url": "https://a.com"},
        ]
        previous = [
            {"title": "A", "url": "https://a.com"},
        ]
        
        removed = find_removed_scholarships(current, previous)
        
        assert len(removed) == 0


class TestMergeScholarships:
    """Tests for merging scholarship lists."""
    
    def test_deduplicate_current(self):
        """Test deduplication of current scholarships."""
        current = [
            {"title": "A", "url": "https://a.com"},
            {"title": "A Again", "url": "https://a.com"},  # Duplicate
            {"title": "B", "url": "https://b.com"},
        ]
        
        merged = merge_scholarships(current, [], keep_removed=False)
        
        assert len(merged) == 2
        urls = [s["url"] for s in merged]
        assert len(urls) == len(set(urls))
    
    def test_merge_with_previous(self):
        """Test merging current with previous (keeping removed)."""
        current = [
            {"title": "Current A", "url": "https://a.com"},
        ]
        previous = [
            {"title": "Previous B", "url": "https://b.com"},
        ]
        
        merged = merge_scholarships(current, previous, keep_removed=True)
        
        assert len(merged) == 2
        urls = [s["url"] for s in merged]
        assert "https://a.com" in urls
        assert "https://b.com" in urls
    
    def test_current_takes_precedence(self):
        """Test that current entries take precedence over previous."""
        current = [
            {"title": "Updated Title", "url": "https://a.com"},
        ]
        previous = [
            {"title": "Old Title", "url": "https://a.com"},
        ]
        
        merged = merge_scholarships(current, previous, keep_removed=True)
        
        assert len(merged) == 1
        assert merged[0]["title"] == "Updated Title"


class TestLoadPreviousResults:
    """Tests for loading previous results from file."""
    
    def test_load_valid_json_list(self):
        """Test loading valid JSON list format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([
                {"title": "A", "url": "https://a.com"},
                {"title": "B", "url": "https://b.com"},
            ], f)
            filepath = f.name
        
        try:
            results = load_previous_results(filepath)
            
            assert len(results) == 2
            assert results[0]["url"] == "https://a.com"
        finally:
            os.unlink(filepath)
    
    def test_load_valid_json_dict(self):
        """Test loading valid JSON dict format with scholarships key."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({
                "last_updated": "2024-01-01",
                "scholarships": [
                    {"title": "A", "url": "https://a.com"},
                ]
            }, f)
            filepath = f.name
        
        try:
            results = load_previous_results(filepath)
            
            assert len(results) == 1
        finally:
            os.unlink(filepath)
    
    def test_load_nonexistent_file(self):
        """Test loading from non-existent file returns empty list."""
        results = load_previous_results("/nonexistent/path/file.json")
        
        assert results == []
    
    def test_load_invalid_json(self):
        """Test loading invalid JSON returns empty list."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write("not valid json {{{")
            filepath = f.name
        
        try:
            results = load_previous_results(filepath)
            
            assert results == []
        finally:
            os.unlink(filepath)


class TestSaveResults:
    """Tests for saving results to file."""
    
    def test_save_scholarships(self):
        """Test saving scholarships to file."""
        scholarships = [
            {"title": "A", "url": "https://a.com"},
            {"title": "B", "url": "https://b.com"},
        ]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            filepath = f.name
        
        try:
            success = save_results(scholarships, filepath, include_metadata=False)
            
            assert success is True
            
            # Verify file contents
            with open(filepath, 'r') as f:
                saved = json.load(f)
            
            assert len(saved) == 2
        finally:
            os.unlink(filepath)
    
    def test_save_with_metadata(self):
        """Test saving scholarships with metadata."""
        scholarships = [{"title": "A", "url": "https://a.com"}]
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            filepath = f.name
        
        try:
            success = save_results(scholarships, filepath, include_metadata=True)
            
            assert success is True
            
            with open(filepath, 'r') as f:
                saved = json.load(f)
            
            assert "last_updated" in saved
            assert "count" in saved
            assert "scholarships" in saved
            assert saved["count"] == 1
        finally:
            os.unlink(filepath)
    
    def test_creates_directory_if_needed(self):
        """Test that parent directory is created if needed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "nested", "dir", "results.json")
            
            success = save_results([{"title": "A", "url": "https://a.com"}], filepath)
            
            assert success is True
            assert os.path.exists(filepath)


class TestCompareAndUpdate:
    """Tests for the main compare_and_update function."""
    
    def test_compare_with_new_entries(self):
        """Test comparison finding new entries."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump([
                {"title": "Existing", "url": "https://existing.com"},
            ], f)
            filepath = f.name
        
        try:
            current = [
                {"title": "Existing", "url": "https://existing.com"},
                {"title": "New", "url": "https://new.com"},
            ]
            
            new_scholarships, all_scholarships = compare_and_update(
                current,
                results_filepath=filepath,
                save_updated=False  # Don't modify file in test
            )
            
            assert len(new_scholarships) == 1
            assert new_scholarships[0]["url"] == "https://new.com"
            assert len(all_scholarships) == 2
        finally:
            os.unlink(filepath)
    
    def test_compare_with_no_previous(self):
        """Test comparison when no previous results exist."""
        filepath = "/nonexistent/path/results.json"
        
        current = [
            {"title": "A", "url": "https://a.com"},
            {"title": "B", "url": "https://b.com"},
        ]
        
        new_scholarships, all_scholarships = compare_and_update(
            current,
            results_filepath=filepath,
            save_updated=False
        )
        
        # All should be new
        assert len(new_scholarships) == 2
    
    def test_saves_updated_results(self):
        """Test that results are saved when save_updated=True."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = os.path.join(tmpdir, "results.json")
            
            current = [{"title": "A", "url": "https://a.com"}]
            
            compare_and_update(
                current,
                results_filepath=filepath,
                save_updated=True
            )
            
            assert os.path.exists(filepath)


class TestGetComparisonSummary:
    """Tests for comparison summary generation."""
    
    def test_summary_counts(self):
        """Test that summary has correct counts."""
        current = [
            {"title": "Existing", "url": "https://existing.com"},
            {"title": "New", "url": "https://new.com"},
        ]
        previous = [
            {"title": "Existing", "url": "https://existing.com"},
            {"title": "Removed", "url": "https://removed.com"},
        ]
        
        summary = get_comparison_summary(current, previous)
        
        assert summary["current_count"] == 2
        assert summary["previous_count"] == 2
        assert summary["new_count"] == 1
        assert summary["removed_count"] == 1
        assert summary["unchanged_count"] == 1
    
    def test_empty_lists(self):
        """Test summary with empty lists."""
        summary = get_comparison_summary([], [])
        
        assert summary["current_count"] == 0
        assert summary["previous_count"] == 0
        assert summary["new_count"] == 0
        assert summary["removed_count"] == 0
        assert summary["unchanged_count"] == 0

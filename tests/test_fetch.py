"""
Tests for the fetch module.

Tests cover:
- Successful URL fetching
- HTTP error handling
- Timeout handling
- Connection error handling
- URL validation
- Session configuration
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import requests

from src.fetch import (
    fetch_scholarship_pages,
    fetch_single_url,
    create_session,
    validate_url,
    get_successful_fetches,
    FetchResult,
    DEFAULT_TIMEOUT,
)


class TestValidateUrl:
    """Tests for URL validation function."""
    
    def test_valid_http_url(self):
        """Test that valid HTTP URLs pass validation."""
        assert validate_url("http://example.com") is True
        assert validate_url("http://example.com/path") is True
        assert validate_url("http://example.com/path?query=value") is True
    
    def test_valid_https_url(self):
        """Test that valid HTTPS URLs pass validation."""
        assert validate_url("https://example.com") is True
        assert validate_url("https://scholarships.example.com/apply") is True
    
    def test_invalid_urls(self):
        """Test that invalid URLs fail validation."""
        assert validate_url("") is False
        assert validate_url("not-a-url") is False
        assert validate_url("ftp://example.com") is False
        assert validate_url("file:///local/path") is False
        assert validate_url("//example.com") is False
    
    def test_malformed_urls(self):
        """Test that malformed URLs are handled gracefully."""
        assert validate_url("http://") is False
        assert validate_url("https://") is False


class TestCreateSession:
    """Tests for session creation."""
    
    def test_session_has_headers(self):
        """Test that created session has required headers."""
        session = create_session()
        
        assert "User-Agent" in session.headers
        assert "Accept" in session.headers
    
    def test_session_has_retry_adapter(self):
        """Test that session has retry adapter mounted."""
        session = create_session(max_retries=3)
        
        # Check adapters are mounted
        assert "http://" in session.adapters
        assert "https://" in session.adapters
    
    def test_custom_retry_config(self):
        """Test custom retry configuration."""
        session = create_session(max_retries=5, backoff_factor=2.0)
        
        # Session should be created without error
        assert session is not None


class TestFetchSingleUrl:
    """Tests for single URL fetching."""
    
    @patch("src.fetch.requests.Session")
    def test_successful_fetch(self, mock_session_class):
        """Test successful URL fetch returns content."""
        # Setup mock
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>Test content</body></html>"
        
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        
        # Execute
        result = fetch_single_url(
            "https://example.com/scholarships",
            mock_session,
            timeout=30
        )
        
        # Verify
        assert result.success is True
        assert result.html_content == "<html><body>Test content</body></html>"
        assert result.source_url == "https://example.com/scholarships"
        assert result.status_code == 200
        assert result.error_message is None
    
    @patch("src.fetch.requests.Session")
    def test_http_404_error(self, mock_session_class):
        """Test 404 error is handled properly."""
        mock_response = Mock()
        mock_response.status_code = 404
        
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        
        result = fetch_single_url("https://example.com/missing", mock_session)
        
        assert result.success is False
        assert result.html_content is None
        assert result.status_code == 404
        assert result.error_message is not None
        assert "404" in result.error_message
    
    @patch("src.fetch.requests.Session")
    def test_http_500_error(self, mock_session_class):
        """Test 500 server error is handled properly."""
        mock_response = Mock()
        mock_response.status_code = 500
        
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        
        result = fetch_single_url("https://example.com/error", mock_session)
        
        assert result.success is False
        assert result.status_code == 500
    
    @patch("src.fetch.requests.Session")
    def test_timeout_handling(self, mock_session_class):
        """Test timeout is handled gracefully."""
        mock_session = Mock()
        mock_session.get.side_effect = requests.exceptions.Timeout("Connection timed out")
        
        result = fetch_single_url("https://slow-server.com", mock_session)
        
        assert result.success is False
        assert result.html_content is None
        assert result.error_message is not None
        assert "timeout" in result.error_message.lower()
    
    @patch("src.fetch.requests.Session")
    def test_connection_error_handling(self, mock_session_class):
        """Test connection errors are handled gracefully."""
        mock_session = Mock()
        mock_session.get.side_effect = requests.exceptions.ConnectionError("DNS lookup failed")
        
        result = fetch_single_url("https://nonexistent.example", mock_session)
        
        assert result.success is False
        assert result.html_content is None
        assert result.error_message is not None
        assert "connection" in result.error_message.lower()
    
    def test_invalid_url_rejected(self):
        """Test invalid URLs are rejected without making request."""
        mock_session = Mock()
        
        result = fetch_single_url("not-a-valid-url", mock_session)
        
        assert result.success is False
        assert result.error_message is not None
        assert "Invalid URL" in result.error_message
        # Session.get should not be called for invalid URLs
        mock_session.get.assert_not_called()


class TestFetchScholarshipPages:
    """Tests for the main fetch function."""
    
    @patch("src.fetch.create_session")
    @patch("src.fetch.fetch_single_url")
    def test_fetches_all_urls(self, mock_fetch_single, mock_create_session):
        """Test that all provided URLs are fetched."""
        mock_session = Mock()
        mock_create_session.return_value = mock_session
        
        mock_fetch_single.return_value = FetchResult(
            source_url="",
            html_content="<html></html>",
            success=True,
            status_code=200
        )
        
        urls = [
            "https://example1.com",
            "https://example2.com",
            "https://example3.com"
        ]
        
        results = fetch_scholarship_pages(
            urls=urls,
            delay_between_requests=0  # No delay for tests
        )
        
        assert len(results) == 3
        assert mock_fetch_single.call_count == 3
    
    @patch("src.fetch.create_session")
    @patch("src.fetch.fetch_single_url")
    def test_handles_partial_failures(self, mock_fetch_single, mock_create_session):
        """Test that partial failures don't stop the pipeline."""
        mock_session = Mock()
        mock_create_session.return_value = mock_session
        
        # First succeeds, second fails, third succeeds
        mock_fetch_single.side_effect = [
            FetchResult("url1", "<html>1</html>", True, status_code=200),
            FetchResult("url2", None, False, error_message="Failed"),
            FetchResult("url3", "<html>3</html>", True, status_code=200),
        ]
        
        results = fetch_scholarship_pages(
            urls=["url1", "url2", "url3"],
            delay_between_requests=0
        )
        
        assert len(results) == 3
        successful = get_successful_fetches(results)
        assert len(successful) == 2
    
    @patch("src.fetch.create_session")
    def test_empty_url_list(self, mock_create_session):
        """Test handling of empty URL list."""
        results = fetch_scholarship_pages(urls=[])
        
        assert results == []
    
    @patch("src.fetch.create_session")
    @patch("src.fetch.fetch_single_url")
    def test_uses_default_urls_when_none_provided(self, mock_fetch_single, mock_create_session):
        """Test that default URLs are used when none provided."""
        mock_session = Mock()
        mock_create_session.return_value = mock_session
        
        mock_fetch_single.return_value = FetchResult(
            source_url="",
            html_content="<html></html>",
            success=True
        )
        
        results = fetch_scholarship_pages(
            urls=None,
            delay_between_requests=0
        )
        
        # Should use default URLs
        assert mock_fetch_single.call_count > 0


class TestGetSuccessfulFetches:
    """Tests for filtering successful fetches."""
    
    def test_filters_successful_only(self):
        """Test that only successful fetches are returned."""
        results = [
            FetchResult("url1", "<html>1</html>", True),
            FetchResult("url2", None, False),
            FetchResult("url3", "<html>3</html>", True),
            FetchResult("url4", None, False),
        ]
        
        successful = get_successful_fetches(results)
        
        assert len(successful) == 2
        assert all(r.success for r in successful)
    
    def test_empty_list(self):
        """Test handling of empty results list."""
        results = get_successful_fetches([])
        assert results == []
    
    def test_all_failures(self):
        """Test when all fetches fail."""
        results = [
            FetchResult("url1", None, False),
            FetchResult("url2", None, False),
        ]
        
        successful = get_successful_fetches(results)
        assert len(successful) == 0

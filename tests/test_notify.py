"""
Unit tests for the notify module.

Tests cover both GitHub Issue notifications and email notifications.
"""

import os
import smtplib
import ssl
from datetime import datetime
from email.message import EmailMessage
from unittest.mock import MagicMock, Mock, patch, call

import pytest
import requests

from src.notify import (
    # GitHub-related
    GitHubAPIError,
    get_github_credentials,
    parse_repository,
    create_github_session,
    check_rate_limit,
    format_issue_body,
    format_issue_title,
    create_issue,
    notify_new_scholarships,
    check_github_connection,
    # Email-related
    EmailNotificationError,
    get_email_credentials,
    is_email_configured,
    format_email_body_html,
    format_email_body_plain,
    send_email_notification,
    check_email_connection,
)


# =============================================================================
# Test Data Fixtures
# =============================================================================


@pytest.fixture
def sample_scholarships():
    """Sample scholarship data for testing."""
    return [
        {"title": "Norwegian Tech Scholarship 2024", "url": "https://example.com/scholarship1"},
        {"title": "Cloud Computing Grant - Norway", "url": "https://example.com/scholarship2"},
    ]


@pytest.fixture
def single_scholarship():
    """Single scholarship for testing."""
    return [{"title": "AI Research Fellowship", "url": "https://example.com/ai-fellowship"}]


@pytest.fixture
def github_env_vars():
    """GitHub environment variables fixture."""
    return {
        "GITHUB_TOKEN": "ghp_test_token_12345",
        "GITHUB_REPOSITORY": "testowner/testrepo"
    }


@pytest.fixture
def email_env_vars():
    """Email environment variables fixture."""
    return {
        "SMTP_HOST": "smtp.example.com",
        "SMTP_PORT": "587",
        "SMTP_USER": "user@example.com",
        "SMTP_PASSWORD": "securepassword123",
        "EMAIL_FROM": "noreply@example.com",
        "EMAIL_TO": "recipient@example.com"
    }


# =============================================================================
# GitHub Notification Tests
# =============================================================================


class TestGitHubCredentials:
    """Tests for GitHub credentials handling."""
    
    def test_get_github_credentials_success(self, github_env_vars):
        """Test successful retrieval of GitHub credentials."""
        with patch.dict(os.environ, github_env_vars, clear=False):
            token, repo = get_github_credentials()
            assert token == "ghp_test_token_12345"
            assert repo == "testowner/testrepo"
    
    def test_get_github_credentials_missing_token(self):
        """Test error when GITHUB_TOKEN is missing."""
        env = {"GITHUB_REPOSITORY": "owner/repo"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="GITHUB_TOKEN"):
                get_github_credentials()
    
    def test_get_github_credentials_missing_repo(self):
        """Test error when GITHUB_REPOSITORY is missing."""
        env = {"GITHUB_TOKEN": "token"}
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError, match="GITHUB_REPOSITORY"):
                get_github_credentials()


class TestParseRepository:
    """Tests for repository string parsing."""
    
    def test_parse_repository_valid(self):
        """Test parsing valid repository string."""
        owner, repo = parse_repository("myowner/myrepo")
        assert owner == "myowner"
        assert repo == "myrepo"
    
    def test_parse_repository_with_slashes(self):
        """Test parsing repository with extra path components."""
        owner, repo = parse_repository("owner/repo/extra")
        assert owner == "owner"
        assert repo == "repo/extra"
    
    def test_parse_repository_invalid_no_slash(self):
        """Test error on repository without slash."""
        with pytest.raises(ValueError, match="Invalid repository format"):
            parse_repository("noslash")
    
    def test_parse_repository_invalid_empty_parts(self):
        """Test error on repository with empty parts."""
        with pytest.raises(ValueError, match="Invalid repository format"):
            parse_repository("/repo")
        with pytest.raises(ValueError, match="Invalid repository format"):
            parse_repository("owner/")


class TestCreateGitHubSession:
    """Tests for GitHub session creation."""
    
    def test_create_session_headers(self):
        """Test that session is created with correct headers."""
        session = create_github_session("test_token")
        
        assert session.headers["Authorization"] == "Bearer test_token"
        assert "application/vnd.github+json" in str(session.headers["Accept"])
        assert "X-GitHub-Api-Version" in session.headers
        assert "User-Agent" in session.headers


class TestCheckRateLimit:
    """Tests for rate limit checking."""
    
    def test_not_rate_limited(self):
        """Test response that is not rate limited."""
        response = Mock()
        response.status_code = 200
        
        is_limited, wait_time = check_rate_limit(response)
        assert is_limited is False
        assert wait_time == 0
    
    def test_rate_limited_with_headers(self):
        """Test rate limited response with reset headers."""
        import time
        response = Mock()
        response.status_code = 403
        response.headers = {
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": str(int(time.time()) + 60)
        }
        
        is_limited, wait_time = check_rate_limit(response)
        assert is_limited is True
        assert 0 <= wait_time <= 60


class TestFormatIssue:
    """Tests for issue formatting functions."""
    
    def test_format_issue_title_single(self, single_scholarship):
        """Test title formatting for single scholarship."""
        title = format_issue_title(1)
        assert "1 New Scholarship Found" in title
        assert datetime.utcnow().strftime("%Y-%m-%d") in title
    
    def test_format_issue_title_multiple(self, sample_scholarships):
        """Test title formatting for multiple scholarships."""
        title = format_issue_title(2)
        assert "2 New Scholarships Found" in title
    
    def test_format_issue_body(self, sample_scholarships):
        """Test body formatting includes all scholarships."""
        body = format_issue_body(sample_scholarships)
        
        assert "New Scholarships Detected" in body
        assert "Norwegian Tech Scholarship 2024" in body
        assert "Cloud Computing Grant - Norway" in body
        assert "https://example.com/scholarship1" in body
        assert "https://example.com/scholarship2" in body


class TestCreateIssue:
    """Tests for GitHub issue creation."""
    
    def test_create_issue_success(self):
        """Test successful issue creation."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {"number": 42, "html_url": "https://github.com/test/repo/issues/42"}
        mock_session.post.return_value = mock_response
        
        result = create_issue(
            session=mock_session,
            owner="testowner",
            repo="testrepo",
            title="Test Issue",
            body="Test Body"
        )
        
        assert result["number"] == 42
    
    def test_create_issue_auth_error(self):
        """Test handling of authentication error."""
        mock_session = Mock()
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.headers = {}
        mock_session.post.return_value = mock_response
        
        with pytest.raises(GitHubAPIError, match="authentication failed"):
            create_issue(
                session=mock_session,
                owner="owner",
                repo="repo",
                title="Test",
                body="Test"
            )


# =============================================================================
# Email Notification Tests
# =============================================================================


class TestEmailCredentials:
    """Tests for email credentials handling."""
    
    def test_get_email_credentials_success(self, email_env_vars):
        """Test successful retrieval of email credentials."""
        with patch.dict(os.environ, email_env_vars, clear=False):
            host, port, user, password, from_addr, to_addr = get_email_credentials()
            
            assert host == "smtp.example.com"
            assert port == 587
            assert user == "user@example.com"
            assert password == "securepassword123"
            assert from_addr == "noreply@example.com"
            assert to_addr == "recipient@example.com"
    
    def test_get_email_credentials_missing_host(self, email_env_vars):
        """Test error when SMTP_HOST is missing."""
        del email_env_vars["SMTP_HOST"]
        with patch.dict(os.environ, email_env_vars, clear=True):
            with pytest.raises(ValueError, match="SMTP_HOST"):
                get_email_credentials()
    
    def test_get_email_credentials_invalid_port(self, email_env_vars):
        """Test error when SMTP_PORT is not a valid integer."""
        email_env_vars["SMTP_PORT"] = "not_a_number"
        with patch.dict(os.environ, email_env_vars, clear=True):
            with pytest.raises(ValueError, match="SMTP_PORT must be a valid integer"):
                get_email_credentials()
    
    def test_get_email_credentials_all_required(self, email_env_vars):
        """Test that all required variables are checked."""
        required = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD", "EMAIL_FROM", "EMAIL_TO"]
        
        for var in required:
            env_copy = email_env_vars.copy()
            del env_copy[var]
            with patch.dict(os.environ, env_copy, clear=True):
                with pytest.raises(ValueError, match=var):
                    get_email_credentials()


class TestIsEmailConfigured:
    """Tests for email configuration detection."""
    
    def test_is_email_configured_all_set(self, email_env_vars):
        """Test returns True when all email vars are set."""
        with patch.dict(os.environ, email_env_vars, clear=False):
            assert is_email_configured() is True
    
    def test_is_email_configured_missing_var(self, email_env_vars):
        """Test returns False when any email var is missing."""
        del email_env_vars["SMTP_PASSWORD"]
        with patch.dict(os.environ, email_env_vars, clear=True):
            assert is_email_configured() is False
    
    def test_is_email_configured_empty_var(self, email_env_vars):
        """Test returns False when any email var is empty."""
        email_env_vars["EMAIL_TO"] = ""
        with patch.dict(os.environ, email_env_vars, clear=True):
            assert is_email_configured() is False
    
    def test_is_email_configured_none_set(self):
        """Test returns False when no email vars are set."""
        with patch.dict(os.environ, {}, clear=True):
            assert is_email_configured() is False


class TestFormatEmailBody:
    """Tests for email body formatting."""
    
    def test_format_email_body_html(self, sample_scholarships):
        """Test HTML body formatting includes all scholarships."""
        html = format_email_body_html(sample_scholarships)
        
        assert "<!DOCTYPE html>" in html
        assert "New Scholarships Detected" in html
        assert "Norwegian Tech Scholarship 2024" in html
        assert "Cloud Computing Grant - Norway" in html
        assert "https://example.com/scholarship1" in html
        assert "https://example.com/scholarship2" in html
        assert "2 new scholarships" in html
    
    def test_format_email_body_html_single(self, single_scholarship):
        """Test HTML body formatting for single scholarship."""
        html = format_email_body_html(single_scholarship)
        
        assert "1 new scholarship found" in html  # Singular
        assert "AI Research Fellowship" in html
    
    def test_format_email_body_html_escapes_special_chars(self):
        """Test HTML body escapes special characters in title."""
        scholarships = [{"title": "<script>alert('XSS')</script>", "url": "https://example.com"}]
        html = format_email_body_html(scholarships)
        
        assert "<script>" not in html
        assert "&lt;script&gt;" in html
    
    def test_format_email_body_plain(self, sample_scholarships):
        """Test plain text body formatting."""
        text = format_email_body_plain(sample_scholarships)
        
        assert "NEW SCHOLARSHIPS DETECTED" in text
        assert "Norwegian Tech Scholarship 2024" in text
        assert "Cloud Computing Grant - Norway" in text
        assert "https://example.com/scholarship1" in text
    
    def test_format_email_body_plain_includes_count(self, sample_scholarships):
        """Test plain text body includes scholarship count."""
        text = format_email_body_plain(sample_scholarships)
        
        assert "Number of New Scholarships: 2" in text


class TestSendEmailNotification:
    """Tests for email sending functionality."""
    
    def test_send_email_no_scholarships(self, email_env_vars):
        """Test returns True immediately for empty scholarships."""
        with patch.dict(os.environ, email_env_vars, clear=False):
            result = send_email_notification([])
            assert result is True
    
    def test_send_email_not_configured(self, sample_scholarships):
        """Test returns True when email not configured (optional feature)."""
        with patch.dict(os.environ, {}, clear=True):
            result = send_email_notification(sample_scholarships)
            assert result is True
    
    def test_send_email_dry_run(self, email_env_vars, sample_scholarships):
        """Test dry run doesn't actually send email."""
        with patch.dict(os.environ, email_env_vars, clear=False):
            with patch("src.notify.smtplib.SMTP") as mock_smtp:
                result = send_email_notification(sample_scholarships, dry_run=True)
                
                assert result is True
                mock_smtp.assert_not_called()
    
    def test_send_email_success(self, email_env_vars, sample_scholarships):
        """Test successful email sending with STARTTLS (port 587)."""
        with patch.dict(os.environ, email_env_vars, clear=False):
            mock_server = MagicMock()
            
            with patch("src.notify.smtplib.SMTP") as mock_smtp:
                mock_smtp.return_value.__enter__.return_value = mock_server
                
                result = send_email_notification(sample_scholarships)
                
                assert result is True
                mock_server.starttls.assert_called_once()
                mock_server.login.assert_called_once_with("user@example.com", "securepassword123")
                mock_server.send_message.assert_called_once()
    
    def test_send_email_success_port_465(self, email_env_vars, sample_scholarships):
        """Test successful email sending with SSL (port 465)."""
        email_env_vars["SMTP_PORT"] = "465"
        with patch.dict(os.environ, email_env_vars, clear=False):
            mock_server = MagicMock()
            
            with patch("src.notify.smtplib.SMTP_SSL") as mock_smtp_ssl:
                mock_smtp_ssl.return_value.__enter__.return_value = mock_server
                
                result = send_email_notification(sample_scholarships)
                
                assert result is True
                mock_server.login.assert_called_once_with("user@example.com", "securepassword123")
                mock_server.send_message.assert_called_once()
    
    def test_send_email_auth_error(self, email_env_vars, sample_scholarships):
        """Test handling of SMTP authentication error."""
        with patch.dict(os.environ, email_env_vars, clear=False):
            mock_server = MagicMock()
            mock_server.login.side_effect = smtplib.SMTPAuthenticationError(535, b"Authentication failed")
            
            with patch("src.notify.smtplib.SMTP") as mock_smtp:
                mock_smtp.return_value.__enter__.return_value = mock_server
                
                result = send_email_notification(sample_scholarships)
                
                assert result is False
    
    def test_send_email_connection_error(self, email_env_vars, sample_scholarships):
        """Test handling of SMTP connection error."""
        with patch.dict(os.environ, email_env_vars, clear=False):
            with patch("src.notify.smtplib.SMTP") as mock_smtp:
                mock_smtp.side_effect = smtplib.SMTPConnectError(421, "Connection refused")
                
                result = send_email_notification(sample_scholarships)
                
                assert result is False
    
    def test_send_email_ssl_error(self, email_env_vars, sample_scholarships):
        """Test handling of SSL/TLS error."""
        with patch.dict(os.environ, email_env_vars, clear=False):
            mock_server = MagicMock()
            mock_server.starttls.side_effect = ssl.SSLError("TLS handshake failed")
            
            with patch("src.notify.smtplib.SMTP") as mock_smtp:
                mock_smtp.return_value.__enter__.return_value = mock_server
                
                result = send_email_notification(sample_scholarships)
                
                assert result is False
    
    def test_send_email_timeout(self, email_env_vars, sample_scholarships):
        """Test handling of timeout error."""
        with patch.dict(os.environ, email_env_vars, clear=False):
            with patch("src.notify.smtplib.SMTP") as mock_smtp:
                mock_smtp.side_effect = TimeoutError("Connection timed out")
                
                result = send_email_notification(sample_scholarships)
                
                assert result is False
    
    def test_send_email_subject_format(self, email_env_vars, sample_scholarships):
        """Test email has correct subject line."""
        with patch.dict(os.environ, email_env_vars, clear=False):
            mock_server = MagicMock()
            sent_message = None
            
            def capture_message(msg):
                nonlocal sent_message
                sent_message = msg
            
            mock_server.send_message = capture_message
            
            with patch("src.notify.smtplib.SMTP") as mock_smtp:
                mock_smtp.return_value.__enter__.return_value = mock_server
                
                send_email_notification(sample_scholarships)
                
                assert sent_message is not None
                assert sent_message["Subject"] == "🎓 New Cloud & IT Scholarships in Norway – Daily Update"


class TestCheckEmailConnection:
    """Tests for email connection verification."""
    
    def test_check_email_connection_not_configured(self):
        """Test returns False when email not configured."""
        with patch.dict(os.environ, {}, clear=True):
            result = check_email_connection()
            assert result is False
    
    def test_check_email_connection_success(self, email_env_vars):
        """Test successful connection check."""
        with patch.dict(os.environ, email_env_vars, clear=False):
            mock_server = MagicMock()
            
            with patch("src.notify.smtplib.SMTP") as mock_smtp:
                mock_smtp.return_value.__enter__.return_value = mock_server
                
                result = check_email_connection()
                
                assert result is True
                mock_server.starttls.assert_called_once()
                mock_server.login.assert_called_once()
    
    def test_check_email_connection_failure(self, email_env_vars):
        """Test connection check failure."""
        with patch.dict(os.environ, email_env_vars, clear=False):
            with patch("src.notify.smtplib.SMTP") as mock_smtp:
                mock_smtp.side_effect = Exception("Connection failed")
                
                result = check_email_connection()
                
                assert result is False


# =============================================================================
# Integration-style Tests
# =============================================================================


class TestNotifyNewScholarships:
    """Tests for the main notification function."""
    
    def test_notify_empty_scholarships(self, github_env_vars):
        """Test returns None for empty scholarships list."""
        with patch.dict(os.environ, github_env_vars, clear=False):
            result = notify_new_scholarships([])
            assert result is None
    
    def test_notify_dry_run(self, github_env_vars, sample_scholarships):
        """Test dry run doesn't create actual issue."""
        with patch.dict(os.environ, github_env_vars, clear=False):
            with patch("src.notify.create_github_session") as mock_session:
                result = notify_new_scholarships(sample_scholarships, dry_run=True)
                
                assert result is None
                # Session should not be created in dry run
                mock_session.assert_not_called()

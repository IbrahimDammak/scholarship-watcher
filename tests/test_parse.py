"""
Tests for the parse module.

Tests cover:
- HTML parsing and scholarship extraction
- Relative URL normalization
- Title extraction strategies
- Handling of various HTML structures
- Edge cases and malformed HTML
"""

import pytest
from unittest.mock import Mock

from src.parse import (
    parse_html_content,
    parse_fetch_results,
    extract_title_from_element,
    extract_url_from_element,
    parse_with_selectors,
    parse_links_with_keywords,
)
from src.fetch import FetchResult


class TestParseHtmlContent:
    """Tests for HTML content parsing."""
    
    def test_parse_simple_scholarship_list(self):
        """Test parsing a simple scholarship listing page."""
        html = """
        <html>
        <body>
            <article class="scholarship">
                <h2><a href="/scholarships/norway-tech-2024">Norway Tech Scholarship 2024</a></h2>
            </article>
            <article class="scholarship">
                <h2><a href="/scholarships/oslo-grant">Oslo Research Grant</a></h2>
            </article>
        </body>
        </html>
        """
        
        result = parse_html_content(html, "https://example.com")
        
        assert len(result) == 2
        assert result[0]["title"] == "Norway Tech Scholarship 2024"
        assert result[0]["url"] == "https://example.com/scholarships/norway-tech-2024"
    
    def test_normalize_relative_urls(self):
        """Test that relative URLs are converted to absolute."""
        html = """
        <html>
        <body>
            <div class="scholarship-item">
                <h3><a href="/apply/scholarship-123">Test Scholarship</a></h3>
            </div>
        </body>
        </html>
        """
        
        result = parse_html_content(html, "https://scholarships.example.org")
        
        assert len(result) >= 1
        # URL should be absolute
        assert result[0]["url"].startswith("https://")
        assert "scholarships.example.org" in result[0]["url"]
    
    def test_normalize_relative_urls_with_parent_path(self):
        """Test normalization of relative URLs with parent paths."""
        html = """
        <html>
        <body>
            <div class="post">
                <h2><a href="../funding/grant-2024">Research Grant 2024</a></h2>
            </div>
        </body>
        </html>
        """
        
        result = parse_html_content(html, "https://example.com/scholarships/list/")
        
        assert len(result) >= 1
        # Should resolve to absolute URL
        assert result[0]["url"].startswith("https://example.com")
    
    def test_preserve_absolute_urls(self):
        """Test that absolute URLs are preserved unchanged."""
        html = """
        <html>
        <body>
            <article>
                <h2><a href="https://external.com/scholarship">External Scholarship</a></h2>
            </article>
        </body>
        </html>
        """
        
        result = parse_html_content(html, "https://example.com")
        
        assert len(result) >= 1
        assert result[0]["url"] == "https://external.com/scholarship"
    
    def test_extract_from_table_structure(self):
        """Test extraction from table-based layouts."""
        html = """
        <html>
        <body>
            <table class="scholarship-table">
                <tr>
                    <td><a href="/s1">Computer Science Fellowship</a></td>
                </tr>
                <tr>
                    <td><a href="/s2">Engineering Scholarship 2024</a></td>
                </tr>
            </table>
        </body>
        </html>
        """
        
        result = parse_html_content(html, "https://example.com")
        
        # Should find scholarships from table
        assert len(result) >= 1
    
    def test_skip_navigation_links(self):
        """Test that navigation and anchor links are skipped."""
        html = """
        <html>
        <body>
            <nav>
                <a href="#top">Back to top</a>
                <a href="javascript:void(0)">Menu</a>
            </nav>
            <article class="scholarship">
                <h2><a href="/scholarship/valid">Valid Scholarship</a></h2>
            </article>
        </body>
        </html>
        """
        
        result = parse_html_content(html, "https://example.com")
        
        # Should only include valid scholarship, not nav links
        urls = [s["url"] for s in result]
        assert not any("#" in url for url in urls)
        assert not any("javascript:" in url for url in urls)
    
    def test_empty_html(self):
        """Test handling of empty HTML content."""
        result = parse_html_content("", "https://example.com")
        assert result == []
    
    def test_malformed_html(self):
        """Test handling of malformed HTML."""
        html = """
        <html>
        <body>
            <div class="scholarship">
                <h2><a href="/test">Unclosed tag
            </div>
            <div class="scholarship">
                <h2><a href="/test2">Another scholarship</a></h2>
        </html>
        """
        
        # Should not raise exception
        result = parse_html_content(html, "https://example.com")
        # BeautifulSoup should handle malformed HTML gracefully
        assert isinstance(result, list)
    
    def test_no_scholarships_found(self):
        """Test handling when no scholarships are found."""
        html = """
        <html>
        <body>
            <p>This is just a regular page with no scholarships.</p>
        </body>
        </html>
        """
        
        result = parse_html_content(html, "https://example.com")
        assert result == []
    
    def test_deduplicate_scholarships(self):
        """Test that duplicate URLs are deduplicated."""
        html = """
        <html>
        <body>
            <div class="scholarship">
                <h2><a href="/scholarship/same">Scholarship A</a></h2>
            </div>
            <div class="scholarship">
                <h2><a href="/scholarship/same">Scholarship A Again</a></h2>
            </div>
        </body>
        </html>
        """
        
        result = parse_html_content(html, "https://example.com")
        
        # Should deduplicate by URL
        urls = [s["url"] for s in result]
        assert len(urls) == len(set(urls))


class TestExtractTitleFromElement:
    """Tests for title extraction from HTML elements."""
    
    def test_extract_from_h2(self):
        """Test extraction from h2 element."""
        from bs4 import BeautifulSoup
        
        html = """
        <article>
            <h2>Norway Scholarship Program</h2>
            <p>Description here</p>
        </article>
        """
        soup = BeautifulSoup(html, "html.parser")
        element = soup.find("article")
        
        title = extract_title_from_element(element)
        
        assert title == "Norway Scholarship Program"
    
    def test_extract_from_title_class(self):
        """Test extraction from element with title class."""
        from bs4 import BeautifulSoup
        
        html = """
        <div class="scholarship-item">
            <span class="title">Cloud Computing Grant</span>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        element = soup.find("div", class_="scholarship-item")
        
        title = extract_title_from_element(element)
        
        assert title == "Cloud Computing Grant"
    
    def test_skip_short_titles(self):
        """Test that very short titles are skipped."""
        from bs4 import BeautifulSoup
        
        html = """
        <div class="item">
            <h3>Hi</h3>
            <a href="/link">A Much Better Title Here</a>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        element = soup.find("div", class_="item")
        
        title = extract_title_from_element(element)
        
        # Should skip "Hi" and use the link text
        assert title is not None
        assert len(title) > 5


class TestExtractUrlFromElement:
    """Tests for URL extraction from HTML elements."""
    
    def test_extract_from_heading_link(self):
        """Test extraction from link inside heading."""
        from bs4 import BeautifulSoup
        
        html = """
        <article>
            <h2><a href="/scholarship/123">Scholarship Title</a></h2>
        </article>
        """
        soup = BeautifulSoup(html, "html.parser")
        element = soup.find("article")
        
        url = extract_url_from_element(element, "https://example.com")
        
        assert url == "https://example.com/scholarship/123"
    
    def test_prefer_scholarship_related_links(self):
        """Test preference for scholarship-related URLs."""
        from bs4 import BeautifulSoup
        
        html = """
        <div>
            <a href="/about">About</a>
            <a href="/scholarship/apply">Apply for Scholarship</a>
            <a href="/contact">Contact</a>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        element = soup.find("div")
        
        url = extract_url_from_element(element, "https://example.com")
        
        # Should prefer the scholarship-related link
        assert url is not None
        assert "scholarship" in url


class TestParseFetchResults:
    """Tests for parsing multiple fetch results."""
    
    def test_parse_multiple_results(self):
        """Test parsing scholarships from multiple fetch results."""
        results = [
            FetchResult(
                source_url="https://site1.com",
                html_content="""
                <article class="scholarship">
                    <h2><a href="/s1">Site 1 Scholarship</a></h2>
                </article>
                """,
                success=True
            ),
            FetchResult(
                source_url="https://site2.com",
                html_content="""
                <article class="scholarship">
                    <h2><a href="/s2">Site 2 Scholarship</a></h2>
                </article>
                """,
                success=True
            ),
        ]
        
        scholarships = parse_fetch_results(results)
        
        assert len(scholarships) >= 2
    
    def test_skip_failed_results(self):
        """Test that failed fetch results are skipped."""
        results = [
            FetchResult(
                source_url="https://working.com",
                html_content="""
                <article class="scholarship">
                    <h2><a href="/s1">Working Scholarship</a></h2>
                </article>
                """,
                success=True
            ),
            FetchResult(
                source_url="https://failed.com",
                html_content=None,
                success=False,
                error_message="Connection failed"
            ),
        ]
        
        scholarships = parse_fetch_results(results)
        
        # Only scholarships from successful fetches
        assert all("failed.com" not in s["url"] for s in scholarships)
    
    def test_deduplicate_across_sources(self):
        """Test deduplication of same URL from different sources."""
        results = [
            FetchResult(
                source_url="https://site1.com",
                html_content="""
                <article>
                    <h2><a href="https://shared.com/scholarship">Shared Scholarship</a></h2>
                </article>
                """,
                success=True
            ),
            FetchResult(
                source_url="https://site2.com",
                html_content="""
                <article>
                    <h2><a href="https://shared.com/scholarship">Shared Scholarship</a></h2>
                </article>
                """,
                success=True
            ),
        ]
        
        scholarships = parse_fetch_results(results)
        
        # Same URL should appear only once
        urls = [s["url"] for s in scholarships]
        assert len(urls) == len(set(urls))
    
    def test_empty_results(self):
        """Test handling of empty results list."""
        scholarships = parse_fetch_results([])
        assert scholarships == []


class TestParseLinksWithKeywords:
    """Tests for keyword-based link parsing."""
    
    def test_find_scholarship_links(self):
        """Test finding links containing scholarship keywords."""
        from bs4 import BeautifulSoup
        
        html = """
        <html>
        <body>
            <a href="/nav">Navigation</a>
            <a href="/scholarship-2024">Apply for Scholarship 2024 Now</a>
            <a href="/grant-program">Research Grant Program for Students</a>
        </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        
        result = parse_links_with_keywords(soup, "https://example.com")
        
        # Should find scholarship and grant links
        assert len(result) >= 2
        titles = [s["title"].lower() for s in result]
        assert any("scholarship" in t for t in titles)
        assert any("grant" in t for t in titles)

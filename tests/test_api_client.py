"""
Tests for the API client module.
"""

import pytest
import responses
import requests
from unittest.mock import Mock, patch
import time

from newsagger.api_client import LocApiClient


class TestLocApiClient:
    """Test cases for LocApiClient."""
    
    def test_init_default_values(self):
        """Test client initialization with default values."""
        client = LocApiClient()
        assert client.base_url == 'https://chroniclingamerica.loc.gov/'
        assert client.request_delay >= 3.0  # Enforces minimum delay
        assert client.max_retries == 3
        assert 'Newsagger' in client.session.headers['User-Agent']
    
    def test_init_custom_values(self):
        """Test client initialization with custom values."""
        client = LocApiClient(
            base_url='https://test.example.com/',
            request_delay=5.0,
            max_retries=5
        )
        assert client.base_url == 'https://test.example.com/'
        assert client.request_delay == 5.0
        assert client.max_retries == 5
    
    def test_enforces_minimum_delay(self):
        """Test that client enforces minimum 3 second delay."""
        client = LocApiClient(request_delay=1.0)
        assert client.request_delay == 3.0
    
    @responses.activate
    def test_make_request_success(self):
        """Test successful API request."""
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/test.json',
            json={'status': 'ok'},
            status=200
        )
        
        client = LocApiClient(request_delay=0.1)  # Fast for testing
        result = client._make_request('test.json')
        
        assert result == {'status': 'ok'}
        assert len(responses.calls) == 1
    
    @responses.activate
    def test_make_request_with_params(self):
        """Test API request with parameters."""
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/test.json',
            json={'data': 'test'},
            status=200
        )
        
        client = LocApiClient(request_delay=0.1)
        result = client._make_request('test.json', {'param': 'value'})
        
        assert result == {'data': 'test'}
        assert 'param=value' in responses.calls[0].request.url
    
    @responses.activate
    def test_rate_limit_handling(self):
        """Test 429 rate limit response handling."""
        # First request returns 429
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/test.json',
            status=429
        )
        # Second request succeeds
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/test.json',
            json={'status': 'ok'},
            status=200
        )
        
        client = LocApiClient(request_delay=0.1)
        
        # Mock time.sleep to avoid actual delays in tests
        with patch('time.sleep'):
            result = client._make_request('test.json')
        
        assert result == {'status': 'ok'}
        assert len(responses.calls) == 2
    
    @responses.activate
    def test_captcha_detection(self):
        """Test CAPTCHA detection in response."""
        # First request returns HTML with CAPTCHA
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/test.json',
            body='<html>Please solve this CAPTCHA</html>',
            status=200
        )
        # Second request succeeds
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/test.json',
            json={'status': 'ok'},
            status=200
        )
        
        client = LocApiClient(request_delay=0.1)
        
        with patch('time.sleep'):
            result = client._make_request('test.json')
        
        assert result == {'status': 'ok'}
        assert len(responses.calls) == 2
    
    @responses.activate
    def test_retry_on_error(self):
        """Test retry logic on network errors."""
        # First two requests fail
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/test.json',
            body=requests.exceptions.ConnectionError()
        )
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/test.json',
            body=requests.exceptions.ConnectionError()
        )
        # Third request succeeds
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/test.json',
            json={'status': 'ok'},
            status=200
        )
        
        client = LocApiClient(request_delay=0.1, max_retries=3)
        
        with patch('time.sleep'):
            result = client._make_request('test.json')
        
        assert result == {'status': 'ok'}
        assert len(responses.calls) == 3
    
    @responses.activate
    def test_max_retries_exceeded(self):
        """Test behavior when max retries exceeded."""
        # All requests fail
        for _ in range(4):
            responses.add(
                responses.GET,
                'https://chroniclingamerica.loc.gov/test.json',
                body=requests.exceptions.ConnectionError()
            )
        
        client = LocApiClient(request_delay=0.1, max_retries=3)
        
        with patch('time.sleep'):
            with pytest.raises(requests.exceptions.ConnectionError):
                client._make_request('test.json')
        
        assert len(responses.calls) == 3  # max_retries attempts
    
    @responses.activate
    def test_get_newspapers(self, sample_newspapers_response):
        """Test get_newspapers method."""
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/newspapers.json',
            json=sample_newspapers_response,
            status=200
        )
        
        client = LocApiClient(request_delay=0.1)
        result = client.get_newspapers(page=1, rows=50)
        
        assert result == sample_newspapers_response
        assert 'page=1' in responses.calls[0].request.url
        assert 'rows=50' in responses.calls[0].request.url
    
    @responses.activate
    def test_get_newspapers_max_rows(self):
        """Test that get_newspapers enforces max 1000 rows."""
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/newspapers.json',
            json={'newspapers': []},
            status=200
        )
        
        client = LocApiClient(request_delay=0.1)
        client.get_newspapers(rows=2000)  # Request more than max
        
        # Should be limited to 1000
        assert 'rows=1000' in responses.calls[0].request.url
    
    @responses.activate
    def test_search_pages(self):
        """Test search_pages method."""
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/search/pages/results/',
            json={'items': []},
            status=200
        )
        
        client = LocApiClient(request_delay=0.1)
        result = client.search_pages(
            andtext='earthquake',
            date1='1906',
            date2='1907',
            page=1,
            rows=100
        )
        
        assert result == {'items': [], 'results': []}
        url = responses.calls[0].request.url
        assert 'andtext=earthquake' in url
        assert 'date1=01%2F01%2F1906' in url  # Updated to match MM/DD/YYYY format
        assert 'date2=12%2F31%2F1907' in url  # Updated to match MM/DD/YYYY format
    
    @responses.activate
    def test_search_pages_with_facet(self):
        """Test search_pages with date facet."""
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/search/pages/results/',
            json={'items': []},
            status=200
        )
        
        client = LocApiClient(request_delay=0.1)
        client.search_pages(dates_facet='1906/1907')
        
        url = responses.calls[0].request.url
        assert 'dates=1906%2F1907' in url  # URL encoded
    
    def test_estimate_download_size(self):
        """Test download size estimation."""
        client = LocApiClient(request_delay=0.1)
        
        # Mock the search_pages method
        with patch.object(client, 'search_pages') as mock_search:
            mock_search.return_value = {'items': [{'id': f'item_{i}'} for i in range(100)]}
            
            estimate = client.estimate_download_size(('1900', '1910'), 'sn84038012')
        
        assert estimate['total_pages'] >= 100  # The estimation algorithm may multiply the sample
        assert estimate['estimated_size_gb'] > 0
        assert estimate['estimated_time_hours'] > 0
        assert estimate['date_range'] == '1900-1910'
        assert estimate['newspaper_lccn'] == 'sn84038012'
    
    @responses.activate
    def test_get_all_newspapers_pagination(self):
        """Test get_all_newspapers with pagination."""
        # First page
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/newspapers.json',
            json={
                'newspapers': [{'lccn': 'test1'}],
                'totalPages': 2
            },
            status=200
        )
        # Second page
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/newspapers.json',
            json={
                'newspapers': [{'lccn': 'test2'}],
                'totalPages': 2
            },
            status=200
        )
        # Third page (empty)
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/newspapers.json',
            json={
                'newspapers': [],
                'totalPages': 2
            },
            status=200
        )
        
        client = LocApiClient(request_delay=0.1)
        newspapers = list(client.get_all_newspapers())
        
        assert len(newspapers) == 2
        assert newspapers[0]['lccn'] == 'test1'
        assert newspapers[1]['lccn'] == 'test2'

    @responses.activate
    def test_search_pages_network_error(self):
        """Test search_pages with network error."""
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/search/pages/results/',
            body=requests.exceptions.ConnectionError("Network error")
        )
        
        client = LocApiClient(request_delay=0.1, max_retries=1)
        
        with pytest.raises(requests.exceptions.ConnectionError):
            client.search_pages(andtext='test')
    
    @responses.activate
    def test_search_pages_http_error(self):
        """Test search_pages with HTTP error."""
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/search/pages/results/',
            status=500
        )
        
        client = LocApiClient(request_delay=0.1, max_retries=1)
        
        with pytest.raises(requests.exceptions.HTTPError):
            client.search_pages(andtext='test')
    
    @responses.activate
    def test_get_newspaper_issues_error(self):
        """Test get_newspaper_issues with API error."""
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/lccn/invalid.json',
            status=404
        )
        
        client = LocApiClient(request_delay=0.1)
        
        with pytest.raises(requests.exceptions.HTTPError):
            client.get_newspaper_issues('invalid')
    
    @responses.activate
    def test_get_page_metadata_error(self):
        """Test get_page_metadata with invalid parameters."""
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/lccn/sn123/issues/1906-04-18/ed-1/seq-999.json',
            status=404
        )
        
        client = LocApiClient(request_delay=0.1)
        
        with pytest.raises(requests.exceptions.HTTPError):
            client.get_page_metadata('sn123', '1906-04-18', 1, 999)
    
    def test_format_search_date_edge_cases(self):
        """Test edge cases in date formatting."""
        client = LocApiClient(request_delay=0.1)
        
        # Test malformed date
        result = client._format_search_date('invalid-date')
        assert result == 'invalid-date'  # Should return as-is
        
        # Test empty string
        result = client._format_search_date('')
        assert result == ''
    
    @responses.activate
    def test_estimate_download_size_multiple_pages(self):
        """Test download size estimation with multiple API calls."""
        # Mock multiple page responses
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/search/pages/results/',
            json={'items': [{'id': f'item{i}'} for i in range(100)]},
            status=200
        )
        responses.add(
            responses.GET, 
            'https://chroniclingamerica.loc.gov/search/pages/results/',
            json={'items': [{'id': f'item{i}'} for i in range(100, 200)]},
            status=200
        )
        responses.add(
            responses.GET,
            'https://chroniclingamerica.loc.gov/search/pages/results/', 
            json={'items': [{'id': f'item{i}'} for i in range(200, 300)]},
            status=200
        )
        
        client = LocApiClient(request_delay=0.1)
        estimate = client.estimate_download_size(('1900', '1910'))
        
        # Should sample multiple pages and estimate
        assert estimate['total_pages'] >= 300
        assert estimate['estimated_size_gb'] > 0
        assert estimate['estimated_time_hours'] > 0
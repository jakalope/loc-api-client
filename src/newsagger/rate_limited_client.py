"""
Centralized Rate-Limited API Client

A singleton client that ensures all API requests respect LOC rate limits
by queuing requests through a single, centralized rate limiter.
"""

import time
import logging
import requests
import threading
from typing import Dict, List, Optional, Generator
from urllib.parse import urljoin
from datetime import datetime, timedelta
import json
from queue import Queue, Empty
import atexit


class RateLimitedRequestManager:
    """
    Singleton class that manages all API requests with centralized rate limiting.
    
    Ensures that no more than 18 requests per minute are made to respect
    LOC's 20 requests/minute limit with a safety buffer.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        """Singleton pattern implementation."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, base_url: str = "https://chroniclingamerica.loc.gov/", 
                 max_requests_per_minute: int = 18, max_retries: int = 3):
        # Only initialize once (singleton pattern)
        if hasattr(self, '_initialized'):
            return
            
        self.base_url = base_url.rstrip('/') + '/'
        self.max_requests_per_minute = max_requests_per_minute
        self.max_retries = max_retries
        
        # Calculate minimum delay between requests (with safety buffer)
        self.min_request_delay = 60.0 / max_requests_per_minute
        
        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Newsagger/0.1.0 (Educational Archive Tool - Rate Limited)'
        })
        
        # Rate limiting state
        self.last_request_time = 0.0
        self.request_count_window = []  # Track requests in current minute
        self.rate_limit_lock = threading.Lock()
        
        # Request queue for threading support
        self.request_queue = Queue()
        self.is_processing = False
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Initialized rate-limited client: max {max_requests_per_minute} req/min")
        
        # Ensure cleanup on exit
        atexit.register(self._cleanup)
        self._initialized = True
    
    def _cleanup(self):
        """Clean up resources on exit."""
        if hasattr(self, 'session'):
            self.session.close()
    
    def _wait_for_rate_limit(self):
        """
        Ensure we don't exceed rate limits by waiting if necessary.
        Uses both time-based and count-based limiting.
        """
        with self.rate_limit_lock:
            current_time = time.time()
            
            # Remove requests older than 1 minute from our tracking window
            cutoff_time = current_time - 60
            self.request_count_window = [
                req_time for req_time in self.request_count_window 
                if req_time > cutoff_time
            ]
            
            # Check if we would exceed the per-minute limit
            if len(self.request_count_window) >= self.max_requests_per_minute:
                # Wait until the oldest request in the window is > 1 minute old
                oldest_request = min(self.request_count_window)
                wait_time = 60 - (current_time - oldest_request) + 0.1  # Small buffer
                if wait_time > 0:
                    self.logger.info(f"Rate limiting: waiting {wait_time:.1f}s to respect {self.max_requests_per_minute}/min limit")
                    time.sleep(wait_time)
                    current_time = time.time()
            
            # Also ensure minimum delay between consecutive requests
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.min_request_delay:
                wait_time = self.min_request_delay - time_since_last
                self.logger.debug(f"Minimum delay: waiting {wait_time:.1f}s")
                time.sleep(wait_time)
                current_time = time.time()
            
            # Record this request
            self.last_request_time = current_time
            self.request_count_window.append(current_time)
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Make a rate-limited request to the API with retries and 429 handling.
        All requests go through this central bottleneck.
        """
        url = urljoin(self.base_url, endpoint)
        
        for attempt in range(self.max_retries):
            try:
                # Wait for rate limit before making request
                self._wait_for_rate_limit()
                
                self.logger.debug(f"Making request to {endpoint} (attempt {attempt + 1})")
                response = self.session.get(url, params=params, timeout=60)
                
                # Handle rate limiting (429) or CAPTCHA responses
                if response.status_code == 429:
                    # Calculate exponential backoff, but start with LOC's recommended 1 hour
                    backoff_time = 3600 * (2 ** attempt)  # 1h, 2h, 4h
                    self.logger.error(f"Rate limited (429) on attempt {attempt + 1}")
                    self.logger.error(f"LOC API requires {backoff_time/3600:.1f} hour wait")
                    
                    if attempt < self.max_retries - 1:
                        self.logger.warning(f"Waiting {backoff_time/60:.1f} minutes before retry...")
                        time.sleep(backoff_time)
                        continue
                    else:
                        raise requests.exceptions.RequestException(
                            f"Rate limited by LOC API (429) after {self.max_retries} attempts. "
                            f"Server requires {backoff_time/3600:.1f} hour wait."
                        )
                
                # Check for CAPTCHA in HTML response
                if 'captcha' in response.text.lower() or 'recaptcha' in response.text.lower():
                    self.logger.warning(f"CAPTCHA detected on attempt {attempt + 1}")
                    if attempt < self.max_retries - 1:
                        wait_time = 300 * (2 ** attempt)  # 5min, 10min, 20min
                        self.logger.warning(f"Waiting {wait_time/60:.1f} minutes before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise requests.exceptions.RequestException(
                            f"CAPTCHA detected by LOC API after {self.max_retries} attempts. "
                            f"Manual intervention required."
                        )
                
                response.raise_for_status()
                self.logger.debug(f"Request successful: {endpoint}")
                return response.json()
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt == self.max_retries - 1:
                    raise
                # Exponential backoff for other errors (network issues, etc.)
                wait_time = 30 * (2 ** attempt)  # 30s, 60s, 120s
                self.logger.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
    
    def get_request_stats(self) -> Dict:
        """Get statistics about request rate limiting."""
        with self.rate_limit_lock:
            current_time = time.time()
            # Count requests in last minute
            recent_requests = [
                req_time for req_time in self.request_count_window
                if req_time > current_time - 60
            ]
            
            return {
                'requests_last_minute': len(recent_requests),
                'max_requests_per_minute': self.max_requests_per_minute,
                'min_delay_seconds': self.min_request_delay,
                'last_request_time': self.last_request_time,
                'time_since_last_request': current_time - self.last_request_time if self.last_request_time else None
            }


class LocApiClient:
    """
    Enhanced LOC API Client that uses the centralized rate limiter.
    
    This provides the same interface as the original but routes all requests
    through the centralized rate-limited request manager.
    """
    
    def __init__(self, base_url: str = "https://chroniclingamerica.loc.gov/", 
                 request_delay: float = 3.0, max_retries: int = 3):
        # Get the singleton rate limiter
        self.rate_limiter = RateLimitedRequestManager(
            base_url=base_url,
            max_requests_per_minute=18,  # Conservative limit
            max_retries=max_retries
        )
        self.logger = logging.getLogger(__name__)
        
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Route all requests through the centralized rate limiter."""
        return self.rate_limiter._make_request(endpoint, params)
    
    def get_newspapers(self, page: int = 1, rows: int = 1000) -> Dict:
        """Get list of newspaper titles with up to 1000 per page."""
        # Limit to recommended max of 1000 items per page
        rows = min(rows, 1000)
        params = {
            'format': 'json',
            'page': page,
            'rows': rows
        }
        return self._make_request('newspapers.json', params)
    
    def get_all_newspapers(self) -> Generator[Dict, None, None]:
        """Generator to fetch all newspapers with pagination."""
        page = 1
        while True:
            data = self.get_newspapers(page=page)
            newspapers = data.get('newspapers', [])
            
            if not newspapers:
                break
                
            for newspaper in newspapers:
                yield newspaper
                
            page += 1
            
            # Check if we've reached the end
            if page > data.get('totalPages', 1):
                break
    
    def get_newspapers_with_details(self, max_newspapers: int = None) -> Generator[Dict, None, None]:
        """Get newspapers with additional details."""
        count = 0
        for newspaper in self.get_all_newspapers():
            if max_newspapers and count >= max_newspapers:
                break
            yield newspaper
            count += 1
    
    def get_newspaper_issues(self, lccn: str) -> Dict:
        """Get issues for a specific newspaper by LCCN."""
        endpoint = f'lccn/{lccn}.json'
        return self._make_request(endpoint)
    
    def search_pages(self, **params) -> Dict:
        """
        Search newspaper pages with various parameters.
        
        Common parameters:
        - andtext: Search text
        - date1, date2: Date range (YYYY-MM-DD)
        - page: Page number
        - rows: Results per page
        - sort: Sort order
        """
        # Add default format
        search_params = {'format': 'json'}
        search_params.update(params)
        
        # Convert date parameters to LOC format if needed
        if 'date1' in search_params:
            date1 = search_params['date1']
            if len(date1) == 4:  # Year only
                search_params['date1'] = f'01/01/{date1}'
        
        if 'date2' in search_params:
            date2 = search_params['date2']
            if len(date2) == 4:  # Year only
                search_params['date2'] = f'12/31/{date2}'
        
        return self._make_request('search/pages/results/', search_params)
    
    def estimate_download_size(self, date_range: tuple) -> Dict:
        """Estimate the number of pages available for a date range."""
        start_year, end_year = date_range
        
        # Use the search API to get total count
        params = {
            'date1': f'01/01/{start_year}',
            'date2': f'12/31/{end_year}',
            'rows': 1,  # Minimal results, we just want the total
            'page': 1
        }
        
        try:
            response = self.search_pages(**params)
            total_pages = response.get('totalItems', 0)
            
            return {
                'total_pages': total_pages,
                'estimated_size_mb': total_pages * 2,  # Rough estimate: 2MB per page
                'date_range': f"{start_year}-{end_year}"
            }
        except Exception as e:
            self.logger.warning(f"Failed to estimate size for {start_year}-{end_year}: {e}")
            return {
                'total_pages': 0,
                'estimated_size_mb': 0,
                'date_range': f"{start_year}-{end_year}",
                'error': str(e)
            }
    
    def get_request_stats(self) -> Dict:
        """Get rate limiting statistics."""
        return self.rate_limiter.get_request_stats()
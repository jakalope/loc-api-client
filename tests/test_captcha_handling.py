"""
Tests for CAPTCHA handling during batch discovery operations.
"""

import pytest
import tempfile
import time
from pathlib import Path
from unittest.mock import Mock, patch, call

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from newsagger.discovery_manager import DiscoveryManager
from newsagger.storage import NewsStorage
from newsagger.rate_limited_client import LocApiClient, CaptchaHandlingException, GlobalCaptchaManager
from newsagger.processor import NewsDataProcessor


class TestCaptchaHandling:
    """Test CAPTCHA handling in discovery operations."""
    
    def setup_method(self):
        """Set up test environment."""
        # Create temporary database
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.storage = NewsStorage(self.temp_db.name)
        
        # Create mock dependencies
        self.mock_api_client = Mock(spec=LocApiClient)
        self.mock_processor = Mock(spec=NewsDataProcessor)
        
        # Create discovery manager
        self.discovery = DiscoveryManager(
            self.mock_api_client,
            self.mock_processor,
            self.storage
        )
        
        # Reset global CAPTCHA state for clean tests
        global_captcha = GlobalCaptchaManager()
        global_captcha.reset_state()
    
    def teardown_method(self):
        """Clean up test environment."""
        Path(self.temp_db.name).unlink(missing_ok=True)
        
        # Reset global CAPTCHA state
        global_captcha = GlobalCaptchaManager()
        global_captcha.reset_state()
    
    def test_batch_discovery_handles_captcha_properly(self):
        """
        Test that batch discovery waits for cooling-off period when CAPTCHA is triggered,
        instead of just logging errors and continuing.
        """
        # Mock batches response
        mock_batches = [
            {
                'name': 'test_batch_1',
                'url': 'https://chroniclingamerica.loc.gov/batches/test_batch_1/',
                'page_count': 100
            }
        ]
        self.mock_api_client.get_all_batches.return_value = mock_batches
        
        # Mock batch details with issues
        mock_batch_details = {
            'issues': [
                {
                    'url': 'https://chroniclingamerica.loc.gov/lccn/sn83045201/1925-02-20/ed-1.json'
                },
                {
                    'url': 'https://chroniclingamerica.loc.gov/lccn/sn83045201/1925-02-27/ed-1.json'
                }
            ]
        }
        
        # Mock issue details with pages
        mock_issue_details = {
            'pages': [
                {'url': 'https://example.com/page1.json'},
                {'url': 'https://example.com/page2.json'}
            ]
        }
        
        # Setup a sequence where the first request succeeds, then CAPTCHA is triggered
        captcha_exception = CaptchaHandlingException(
            "CAPTCHA detected - global cooling-off period required",
            retry_strategy="global_cooling_off",
            suggested_params={'reason': 'Global cooling-off active: 60.0 minutes remaining'}
        )
        
        # Mock _make_request to return batch details first, then trigger CAPTCHA on issue request
        self.mock_api_client._make_request.side_effect = [
            mock_batch_details,  # First call for batch details
            captcha_exception,   # Second call for issue details triggers CAPTCHA
        ]
        
        # Mock processor to return empty pages (to avoid processing complexity)
        self.mock_processor.process_page_from_issue.return_value = None
        
        # Mock the GlobalCaptchaManager to simulate cooling-off behavior
        with patch('newsagger.batch_discovery.GlobalCaptchaManager') as mock_global_captcha_class:
            mock_global_captcha = Mock()
            mock_global_captcha_class.return_value = mock_global_captcha
            
            # Simulate that first check shows cooling-off is active, second check shows it's ready
            mock_global_captcha.can_make_requests.side_effect = [
                (False, "Global cooling-off active: 60.0 minutes remaining"),  # First check - still cooling off
                (True, "Cooling-off period completed")  # Second check - ready to proceed
            ]
            
            # Mock time.sleep to avoid actual waiting in tests
            with patch('time.sleep') as mock_sleep:
                # Run batch discovery - should handle CAPTCHA properly and wait
                result = self.discovery.discover_content_via_batches(
                    max_batches=1, 
                    auto_enqueue=False
                )
                
                # Verify that discovery attempted to wait for cooling-off period
                # The function should have called can_make_requests multiple times
                assert mock_global_captcha.can_make_requests.call_count >= 2
                
                # Verify that time.sleep was called (cooling-off wait)
                assert mock_sleep.call_count >= 1
                
                # The result should show that discovery was attempted despite CAPTCHA
                assert result['method'] == 'batch_discovery'
                assert result['errors'] == 0  # Should not count CAPTCHA as an error if handled properly
    
    def test_batch_discovery_without_captcha_handling_fails(self):
        """
        Test that demonstrates the current bug where CAPTCHA errors are not handled properly.
        This test should FAIL before the fix and PASS after the fix.
        """
        import logging
        from unittest.mock import Mock, patch
        
        # Mock batches response
        mock_batches = [
            {
                'name': 'test_batch_1', 
                'url': 'https://chroniclingamerica.loc.gov/batches/test_batch_1/',
                'page_count': 100
            }
        ]
        self.mock_api_client.get_all_batches.return_value = mock_batches
        
        # Mock batch details with issues
        mock_batch_details = {
            'issues': [
                {'url': 'https://chroniclingamerica.loc.gov/lccn/sn83045201/1925-02-20/ed-1.json'},
                {'url': 'https://chroniclingamerica.loc.gov/lccn/sn83045201/1925-02-27/ed-1.json'},
            ]
        }
        
        # Create CAPTCHA exception
        captcha_exception = CaptchaHandlingException(
            "CAPTCHA detected - global cooling-off period required",
            retry_strategy="global_cooling_off", 
            suggested_params={'reason': 'Global cooling-off active: 60.0 minutes remaining'}
        )
        
        # Mock _make_request to succeed for batch details, then trigger CAPTCHA on issue request
        self.mock_api_client._make_request.side_effect = [
            mock_batch_details,  # Batch details succeed
            captcha_exception,   # First issue request triggers CAPTCHA
            captcha_exception,   # Second issue request also triggers CAPTCHA (no wait)
        ]
        
        # Mock processor to avoid processing issues
        self.mock_processor.process_page_from_issue.return_value = None
        
        # Capture log messages to verify CAPTCHA is being caught as generic error
        import logging
        import io
        
        # Create a string buffer to capture log messages at WARNING level and above
        log_capture_string = io.StringIO()
        ch = logging.StreamHandler(log_capture_string)
        ch.setLevel(logging.WARNING)
        
        # Get the batch discovery logger and add our handler
        logger = logging.getLogger('newsagger.batch_discovery')
        logger.addHandler(ch)
        logger.setLevel(logging.WARNING)
        
        # Mock the GlobalCaptchaManager to simulate cooling-off behavior but avoid infinite wait
        with patch('newsagger.batch_discovery.GlobalCaptchaManager') as mock_global_captcha_class:
            mock_global_captcha = Mock()
            mock_global_captcha_class.return_value = mock_global_captcha
            
            # First call: cooling-off active, second call: ready to proceed
            mock_global_captcha.can_make_requests.side_effect = [
                (False, "Global cooling-off active: 60.0 minutes remaining"),
                (True, "Cooling-off period completed")
            ]
            
            # Mock time.sleep to avoid actual waiting in tests
            with patch('time.sleep') as mock_sleep:
                result = self.discovery.discover_content_via_batches(max_batches=1, auto_enqueue=False)
                
                # Verify that cooling-off wait was called
                assert mock_sleep.called, "time.sleep should have been called during cooling-off wait"
        
        # Get the captured log content
        log_contents = log_capture_string.getvalue()
        
        # After the fix: CAPTCHA should be logged as WARNING (proper handling) not ERROR (generic handler)
        captcha_warning_found = 'CAPTCHA detected while processing issue' in log_contents
        global_captcha_protection_found = 'Global CAPTCHA protection triggered' in log_contents
        
        assert captcha_warning_found, (
            "CAPTCHA should be logged as warning by proper handler. "
            f"Log contents: {log_contents}"
        )
        
        assert global_captcha_protection_found, (
            "Global CAPTCHA protection should be triggered and logged. "
            f"Log contents: {log_contents}"
        )
        
        # Verify that no pages were discovered due to CAPTCHA blocking everything
        assert result['discovered_pages'] == 0, f"Expected 0 discovered pages due to CAPTCHA, got {result['discovered_pages']}"
        
        # The batch processing should be stopped due to CAPTCHA, so no batches should be fully processed
        # This shows that CAPTCHA is properly handled instead of being ignored
        assert result['processed_batches'] == 0, f"Expected 0 processed batches due to CAPTCHA interruption, got {result['processed_batches']}"
    
    def test_captcha_cooling_off_simulation(self):
        """Test the cooling-off period simulation logic."""
        global_captcha = GlobalCaptchaManager()
        
        # Initially should be able to make requests
        can_proceed, reason = global_captcha.can_make_requests()
        assert can_proceed is True
        
        # Record a CAPTCHA
        global_captcha.record_captcha("test_endpoint")
        
        # Now should be blocked
        can_proceed, reason = global_captcha.can_make_requests()
        assert can_proceed is False
        assert "Global cooling-off active" in reason
        
        # Reset state
        global_captcha.reset_state()
        
        # Should be able to make requests again
        can_proceed, reason = global_captcha.can_make_requests()
        assert can_proceed is True
    
    def test_captcha_exception_properties(self):
        """Test that CaptchaHandlingException has the right properties."""
        exception = CaptchaHandlingException(
            "Test CAPTCHA message",
            retry_strategy="global_cooling_off",
            suggested_params={'reason': 'Test reason'}
        )
        
        assert str(exception) == "Test CAPTCHA message"
        assert exception.retry_strategy == "global_cooling_off"
        assert exception.suggested_params == {'reason': 'Test reason'}
"""
Tests for utility functions and decorators.
"""
import time
import logging
import pytest
from unittest.mock import Mock, patch
import requests

from src.newsagger.utils import (
    RetryConfig,
    retry_with_backoff,
    retry_on_request_failure,
    retry_on_network_failure
)


class TestRetryConfig:
    """Test RetryConfig class."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.base_delay == 1.0
        assert config.exponential_base == 2.0
        assert config.max_delay == 300.0
        assert config.retry_on == (requests.exceptions.RequestException,)
        assert isinstance(config.logger, logging.Logger)
    
    def test_custom_config(self):
        """Test custom configuration values."""
        logger = Mock()
        config = RetryConfig(
            max_attempts=5,
            base_delay=2.0,
            exponential_base=1.5,
            max_delay=600.0,
            retry_on=(ValueError, TypeError),
            logger=logger
        )
        assert config.max_attempts == 5
        assert config.base_delay == 2.0
        assert config.exponential_base == 1.5
        assert config.max_delay == 600.0
        assert config.retry_on == (ValueError, TypeError)
        assert config.logger is logger


class TestRetryWithBackoff:
    """Test retry_with_backoff decorator."""
    
    def test_successful_function_no_retry(self):
        """Test function that succeeds on first attempt."""
        mock_func = Mock(return_value="success")
        
        @retry_with_backoff(max_attempts=3)
        def test_func():
            return mock_func()
        
        result = test_func()
        assert result == "success"
        assert mock_func.call_count == 1
    
    def test_function_succeeds_after_retries(self):
        """Test function that succeeds after some failures."""
        mock_func = Mock()
        mock_func.side_effect = [
            requests.exceptions.RequestException("fail 1"),
            requests.exceptions.RequestException("fail 2"),
            "success"
        ]
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            @retry_with_backoff(max_attempts=3, base_delay=0.1)
            def test_func():
                return mock_func()
            
            result = test_func()
            assert result == "success"
            assert mock_func.call_count == 3
    
    def test_function_fails_all_attempts(self):
        """Test function that fails all retry attempts."""
        mock_func = Mock()
        mock_func.side_effect = requests.exceptions.RequestException("always fails")
        
        with patch('time.sleep'):  # Mock sleep to speed up test
            @retry_with_backoff(max_attempts=3, base_delay=0.1)
            def test_func():
                return mock_func()
            
            with pytest.raises(requests.exceptions.RequestException, match="always fails"):
                test_func()
            
            assert mock_func.call_count == 3
    
    def test_non_retryable_exception_immediate_failure(self):
        """Test that non-retryable exceptions are not retried."""
        mock_func = Mock()
        mock_func.side_effect = ValueError("not retryable")
        
        @retry_with_backoff(max_attempts=3, retry_on=(requests.exceptions.RequestException,))
        def test_func():
            return mock_func()
        
        with pytest.raises(ValueError, match="not retryable"):
            test_func()
        
        assert mock_func.call_count == 1  # No retries
    
    def test_exponential_backoff_delays(self):
        """Test that exponential backoff calculates correct delays."""
        mock_func = Mock()
        mock_func.side_effect = [
            requests.exceptions.RequestException("fail 1"),
            requests.exceptions.RequestException("fail 2"),
            "success"
        ]
        
        with patch('time.sleep') as mock_sleep:
            @retry_with_backoff(max_attempts=3, base_delay=2.0, exponential_base=2.0)
            def test_func():
                return mock_func()
            
            result = test_func()
            assert result == "success"
            
            # Check sleep was called with exponential delays: 2.0, 4.0
            expected_calls = [2.0, 4.0]
            actual_calls = [call[0][0] for call in mock_sleep.call_args_list]
            assert actual_calls == expected_calls
    
    def test_max_delay_cap(self):
        """Test that delays are capped at max_delay."""
        mock_func = Mock()
        mock_func.side_effect = [
            requests.exceptions.RequestException("fail 1"),
            requests.exceptions.RequestException("fail 2"),
            "success"
        ]
        
        with patch('time.sleep') as mock_sleep:
            @retry_with_backoff(
                max_attempts=3, 
                base_delay=100.0, 
                exponential_base=3.0,
                max_delay=150.0
            )
            def test_func():
                return mock_func()
            
            result = test_func()
            assert result == "success"
            
            # Check that delays are capped: 100.0, 150.0 (capped from 300.0)
            actual_calls = [call[0][0] for call in mock_sleep.call_args_list]
            assert actual_calls == [100.0, 150.0]
    
    def test_custom_config_object(self):
        """Test using custom RetryConfig object."""
        mock_func = Mock()
        mock_func.side_effect = [ValueError("fail"), "success"]
        
        config = RetryConfig(
            max_attempts=2,
            base_delay=0.5,
            retry_on=(ValueError,)
        )
        
        with patch('time.sleep'):
            @retry_with_backoff(config)
            def test_func():
                return mock_func()
            
            result = test_func()
            assert result == "success"
            assert mock_func.call_count == 2
    
    def test_preserves_function_metadata(self):
        """Test that decorator preserves original function metadata."""
        @retry_with_backoff(max_attempts=1)
        def example_function():
            """Example docstring."""
            return "test"
        
        assert example_function.__name__ == "example_function"
        assert example_function.__doc__ == "Example docstring."
    
    def test_function_with_arguments(self):
        """Test decorator works with functions that take arguments."""
        @retry_with_backoff(max_attempts=1)
        def test_func(a, b, c=None):
            return f"{a}-{b}-{c}"
        
        result = test_func("x", "y", c="z")
        assert result == "x-y-z"


class TestConvenienceDecorators:
    """Test convenience decorator functions."""
    
    def test_retry_on_request_failure(self):
        """Test retry_on_request_failure decorator."""
        mock_func = Mock()
        mock_func.side_effect = [
            requests.exceptions.RequestException("network error"),
            "success"
        ]
        
        with patch('time.sleep'):
            @retry_on_request_failure(max_attempts=2, base_delay=0.1)
            def test_func():
                return mock_func()
            
            result = test_func()
            assert result == "success"
            assert mock_func.call_count == 2
    
    def test_retry_on_network_failure(self):
        """Test retry_on_network_failure decorator."""
        mock_func = Mock()
        mock_func.side_effect = [
            requests.exceptions.ConnectionError("connection failed"),
            "success"
        ]
        
        with patch('time.sleep'):
            @retry_on_network_failure(max_attempts=2, base_delay=0.1)
            def test_func():
                return mock_func()
            
            result = test_func()
            assert result == "success"
            assert mock_func.call_count == 2
    
    def test_network_failure_different_exceptions(self):
        """Test that network failure decorator handles multiple exception types."""
        exceptions_to_test = [
            requests.exceptions.ConnectionError("connection"),
            requests.exceptions.Timeout("timeout"),
            requests.exceptions.ChunkedEncodingError("chunked")
        ]
        
        for exception in exceptions_to_test:
            mock_func = Mock()
            mock_func.side_effect = [exception, "success"]
            
            with patch('time.sleep'):
                @retry_on_network_failure(max_attempts=2, base_delay=0.1)
                def test_func():
                    return mock_func()
                
                result = test_func()
                assert result == "success"
                assert mock_func.call_count == 2
    
    def test_network_failure_ignores_other_request_exceptions(self):
        """Test that network failure decorator doesn't retry other request exceptions."""
        mock_func = Mock()
        mock_func.side_effect = requests.exceptions.HTTPError("HTTP 404")
        
        @retry_on_network_failure(max_attempts=3)
        def test_func():
            return mock_func()
        
        with pytest.raises(requests.exceptions.HTTPError):
            test_func()
        
        assert mock_func.call_count == 1  # No retries
    
    def test_logger_integration(self):
        """Test that custom logger is used for retry messages."""
        mock_logger = Mock()
        mock_func = Mock()
        mock_func.side_effect = [
            requests.exceptions.RequestException("fail"),
            "success"
        ]
        
        with patch('time.sleep'):
            @retry_on_request_failure(max_attempts=2, base_delay=0.1, logger=mock_logger)
            def test_func():
                return mock_func()
            
            result = test_func()
            assert result == "success"
            
            # Check that logger was called for warning
            assert mock_logger.warning.called
            warning_call = mock_logger.warning.call_args[0][0]
            assert "test_func failed" in warning_call
            assert "Retrying in" in warning_call
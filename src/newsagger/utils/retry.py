"""
Retry decorators and configuration for handling failures with exponential backoff.
"""
import time
import logging
import functools
from typing import Callable, Optional, Type, Tuple, Any
import requests


class RetryConfig:
    """Configuration for retry behavior."""
    
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        exponential_base: float = 2.0,
        max_delay: float = 300.0,
        retry_on: Tuple[Type[Exception], ...] = (requests.exceptions.RequestException,),
        logger: Optional[logging.Logger] = None
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.exponential_base = exponential_base
        self.max_delay = max_delay
        self.retry_on = retry_on
        self.logger = logger or logging.getLogger(__name__)


def retry_with_backoff(config: Optional[RetryConfig] = None, **config_kwargs) -> Callable:
    """
    Decorator that adds retry logic with exponential backoff to any function.
    
    Args:
        config: RetryConfig instance, or None to use config_kwargs
        **config_kwargs: Configuration parameters passed to RetryConfig if config is None
        
    Example:
        @retry_with_backoff(max_attempts=3, base_delay=1.0)
        def make_api_call():
            # Function that might fail
            pass
            
        # Or with custom config
        config = RetryConfig(max_attempts=5, exponential_base=1.5)
        @retry_with_backoff(config)
        def another_function():
            pass
    """
    if config is None:
        config = RetryConfig(**config_kwargs)
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(config.max_attempts):
                try:
                    return func(*args, **kwargs)
                    
                except config.retry_on as e:
                    last_exception = e
                    
                    if attempt == config.max_attempts - 1:
                        # Last attempt, don't wait and re-raise
                        config.logger.error(
                            f"Function {func.__name__} failed after {config.max_attempts} attempts: {e}"
                        )
                        raise
                    
                    # Calculate delay with exponential backoff
                    delay = min(
                        config.base_delay * (config.exponential_base ** attempt),
                        config.max_delay
                    )
                    
                    config.logger.warning(
                        f"Function {func.__name__} failed (attempt {attempt + 1}/{config.max_attempts}): {e}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    
                    time.sleep(delay)
                
                except Exception as e:
                    # Non-retryable exception, re-raise immediately
                    config.logger.error(f"Function {func.__name__} failed with non-retryable error: {e}")
                    raise
            
            # Should never reach here, but just in case
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator


def retry_on_request_failure(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    exponential_base: float = 2.0,
    max_delay: float = 300.0,
    logger: Optional[logging.Logger] = None
) -> Callable:
    """
    Convenience decorator specifically for HTTP request failures.
    
    This is a shorthand for retry_with_backoff configured for common request patterns.
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        base_delay=base_delay,
        exponential_base=exponential_base,
        max_delay=max_delay,
        retry_on=(requests.exceptions.RequestException,),
        logger=logger
    )
    return retry_with_backoff(config)


def retry_on_network_failure(
    max_attempts: int = 3,
    base_delay: float = 30.0,
    exponential_base: float = 2.0,
    max_delay: float = 300.0,
    logger: Optional[logging.Logger] = None
) -> Callable:
    """
    Convenience decorator for network-related failures with longer delays.
    
    Uses longer base delay (30s) suitable for network connectivity issues.
    """
    config = RetryConfig(
        max_attempts=max_attempts,
        base_delay=base_delay,
        exponential_base=exponential_base,
        max_delay=max_delay,
        retry_on=(
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ChunkedEncodingError,
        ),
        logger=logger
    )
    return retry_with_backoff(config)
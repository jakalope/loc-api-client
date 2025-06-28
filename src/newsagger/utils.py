"""
Utility functions and decorators for common patterns across the newsagger package.
"""
import time
import logging
import functools
import sqlite3
from typing import Callable, Optional, Union, Type, Tuple, Any, Dict, List
import requests
from tqdm import tqdm


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


class DatabaseOperationMixin:
    """
    Mixin class that provides common database operation patterns.
    
    Classes that inherit from this mixin should have a 'db_path' attribute
    that points to the SQLite database file.
    """
    
    def _build_dynamic_update(self, table_name: str, where_column: str, 
                            where_value: Any, include_timestamp: bool = True,
                            **updates) -> None:
        """
        Build and execute a dynamic UPDATE statement with optional fields.
        
        Args:
            table_name: Name of the table to update
            where_column: Column name for the WHERE clause
            where_value: Value for the WHERE clause
            include_timestamp: Whether to include updated_at = CURRENT_TIMESTAMP
            **updates: Field name -> value pairs to update (None values are ignored)
            
        Example:
            self._build_dynamic_update(
                'search_facets', 'id', facet_id,
                status='completed',
                items_discovered=100,
                error_message=None  # This will be ignored
            )
        """
        # Build the dynamic update list
        update_clauses = []
        params = []
        
        # Add timestamp if requested
        if include_timestamp:
            update_clauses.append("updated_at = CURRENT_TIMESTAMP")
        
        # Add all non-None updates
        for field_name, value in updates.items():
            if value is not None:
                if value == 'CURRENT_TIMESTAMP':
                    update_clauses.append(f"{field_name} = CURRENT_TIMESTAMP")
                else:
                    update_clauses.append(f"{field_name} = ?")
                    params.append(value)
        
        # Only proceed if we have something to update
        if not update_clauses:
            return
        
        # Add the WHERE parameter
        params.append(where_value)
        
        # Build and execute the query
        sql = f"""
            UPDATE {table_name} 
            SET {', '.join(update_clauses)}
            WHERE {where_column} = ?
        """
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(sql, params)
            conn.commit()
    
    def _build_conditional_update(self, table_name: str, where_column: str,
                                where_value: Any, updates: Dict[str, Any],
                                conditional_updates: Dict[str, Dict[str, Any]] = None) -> None:
        """
        Build and execute a dynamic UPDATE with conditional logic based on status changes.
        
        Args:
            table_name: Name of the table to update
            where_column: Column name for the WHERE clause  
            where_value: Value for the WHERE clause
            updates: Basic field updates {field_name: value}
            conditional_updates: Status-dependent updates {status_value: {field: value}}
            
        Example:
            self._build_conditional_update(
                'download_queue', 'id', queue_id,
                {'status': 'completed', 'progress_percent': 100},
                conditional_updates={
                    'active': {'started_at': 'CURRENT_TIMESTAMP'},
                    'completed': {'completed_at': 'CURRENT_TIMESTAMP'}
                }
            )
        """
        update_clauses = ["updated_at = CURRENT_TIMESTAMP"]
        params = []
        
        # Add basic updates
        for field_name, value in updates.items():
            if value is not None:
                if value == 'CURRENT_TIMESTAMP':
                    update_clauses.append(f"{field_name} = CURRENT_TIMESTAMP")
                else:
                    update_clauses.append(f"{field_name} = ?")
                    params.append(value)
        
        # Add conditional updates based on status
        if conditional_updates and 'status' in updates:
            status_value = updates['status']
            if status_value in conditional_updates:
                for field_name, value in conditional_updates[status_value].items():
                    if value == 'CURRENT_TIMESTAMP':
                        update_clauses.append(f"{field_name} = CURRENT_TIMESTAMP")
                    else:
                        update_clauses.append(f"{field_name} = ?")
                        params.append(value)
        
        # Only proceed if we have something to update beyond timestamp
        if len(update_clauses) <= 1:
            return
        
        # Add the WHERE parameter
        params.append(where_value)
        
        # Build and execute the query
        sql = f"""
            UPDATE {table_name} 
            SET {', '.join(update_clauses)}
            WHERE {where_column} = ?
        """
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(sql, params)
            conn.commit()


class ProgressTracker:
    """
    Context manager for standardized progress tracking with tqdm.
    
    Provides common patterns for progress bars with statistics tracking,
    error counting, and dynamic description updates.
    """
    
    def __init__(self, total: Optional[int] = None, desc: str = "Processing", 
                 unit: str = "item", show_rate: bool = True):
        """
        Initialize progress tracker.
        
        Args:
            total: Total number of items to process (None for unknown)
            desc: Initial description for the progress bar
            unit: Unit name for rate display (items/second)
            show_rate: Whether to show processing rate in postfix
        """
        self.total = total
        self.initial_desc = desc
        self.unit = unit
        self.show_rate = show_rate
        self._pbar = None
        
        # Statistics tracking
        self.stats = {
            'processed': 0,
            'success': 0,
            'errors': 0,
            'skipped': 0
        }
        self._start_time = None
    
    def __enter__(self):
        """Enter context manager and create tqdm progress bar."""
        self._start_time = time.time()
        self._pbar = tqdm(total=self.total, desc=self.initial_desc, unit=self.unit)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and close progress bar."""
        if self._pbar:
            self._pbar.close()
    
    def update(self, count: int = 1, success: bool = True, skipped: bool = False):
        """
        Update progress with automatic statistics tracking.
        
        Args:
            count: Number of items processed
            success: Whether the operation was successful
            skipped: Whether the items were skipped
        """
        if not self._pbar:
            return
        
        self.stats['processed'] += count
        
        if skipped:
            self.stats['skipped'] += count
        elif success:
            self.stats['success'] += count
        else:
            self.stats['errors'] += count
        
        self._pbar.update(count)
        self._update_postfix()
    
    def increment_error(self, count: int = 1):
        """Increment error count without updating progress."""
        self.stats['errors'] += count
        self._update_postfix()
    
    def set_description(self, desc: str):
        """Update the progress bar description."""
        if self._pbar:
            self._pbar.set_description(desc)
    
    def set_postfix(self, **kwargs):
        """Set custom postfix information."""
        if self._pbar:
            self._pbar.set_postfix(**kwargs)
    
    def _update_postfix(self):
        """Update postfix with current statistics."""
        if not self._pbar:
            return
        
        postfix = {
            'success': self.stats['success'],
            'errors': self.stats['errors']
        }
        
        if self.stats['skipped'] > 0:
            postfix['skipped'] = self.stats['skipped']
        
        if self.show_rate and self._start_time:
            elapsed = time.time() - self._start_time
            if elapsed > 0:
                rate = self.stats['processed'] / elapsed
                postfix['rate'] = f"{rate:.1f}/{self.unit}/s"
        
        self._pbar.set_postfix(**postfix)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        stats = self.stats.copy()
        if self._start_time:
            stats['elapsed_seconds'] = time.time() - self._start_time
        return stats
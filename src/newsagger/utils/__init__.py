"""
Utility functions and classes for common patterns across the newsagger package.

This module provides reusable components for:
- Retry handling with exponential backoff
- Database operation patterns
- Progress tracking with tqdm
"""

# Import all utilities for backward compatibility
from .retry import (
    RetryConfig,
    retry_with_backoff,
    retry_on_request_failure,
    retry_on_network_failure
)

from .database import DatabaseOperationMixin

from .progress import ProgressTracker

# Maintain backward compatibility by exposing all utilities at package level
__all__ = [
    'RetryConfig',
    'retry_with_backoff', 
    'retry_on_request_failure',
    'retry_on_network_failure',
    'DatabaseOperationMixin',
    'ProgressTracker'
]
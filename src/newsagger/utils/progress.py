"""
Progress tracking utilities for standardized progress bar patterns.
"""
import time
from typing import Optional, Dict, Any
from tqdm import tqdm


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
#!/usr/bin/env python3
"""
Test runner script for newsagger.
"""

import sys
import subprocess
from pathlib import Path

def run_tests():
    """Run the test suite with proper configuration."""
    # Add src to Python path
    src_path = Path(__file__).parent / 'src'
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    
    # Run pytest with coverage if available
    try:
        # Try to run with coverage
        result = subprocess.run([
            sys.executable, '-m', 'pytest',
            '--cov=newsagger',
            '--cov-report=html',
            '--cov-report=term-missing',
            'tests/'
        ], cwd=Path(__file__).parent)
    except FileNotFoundError:
        # Fall back to basic pytest
        result = subprocess.run([
            sys.executable, '-m', 'pytest',
            'tests/'
        ], cwd=Path(__file__).parent)
    
    return result.returncode

if __name__ == '__main__':
    sys.exit(run_tests())
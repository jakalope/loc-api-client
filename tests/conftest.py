"""
Pytest configuration and shared fixtures.
"""

import pytest
import tempfile
import sqlite3
from pathlib import Path
from unittest.mock import Mock

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from newsagger.config import Config
from newsagger.storage import NewsStorage
from newsagger.processor import NewsDataProcessor, NewspaperInfo, PageInfo


@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    yield db_path
    
    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.fixture
def storage(temp_db):
    """Create a NewsStorage instance with temporary database."""
    return NewsStorage(temp_db)


@pytest.fixture
def processor():
    """Create a NewsDataProcessor instance."""
    return NewsDataProcessor()


@pytest.fixture
def test_config():
    """Create a test configuration."""
    config = Config()
    config.database_path = ':memory:'
    config.request_delay = 0.1  # Fast for testing
    config.log_level = 'WARNING'  # Reduce noise
    return config


@pytest.fixture
def sample_newspaper_data():
    """Sample newspaper API response data."""
    return {
        'lccn': 'sn84038012',
        'title': 'The San Francisco Call',
        'place_of_publication': ['San Francisco, Calif.'],
        'start_year': '1895',
        'end_year': '1913',
        'frequency': 'Daily',
        'subject': ['San Francisco (Calif.)--Newspapers'],
        'language': ['English'],
        'url': 'https://chroniclingamerica.loc.gov/lccn/sn84038012/'
    }


@pytest.fixture
def sample_page_data():
    """Sample page search result data."""
    return {
        'id': 'item123',
        'lccn': 'sn84038012',
        'title': 'The San Francisco Call',
        'date': '1906-04-18',
        'edition': 1,
        'sequence': 1,
        'url': 'https://chroniclingamerica.loc.gov/lccn/sn84038012/1906-04-18/ed-1/seq-1/',
        'pdf_url': 'https://chroniclingamerica.loc.gov/lccn/sn84038012/1906-04-18/ed-1/seq-1.pdf',
        'image_url': ['https://chroniclingamerica.loc.gov/lccn/sn84038012/1906-04-18/ed-1/seq-1.jp2']
    }


@pytest.fixture
def sample_newspapers_response(sample_newspaper_data):
    """Sample newspapers API response."""
    return {
        'newspapers': [sample_newspaper_data],
        'totalItems': 1,
        'totalPages': 1
    }


@pytest.fixture
def sample_search_response(sample_page_data):
    """Sample search API response."""
    return {
        'items': [sample_page_data],
        'totalItems': 1,
        'pagination': {'current': 1, 'total': 1}
    }


@pytest.fixture
def mock_requests():
    """Mock requests session for API testing."""
    return Mock()
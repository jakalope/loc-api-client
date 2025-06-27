"""
Tests for automated discovery functionality.
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from newsagger.discovery import DiscoveryManager
from newsagger.storage import NewsStorage
from newsagger.api_client import LocApiClient
from newsagger.processor import NewsDataProcessor


class TestDiscoveryAutomation:
    """Test automated discovery functionality."""
    
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
    
    def teardown_method(self):
        """Clean up test environment."""
        Path(self.temp_db.name).unlink(missing_ok=True)
    
    def test_discover_facet_content_date_range(self):
        """Test discovering content for a date range facet."""
        # Create a test facet
        facet_id = self.storage.create_search_facet(
            'date_range', '1906/1906', '', 1000
        )
        
        # Mock API responses
        mock_pages = [
            {
                'id': 'item1',
                'lccn': 'sn84038012',
                'title': 'Test Paper',
                'date': '1906-04-18',
                'edition': 1,
                'sequence': 1,
                'url': 'https://example.com/item1'
            },
            {
                'id': 'item2', 
                'lccn': 'sn84038012',
                'title': 'Test Paper',
                'date': '1906-04-19',
                'edition': 1,
                'sequence': 1,
                'url': 'https://example.com/item2'
            }
        ]
        
        self.mock_api_client.search_pages.return_value = {
            'items': mock_pages,
            'totalItems': 2
        }
        from newsagger.processor import PageInfo
        self.mock_processor.process_search_response.return_value = [
            PageInfo(
                item_id='item1',
                lccn='sn84038012',
                title='Test Paper',
                date='1906-04-18',
                edition=1,
                sequence=1,
                page_url='https://example.com/item1',
                pdf_url=None,
                jp2_url=None,
                ocr_text=None,
                word_count=None
            ),
            PageInfo(
                item_id='item2',
                lccn='sn84038012',
                title='Test Paper',
                date='1906-04-19',
                edition=1,
                sequence=1,
                page_url='https://example.com/item2',
                pdf_url=None,
                jp2_url=None,
                ocr_text=None,
                word_count=None
            )
        ]
        
        # Test discovery
        discovered_count = self.discovery.discover_facet_content(facet_id, batch_size=50)
        
        # Verify results
        assert discovered_count == 2
        
        # Check API was called with correct parameters
        self.mock_api_client.search_pages.assert_called_once()
        call_args = self.mock_api_client.search_pages.call_args[1]
        assert call_args['dates'] == '1906/1906'
        assert call_args['c'] == 50
        assert call_args['sp'] == 1
        
        # Check facet status was updated
        facet = self.storage.get_search_facet(facet_id)
        assert facet['status'] == 'completed'
        assert facet['items_discovered'] == 2
        assert facet['actual_items'] == 2
    
    def test_discover_facet_content_state_facet(self):
        """Test discovering content for a state facet."""
        # Create a test facet
        facet_id = self.storage.create_search_facet(
            'state', 'California', '', 500
        )
        
        # Mock empty response (no pages found)
        self.mock_api_client.search_pages.return_value = {
            'items': [],
            'totalItems': 0
        }
        self.mock_processor.process_search_response.return_value = []
        
        # Test discovery
        discovered_count = self.discovery.discover_facet_content(facet_id)
        
        # Verify results
        assert discovered_count == 0
        
        # Check API was called with state parameter
        self.mock_api_client.search_pages.assert_called_once()
        call_args = self.mock_api_client.search_pages.call_args[1]
        assert call_args['state'] == 'California'
    
    def test_discover_facet_content_with_max_items(self):
        """Test discovery with max items limit."""
        # Create a test facet
        facet_id = self.storage.create_search_facet(
            'date_range', '1906/1906', '', 1000
        )
        
        # Mock API response with many items
        mock_pages = [
            {
                'id': f'item{i}',
                'lccn': 'sn84038012',
                'title': 'Test Paper',
                'date': '1906-04-18',
                'url': f'https://example.com/item{i}'
            }
            for i in range(20)
        ]
        
        self.mock_api_client.search_pages.return_value = {
            'items': mock_pages,
            'totalItems': 20
        }
        from newsagger.processor import PageInfo
        self.mock_processor.process_search_response.return_value = [
            PageInfo(
                item_id=f'item{i}',
                lccn='sn84038012',
                title='Test Paper',
                date='1906-04-18',
                edition=1,
                sequence=i,
                page_url=f'https://example.com/item{i}',
                pdf_url=None,
                jp2_url=None,
                ocr_text=None,
                word_count=None
            )
            for i in range(20)
        ]
        
        # Test discovery with limit
        discovered_count = self.discovery.discover_facet_content(
            facet_id, max_items=10
        )
        
        # Should stop at max_items limit
        assert discovered_count == 10
        
        # Check facet status
        facet = self.storage.get_search_facet(facet_id)
        assert facet['status'] == 'completed'
        assert facet['items_discovered'] == 10
    
    def test_discover_facet_content_error_handling(self):
        """Test error handling during discovery."""
        # Create a test facet
        facet_id = self.storage.create_search_facet(
            'date_range', '1906/1906', '', 1000
        )
        
        # Mock API error
        self.mock_api_client.search_pages.side_effect = Exception("API Error")
        
        # Test discovery should raise exception
        with pytest.raises(Exception, match="API Error"):
            self.discovery.discover_facet_content(facet_id)
        
        # Check facet status was set to error
        facet = self.storage.get_search_facet(facet_id)
        assert facet['status'] == 'error'
        assert 'API Error' in facet['error_message']
    
    def test_discover_facet_content_invalid_facet(self):
        """Test discovery with invalid facet ID."""
        with pytest.raises(ValueError, match="Facet 999 not found"):
            self.discovery.discover_facet_content(999)
    
    def test_enqueue_facet_content(self):
        """Test enqueuing discovered content from a facet."""
        # Create a test facet
        facet_id = self.storage.create_search_facet(
            'date_range', '1906/1906', '', 1000
        )
        
        # Add some test pages to storage
        from newsagger.processor import PageInfo
        pages = [
            PageInfo(
                item_id=f'item{i}',
                lccn='sn84038012',
                title='Test Paper',
                date='1906-04-18',
                edition=1,
                sequence=i,
                page_url=f'https://example.com/item{i}',
                pdf_url=None,
                jp2_url=None,
                ocr_text=None,
                word_count=None
            )
            for i in range(1, 6)
        ]
        
        self.storage.store_pages(pages)
        
        # Test enqueuing
        enqueued_count = self.discovery.enqueue_facet_content(facet_id)
        
        # Should enqueue all pages (5)
        assert enqueued_count == 5
        
        # Check download queue
        queue = self.storage.get_download_queue()
        assert len(queue) == 5
        
        for item in queue:
            assert item['queue_type'] == 'page'
            assert item['reference_id'].startswith('item')
            assert item['priority'] == 1  # 1906 gets highest priority
            assert item['estimated_size_mb'] == 1.0
    
    def test_enqueue_facet_content_with_limit(self):
        """Test enqueuing with max items limit."""
        # Create a test facet
        facet_id = self.storage.create_search_facet(
            'date_range', '1920/1920', '', 1000
        )
        
        # Add some test pages
        from newsagger.processor import PageInfo
        pages = [
            PageInfo(
                item_id=f'item{i}',
                lccn='sn84038012',
                title='Test Paper',
                date='1920-01-01',
                edition=1,
                sequence=i,
                page_url=f'https://example.com/item{i}',
                pdf_url=None,
                jp2_url=None,
                ocr_text=None,
                word_count=None
            )
            for i in range(1, 11)
        ]
        
        self.storage.store_pages(pages)
        
        # Test enqueuing with limit
        enqueued_count = self.discovery.enqueue_facet_content(facet_id, max_items=3)
        
        # Should only enqueue 3 items
        assert enqueued_count == 3
        
        # Check download queue
        queue = self.storage.get_download_queue()
        assert len(queue) == 3
    
    def test_enqueue_facet_content_invalid_facet(self):
        """Test enqueuing with invalid facet ID."""
        with pytest.raises(ValueError, match="Facet 999 not found"):
            self.discovery.enqueue_facet_content(999)
    
    def test_calculate_priority(self):
        """Test priority calculation for different facets and pages."""
        # Test 1906 earthquake year gets priority 1
        facet_1906 = {
            'facet_type': 'date_range',
            'facet_value': '1906/1906'
        }
        page = {'date': '1906-04-18'}
        priority = self.discovery._calculate_priority(facet_1906, page)
        assert priority == 1
        
        # Test WWI era gets priority 2
        facet_wwi = {
            'facet_type': 'date_range',
            'facet_value': '1918/1918'
        }
        priority = self.discovery._calculate_priority(facet_wwi, page)
        assert priority == 2
        
        # Test California state gets higher priority
        facet_ca = {
            'facet_type': 'state',
            'facet_value': 'California'
        }
        priority = self.discovery._calculate_priority(facet_ca, page)
        assert priority == 4  # max(1, 5-1)
        
        # Test default priority
        facet_default = {
            'facet_type': 'date_range',
            'facet_value': '1950/1950'
        }
        priority = self.discovery._calculate_priority(facet_default, page)
        assert priority == 5
    
    def test_discover_facet_content_pagination(self):
        """Test discovery handles pagination correctly."""
        # Create a test facet
        facet_id = self.storage.create_search_facet(
            'date_range', '1906/1906', '', 1000
        )
        
        # Mock API responses for multiple pages
        page1_response = {
            'items': [{'id': f'item{i}', 'title': 'Test'} for i in range(1, 101)],
            'totalItems': 150
        }
        page2_response = {
            'items': [{'id': f'item{i}', 'title': 'Test'} for i in range(101, 151)],
            'totalItems': 150
        }
        
        self.mock_api_client.search_pages.side_effect = [page1_response, page2_response]
        from newsagger.processor import PageInfo
        self.mock_processor.process_search_response.side_effect = [
            [PageInfo(
                item_id=f'item{i}',
                lccn='sn84038012',
                title='Test Paper',
                date='1906-04-18',
                edition=1,
                sequence=i,
                page_url=f'https://example.com/item{i}',
                pdf_url=None,
                jp2_url=None,
                ocr_text=None,
                word_count=None
            ) for i in range(1, 101)],
            [PageInfo(
                item_id=f'item{i}',
                lccn='sn84038012',
                title='Test Paper',
                date='1906-04-18',
                edition=1,
                sequence=i,
                page_url=f'https://example.com/item{i}',
                pdf_url=None,
                jp2_url=None,
                ocr_text=None,
                word_count=None
            ) for i in range(101, 151)]
        ]
        
        # Test discovery
        discovered_count = self.discovery.discover_facet_content(facet_id, batch_size=100)
        
        # Should discover all items across pages
        assert discovered_count == 150
        
        # Should have made 2 API calls
        assert self.mock_api_client.search_pages.call_count == 2
        
        # Check pagination parameters
        calls = self.mock_api_client.search_pages.call_args_list
        assert calls[0][1]['sp'] == 1  # First page
        assert calls[1][1]['sp'] == 2  # Second page
    
    def test_discover_facet_content_query_facet(self):
        """Test discovering content for a query-based facet."""
        # Create a test facet with query
        facet_id = self.storage.create_search_facet(
            'query', 'earthquake', 'earthquake', 500
        )
        
        # Mock API response
        self.mock_api_client.search_pages.return_value = {
            'items': [{'id': 'item1', 'title': 'Earthquake News'}],
            'totalItems': 1
        }
        from newsagger.processor import PageInfo
        self.mock_processor.process_search_response.return_value = [
            PageInfo(
                item_id='item1',
                lccn='sn84038012',
                title='Earthquake News',
                date='1906-04-18',
                edition=1,
                sequence=1,
                page_url='https://example.com/item1',
                pdf_url=None,
                jp2_url=None,
                ocr_text=None,
                word_count=None
            )
        ]
        
        # Test discovery
        discovered_count = self.discovery.discover_facet_content(facet_id)
        
        # Verify query parameter was used
        call_args = self.mock_api_client.search_pages.call_args[1]
        assert call_args['q'] == 'earthquake'
        assert discovered_count == 1
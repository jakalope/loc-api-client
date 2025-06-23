"""
Tests for the data processor module.
"""

import pytest
from datetime import datetime

from newsagger.processor import NewsDataProcessor, NewspaperInfo, PageInfo


class TestNewspaperInfo:
    """Test cases for NewspaperInfo dataclass."""
    
    def test_from_api_response(self, sample_newspaper_data):
        """Test creating NewspaperInfo from API response."""
        newspaper = NewspaperInfo.from_api_response(sample_newspaper_data)
        
        assert newspaper.lccn == 'sn84038012'
        assert newspaper.title == 'The San Francisco Call'
        assert newspaper.place_of_publication == ['San Francisco, Calif.']
        assert newspaper.start_year == 1895
        assert newspaper.end_year == 1913
        assert newspaper.frequency == 'Daily'
        assert newspaper.subject == ['San Francisco (Calif.)--Newspapers']
        assert newspaper.language == ['English']
        assert newspaper.url == 'https://chroniclingamerica.loc.gov/lccn/sn84038012/'
    
    def test_from_api_response_minimal(self):
        """Test creating NewspaperInfo with minimal data."""
        minimal_data = {
            'lccn': 'test123',
            'title': 'Test Paper'
        }
        
        newspaper = NewspaperInfo.from_api_response(minimal_data)
        
        assert newspaper.lccn == 'test123'
        assert newspaper.title == 'Test Paper'
        assert newspaper.place_of_publication == []
        assert newspaper.start_year is None
        assert newspaper.end_year is None
        assert newspaper.frequency is None
        assert newspaper.subject == []
        assert newspaper.language == []
        assert newspaper.url == ''
    
    def test_parse_year_valid(self):
        """Test year parsing with valid years."""
        assert NewspaperInfo._parse_year('1900') == 1900
        assert NewspaperInfo._parse_year('From 1895 to 1913') == 1895
        assert NewspaperInfo._parse_year('Published in 2020') == 2020
    
    def test_parse_year_invalid(self):
        """Test year parsing with invalid data."""
        assert NewspaperInfo._parse_year(None) is None
        assert NewspaperInfo._parse_year('') is None
        assert NewspaperInfo._parse_year('No year here') is None
        assert NewspaperInfo._parse_year('123') is None  # Too short


class TestPageInfo:
    """Test cases for PageInfo dataclass."""
    
    def test_from_search_result(self, sample_page_data):
        """Test creating PageInfo from search result."""
        page = PageInfo.from_search_result(sample_page_data)
        
        assert page.item_id == 'item123'
        assert page.lccn == 'sn84038012'
        assert page.title == 'The San Francisco Call'
        assert page.date == '1906-04-18'
        assert page.edition == 1
        assert page.sequence == 1
        assert page.page_url == 'https://chroniclingamerica.loc.gov/lccn/sn84038012/1906-04-18/ed-1/seq-1/'
        assert page.pdf_url == 'https://chroniclingamerica.loc.gov/lccn/sn84038012/1906-04-18/ed-1/seq-1.pdf'
        assert 'seq-1.jp2' in page.jp2_url
        assert page.ocr_text is None
        assert page.word_count is None
    
    def test_from_search_result_minimal(self):
        """Test creating PageInfo with minimal data."""
        minimal_data = {
            'lccn': 'test123',
            'title': 'Test Paper',
            'date': '1900-01-01'
        }
        
        page = PageInfo.from_search_result(minimal_data)
        
        assert page.lccn == 'test123'
        assert page.title == 'Test Paper'
        assert page.date == '1900-01-01'
        assert page.edition == 1  # Default
        assert page.sequence == 1  # Default
        assert page.item_id == 'test123_1900-01-01_1'  # Fallback ID generated
        assert page.pdf_url is None
        assert page.jp2_url is None
    
    def test_item_id_extraction_from_url(self):
        """Test item ID extraction from URL when no explicit ID."""
        data = {
            'url': 'https://chroniclingamerica.loc.gov/lccn/sn84038012/1906-04-18/ed-1/seq-1/',
            'title': 'Test'
        }
        
        page = PageInfo.from_search_result(data)
        assert page.item_id == 'seq-1'


class TestNewsDataProcessor:
    """Test cases for NewsDataProcessor."""
    
    def test_init(self):
        """Test processor initialization."""
        processor = NewsDataProcessor()
        assert len(processor._seen_items) == 0
    
    def test_process_newspapers_response(self, processor, sample_newspapers_response):
        """Test processing newspapers API response."""
        newspapers = processor.process_newspapers_response(sample_newspapers_response)
        
        assert len(newspapers) == 1
        assert isinstance(newspapers[0], NewspaperInfo)
        assert newspapers[0].lccn == 'sn84038012'
        assert newspapers[0].title == 'The San Francisco Call'
    
    def test_process_newspapers_response_empty(self, processor):
        """Test processing empty newspapers response."""
        empty_response = {'newspapers': []}
        newspapers = processor.process_newspapers_response(empty_response)
        
        assert len(newspapers) == 0
    
    def test_process_newspapers_response_invalid_data(self, processor, caplog):
        """Test processing newspapers with invalid data."""
        invalid_response = {
            'newspapers': [
                {'lccn': 'valid123', 'title': 'Valid Paper'},
                {'invalid': 'data'},  # Missing required fields
                {'lccn': 'valid456', 'title': 'Another Valid Paper'}
            ]
        }
        
        newspapers = processor.process_newspapers_response(invalid_response)
        
        # Should process all entries, even with missing fields (graceful handling)
        assert len(newspapers) == 3
        # Valid entries should have their data
        assert newspapers[0].lccn == 'valid123'
        assert newspapers[2].lccn == 'valid456'
        # Invalid entry gets empty/default values
        assert newspapers[1].lccn == ''
    
    def test_process_search_response(self, processor, sample_search_response):
        """Test processing search results response."""
        pages = processor.process_search_response(sample_search_response)
        
        assert len(pages) == 1
        assert isinstance(pages[0], PageInfo)
        assert pages[0].item_id == 'item123'
        assert pages[0].lccn == 'sn84038012'
    
    def test_process_search_response_deduplication(self, processor):
        """Test deduplication in search response processing."""
        response_with_duplicates = {
            'results': [
                {'id': 'item1', 'title': 'Paper 1', 'lccn': 'test1'},
                {'id': 'item2', 'title': 'Paper 2', 'lccn': 'test2'},
                {'id': 'item1', 'title': 'Paper 1 Duplicate', 'lccn': 'test1'},  # Duplicate
                {'id': 'item3', 'title': 'Paper 3', 'lccn': 'test3'}
            ]
        }
        
        pages = processor.process_search_response(response_with_duplicates, deduplicate=True)
        
        # Should only have 3 unique items
        assert len(pages) == 3
        item_ids = [page.item_id for page in pages]
        assert 'item1' in item_ids
        assert 'item2' in item_ids
        assert 'item3' in item_ids
        assert item_ids.count('item1') == 1  # No duplicates
    
    def test_process_search_response_no_deduplication(self, processor):
        """Test processing without deduplication."""
        response_with_duplicates = {
            'results': [
                {'id': 'item1', 'title': 'Paper 1', 'lccn': 'test1'},
                {'id': 'item1', 'title': 'Paper 1 Duplicate', 'lccn': 'test1'}
            ]
        }
        
        pages = processor.process_search_response(response_with_duplicates, deduplicate=False)
        
        # Should have both items including duplicate
        assert len(pages) == 2
    
    def test_reset_deduplication(self, processor):
        """Test resetting deduplication cache."""
        # Process some items to populate the cache
        response = {
            'results': [
                {'id': 'item1', 'title': 'Paper 1', 'lccn': 'test1'},
                {'id': 'item2', 'title': 'Paper 2', 'lccn': 'test2'}
            ]
        }
        processor.process_search_response(response, deduplicate=True)
        
        assert len(processor._seen_items) == 2
        
        # Reset cache
        processor.reset_deduplication()
        assert len(processor._seen_items) == 0
        
        # Should be able to process same items again
        pages = processor.process_search_response(response, deduplicate=True)
        assert len(pages) == 2
    
    def test_filter_newspapers_by_state(self, processor):
        """Test filtering newspapers by state."""
        newspapers = [
            NewspaperInfo(
                lccn='ca1', title='CA Paper', place_of_publication=['San Francisco, California'],
                start_year=1900, end_year=1920, frequency='Daily', subject=[], language=['English'], url=''
            ),
            NewspaperInfo(
                lccn='ny1', title='NY Paper', place_of_publication=['New York, New York'],
                start_year=1900, end_year=1920, frequency='Daily', subject=[], language=['English'], url=''
            ),
            NewspaperInfo(
                lccn='ca2', title='CA Paper 2', place_of_publication=['Los Angeles, California'],
                start_year=1900, end_year=1920, frequency='Daily', subject=[], language=['English'], url=''
            )
        ]
        
        filtered = processor.filter_newspapers_by_criteria(newspapers, state='California')
        
        assert len(filtered) == 2
        assert all('California' in place for newspaper in filtered for place in newspaper.place_of_publication)
    
    def test_filter_newspapers_by_language(self, processor):
        """Test filtering newspapers by language."""
        newspapers = [
            NewspaperInfo(
                lccn='en1', title='English Paper', place_of_publication=['Test City'],
                start_year=1900, end_year=1920, frequency='Daily', subject=[], language=['English'], url=''
            ),
            NewspaperInfo(
                lccn='es1', title='Spanish Paper', place_of_publication=['Test City'],
                start_year=1900, end_year=1920, frequency='Daily', subject=[], language=['Spanish'], url=''
            ),
            NewspaperInfo(
                lccn='multi1', title='Multilingual Paper', place_of_publication=['Test City'],
                start_year=1900, end_year=1920, frequency='Daily', subject=[], language=['English', 'Spanish'], url=''
            )
        ]
        
        filtered = processor.filter_newspapers_by_criteria(newspapers, language='English')
        
        assert len(filtered) == 2  # English-only and multilingual
        assert all(any('English' in lang for lang in newspaper.language) for newspaper in filtered)
    
    def test_filter_newspapers_by_year_range(self, processor):
        """Test filtering newspapers by year range."""
        newspapers = [
            NewspaperInfo(
                lccn='old1', title='Old Paper', place_of_publication=['Test City'],
                start_year=1850, end_year=1880, frequency='Daily', subject=[], language=['English'], url=''
            ),
            NewspaperInfo(
                lccn='new1', title='New Paper', place_of_publication=['Test City'],
                start_year=1900, end_year=1920, frequency='Daily', subject=[], language=['English'], url=''
            ),
            NewspaperInfo(
                lccn='overlap1', title='Overlap Paper', place_of_publication=['Test City'],
                start_year=1880, end_year=1910, frequency='Daily', subject=[], language=['English'], url=''
            )
        ]
        
        filtered = processor.filter_newspapers_by_criteria(newspapers, start_year=1890, end_year=1920)
        
        assert len(filtered) == 2  # new1 and overlap1
        # Verify overlap logic: newspapers should overlap with requested range (1890-1920)
        for newspaper in filtered:
            # Must end after start of range AND start before end of range
            assert newspaper.end_year >= 1890  # Ends after 1890
            assert newspaper.start_year <= 1920  # Starts before 1920
    
    def test_get_newspaper_summary(self, processor):
        """Test generating newspaper summary statistics."""
        newspapers = [
            NewspaperInfo(
                lccn='ca1', title='California Daily', place_of_publication=['San Francisco, California'],
                start_year=1900, end_year=1920, frequency='Daily', subject=[], language=['English'], url=''
            ),
            NewspaperInfo(
                lccn='ca2', title='Los Angeles Times', place_of_publication=['Los Angeles, California'],
                start_year=1910, end_year=1930, frequency='Daily', subject=[], language=['English'], url=''
            ),
            NewspaperInfo(
                lccn='ny1', title='New York Herald', place_of_publication=['New York, New York'],
                start_year=1890, end_year=1925, frequency='Daily', subject=[], language=['English'], url=''
            ),
            NewspaperInfo(
                lccn='es1', title='El Periódico', place_of_publication=['Miami, Florida'],
                start_year=1950, end_year=1970, frequency='Weekly', subject=[], language=['Spanish'], url=''
            )
        ]
        
        summary = processor.get_newspaper_summary(newspapers)
        
        assert summary['total_newspapers'] == 4
        assert 'California' in summary['states']
        assert summary['states']['California'] == 2
        assert 'New York' in summary['states']
        assert summary['states']['New York'] == 1
        assert summary['languages']['English'] == 3
        assert summary['languages']['Spanish'] == 1
        assert summary['year_range'] == (1890, 1970)
        assert len(summary['sample_titles']) == 4
    
    def test_get_newspaper_summary_empty(self, processor):
        """Test summary with empty list."""
        summary = processor.get_newspaper_summary([])
        assert summary == {'total_newspapers': 0}
    
    def test_validate_date_range_valid(self, processor):
        """Test date range validation with valid ranges."""
        assert processor.validate_date_range('1900', '1910') is True
        assert processor.validate_date_range('1900-01-01', '1910-12-31') is True
        assert processor.validate_date_range('1836', '2024') is True
    
    def test_validate_date_range_invalid(self, processor):
        """Test date range validation with invalid ranges."""
        # End before start
        assert processor.validate_date_range('1910', '1900') is False
        
        # Before LOC data range
        assert processor.validate_date_range('1800', '1850') is False
        
        # After current date
        future_year = str(datetime.now().year + 10)
        assert processor.validate_date_range('1900', future_year) is False
        
        # Invalid date format
        assert processor.validate_date_range('invalid', '1900') is False
        assert processor.validate_date_range('1900', 'invalid') is False
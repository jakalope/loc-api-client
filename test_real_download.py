#!/usr/bin/env python3
"""
Test script to perform a small real download from Library of Congress
and compare the data format to our test expectations.
"""

import sys
import json
import pprint
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from newsagger.config import Config
from newsagger.api_client import LocApiClient
from newsagger.processor import NewsDataProcessor
from newsagger.storage import NewsStorage

def test_small_download():
    """Test a small download and examine the data format."""
    print("🔍 Testing small bulk download from Library of Congress...")
    
    # Setup with faster rate for testing (but still respectful)
    config = Config()
    config.request_delay = 3.0  # Minimum LOC requirement
    config.database_path = './test_download.db'
    
    client = LocApiClient(**config.get_api_config())
    processor = NewsDataProcessor()
    storage = NewsStorage(config.database_path)
    
    print(f"📡 Using rate limit: {config.request_delay} seconds between requests")
    
    # Test 1: Get a small sample of newspapers
    print("\n📰 Testing newspaper list API...")
    try:
        newspapers_response = client.get_newspapers(rows=5)  # Just 5 newspapers
        print(f"✅ Retrieved newspapers response with {len(newspapers_response.get('newspapers', []))} newspapers")
        
        # Show sample newspaper data format
        if newspapers_response.get('newspapers'):
            print("\n📋 Sample newspaper data format:")
            sample_newspaper = newspapers_response['newspapers'][0]
            pprint.pprint(sample_newspaper, indent=2, width=80)
            
            # Process and store
            newspapers = processor.process_newspapers_response(newspapers_response)
            stored = storage.store_newspapers(newspapers)
            print(f"✅ Processed and stored {stored} newspapers")
            
            # Compare to our test data format
            print("\n🔍 Comparing to our test data format...")
            print("Expected fields in test data:")
            expected_fields = ['lccn', 'title', 'place_of_publication', 'start_year', 'end_year', 'frequency', 'subject', 'language', 'url']
            
            actual_fields = set(sample_newspaper.keys())
            expected_set = set(expected_fields)
            
            print(f"✅ Fields in common: {sorted(actual_fields & expected_set)}")
            print(f"➕ Extra fields in real data: {sorted(actual_fields - expected_set)}")
            print(f"➖ Missing fields in real data: {sorted(expected_set - actual_fields)}")
            
    except Exception as e:
        print(f"❌ Error fetching newspapers: {e}")
        return False
    
    # Test 2: Search for a small number of pages
    print("\n📄 Testing page search API...")
    try:
        # Search for a very specific query to get small results
        search_response = client.search_pages(
            andtext="earthquake", 
            date1="1906", 
            date2="1906",
            rows=3  # Just 3 results
        )
        
        print(f"✅ Retrieved search response with {len(search_response.get('results', []))} pages")
        
        if search_response.get('results'):
            print("\n📋 Sample page data format:")
            sample_page = search_response['results'][0]
            pprint.pprint(sample_page, indent=2, width=80)
            
            # Process and store
            pages = processor.process_search_response(search_response)
            stored_pages = storage.store_pages(pages)
            print(f"✅ Processed and stored {stored_pages} pages")
            
            # Compare to our test data format
            print("\n🔍 Comparing to our test page data format...")
            expected_page_fields = ['id', 'lccn', 'title', 'date', 'edition', 'sequence', 'url', 'pdf_url', 'image_url']
            
            actual_page_fields = set(sample_page.keys())
            expected_page_set = set(expected_page_fields)
            
            print(f"✅ Fields in common: {sorted(actual_page_fields & expected_page_set)}")
            print(f"➕ Extra fields in real data: {sorted(actual_page_fields - expected_page_set)}")
            print(f"➖ Missing fields in real data: {sorted(expected_page_set - actual_page_fields)}")
            
    except Exception as e:
        print(f"❌ Error searching pages: {e}")
        return False
    
    # Test 3: Check our data processing
    print("\n🗄️ Testing stored data retrieval...")
    try:
        stored_newspapers = storage.get_newspapers()
        stored_pages = storage.get_pages()
        stats = storage.get_storage_stats()
        
        print(f"✅ Database contains:")
        print(f"   📰 {stats['total_newspapers']} newspapers")
        print(f"   📄 {stats['total_pages']} pages")
        print(f"   💾 {stats['db_size_mb']} MB database size")
        
        if stored_newspapers:
            print(f"\n📋 Sample stored newspaper:")
            sample_stored = stored_newspapers[0]
            for key, value in sample_stored.items():
                if key in ['place_of_publication', 'subject', 'language']:
                    # These are stored as JSON strings
                    print(f"   {key}: {json.loads(value)}")
                else:
                    print(f"   {key}: {value}")
                    
    except Exception as e:
        print(f"❌ Error checking stored data: {e}")
        return False
    
    print("\n✅ Small bulk download test completed successfully!")
    print("🔍 Check 'test_download.db' for the downloaded data")
    return True

if __name__ == '__main__':
    success = test_small_download()
    sys.exit(0 if success else 1)
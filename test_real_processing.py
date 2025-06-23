#!/usr/bin/env python3
"""
Test the updated processing with real API data.
"""

import sys
import json
import pprint
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from newsagger.api_client import LocApiClient
from newsagger.processor import NewsDataProcessor
from newsagger.storage import NewsStorage

def test_real_processing():
    """Test processing with real API data."""
    print("🧪 Testing updated processing with real API data...\n")
    
    client = LocApiClient(request_delay=3.0)
    processor = NewsDataProcessor()
    storage = NewsStorage('./test_real_processing.db')
    
    # Test 1: Process actual search results
    print("📄 Test 1: Processing real search results...")
    try:
        search_response = client.search_pages(
            andtext="",  # No text filter for broad results
            date1="1906", 
            date2="1906",
            rows=3
        )
        
        print(f"✅ Retrieved {len(search_response.get('results', []))} search results")
        
        if search_response.get('results'):
            # Process the results
            pages = processor.process_search_response(search_response)
            print(f"✅ Processed {len(pages)} page objects")
            
            # Show processed page info
            if pages:
                page = pages[0]
                print(f"\n📋 Processed page info:")
                print(f"   Item ID: {page.item_id}")
                print(f"   LCCN: {page.lccn}")
                print(f"   Title: {page.title}")
                print(f"   Date: {page.date}")
                print(f"   Edition: {page.edition}")
                print(f"   Sequence: {page.sequence}")
                print(f"   Page URL: {page.page_url}")
                print(f"   PDF URL: {page.pdf_url}")
                print(f"   JP2 URL: {page.jp2_url}")
                print(f"   OCR Text: {page.ocr_text[:100] if page.ocr_text else 'None'}...")
            
            # Store and retrieve
            stored_count = storage.store_pages(pages)
            print(f"✅ Stored {stored_count} pages in database")
            
            # Retrieve and check
            retrieved_pages = storage.get_pages()
            print(f"✅ Retrieved {len(retrieved_pages)} pages from database")
            
            if retrieved_pages:
                stored_page = retrieved_pages[0]
                print(f"\n📋 Stored page data:")
                for key, value in stored_page.items():
                    if key == 'ocr_text' and value:
                        print(f"   {key}: {value[:100]}...")
                    else:
                        print(f"   {key}: {value}")
        
    except Exception as e:
        print(f"❌ Error in processing: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*60 + "\n")
    
    # Test 2: Try a San Francisco earthquake search
    print("📄 Test 2: San Francisco earthquake search...")
    try:
        sf_search = client.search_pages(
            andtext="San Francisco earthquake",
            date1="1906", 
            date2="1906",
            rows=2
        )
        
        results_count = len(sf_search.get('results', []))
        total_items = sf_search.get('totalItems', 0)
        print(f"✅ SF earthquake search: {results_count} results out of {total_items} total")
        
        if sf_search.get('results'):
            pages = processor.process_search_response(sf_search)
            print(f"✅ Processed {len(pages)} SF earthquake pages")
            
            if pages:
                page = pages[0]
                print(f"\n📋 SF earthquake page:")
                print(f"   Date: {page.date}")
                print(f"   Title: {page.title}")
                print(f"   LCCN: {page.lccn}")
                print(f"   OCR snippet: {page.ocr_text[:200] if page.ocr_text else 'None'}...")
        
    except Exception as e:
        print(f"❌ Error in SF earthquake search: {e}")

if __name__ == '__main__':
    test_real_processing()
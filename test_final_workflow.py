#!/usr/bin/env python3
"""
Final test of the complete workflow with real LOC API after all updates.
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

def test_final_workflow():
    """Test the complete updated workflow."""
    print("🎯 Testing final updated workflow with real LOC API...\n")
    
    # Setup
    config = Config()
    config.request_delay = 3.0
    config.database_path = './test_final_workflow.db'
    
    client = LocApiClient(**config.get_api_config())
    processor = NewsDataProcessor()
    storage = NewsStorage(config.database_path)
    
    print(f"📡 Using {config.request_delay}s rate limit (LOC requirement)\n")
    
    # Test 1: Newspaper list with details
    print("📰 Test 1: Fetching newspapers with detailed metadata...")
    try:
        newspapers_count = 0
        for newspaper in client.get_newspapers_with_details(max_newspapers=3):
            newspapers_count += 1
            print(f"   📄 Newspaper {newspapers_count}: {newspaper.get('title', 'Unknown')}")
            
            # Show structure
            if newspapers_count == 1:
                print(f"   📋 Sample newspaper fields: {list(newspaper.keys())}")
                
        print(f"✅ Successfully processed {newspapers_count} newspapers with details\n")
        
    except Exception as e:
        print(f"❌ Error in newspaper fetching: {e}\n")
    
    # Test 2: Search with real format
    print("📄 Test 2: Search and processing...")
    try:
        search_response = client.search_pages(
            andtext="",  # Broad search
            date1="1850", 
            date2="1860",
            rows=3
        )
        
        print(f"✅ Search returned {len(search_response.get('results', []))} results")
        
        if search_response.get('results'):
            # Process with updated format handling
            pages = processor.process_search_response(search_response)
            print(f"✅ Processed {len(pages)} pages")
            
            # Store in database
            stored_count = storage.store_pages(pages)
            print(f"✅ Stored {stored_count} pages in database")
            
            # Show processed page
            if pages:
                page = pages[0]
                print(f"\n📋 Sample processed page:")
                print(f"   Date: {page.date}")
                print(f"   Title: {page.title}")
                print(f"   Edition: {page.edition}, Sequence: {page.sequence}")
                print(f"   PDF URL: {page.pdf_url}")
                print(f"   OCR available: {'Yes' if page.ocr_text else 'No'}")
        
    except Exception as e:
        print(f"❌ Error in search and processing: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "="*60 + "\n")
    
    # Test 3: Storage and retrieval
    print("💾 Test 3: Storage functionality...")
    try:
        stats = storage.get_storage_stats()
        print(f"✅ Database stats:")
        print(f"   📰 Newspapers: {stats.get('total_newspapers', 0)}")
        print(f"   📄 Pages: {stats.get('total_pages', 0)}")
        print(f"   💾 Size: {stats.get('db_size_mb', 0)} MB")
        
        # Test retrieval
        all_pages = storage.get_pages(limit=5)
        print(f"✅ Retrieved {len(all_pages)} pages from storage")
        
        if all_pages:
            print(f"   📋 Sample stored page keys: {list(all_pages[0].keys())}")
        
    except Exception as e:
        print(f"❌ Error in storage operations: {e}")
    
    print("\n✅ Final workflow test completed!")
    print("🗄️ Check 'test_final_workflow.db' for stored data")

if __name__ == '__main__':
    test_final_workflow()
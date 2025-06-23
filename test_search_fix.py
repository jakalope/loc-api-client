#!/usr/bin/env python3
"""
Test the fixed search API parameters.
"""

import sys
import json
import pprint
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from newsagger.api_client import LocApiClient

def test_search_fixes():
    """Test the updated search API parameters."""
    client = LocApiClient(request_delay=3.0)
    
    print("🔧 Testing fixed search API parameters...\n")
    
    # Test 1: Basic search without text filter
    print("📄 Test 1: Basic search for 1906 (no text filter)...")
    try:
        search_response = client.search_pages(
            date1="1906", 
            date2="1906",
            rows=5
        )
        
        results_count = len(search_response.get('results', []))
        total_items = search_response.get('totalItems', 0)
        
        print(f"✅ Search returned {results_count} results out of {total_items} total")
        
        if results_count > 0:
            print("📋 Sample result:")
            sample = search_response['results'][0]
            pprint.pprint(sample, indent=2, width=80)
        
    except Exception as e:
        print(f"❌ Error in basic search: {e}")
    
    print("\n" + "="*60 + "\n")
    
    # Test 2: Search with earthquake keyword
    print("📄 Test 2: Search for 'earthquake' in 1906...")
    try:
        search_response = client.search_pages(
            andtext="earthquake",
            date1="1906", 
            date2="1906",
            rows=5
        )
        
        results_count = len(search_response.get('results', []))
        total_items = search_response.get('totalItems', 0)
        
        print(f"✅ Earthquake search returned {results_count} results out of {total_items} total")
        
        if results_count > 0:
            print("📋 Sample earthquake result:")
            sample = search_response['results'][0]
            pprint.pprint(sample, indent=2, width=80)
        
    except Exception as e:
        print(f"❌ Error in earthquake search: {e}")
    
    print("\n" + "="*60 + "\n")
    
    # Test 3: Check date formatting
    print("📅 Test 3: Verify date formatting...")
    try:
        formatted_start = client._format_search_date("1906", is_end_date=False)
        formatted_end = client._format_search_date("1906", is_end_date=True)
        
        print(f"✅ Date formatting:")
        print(f"   Start date (1906): {formatted_start}")
        print(f"   End date (1906): {formatted_end}")
        
        # Test specific date formatting
        formatted_specific = client._format_search_date("1906-04-18", is_end_date=False)
        print(f"   Specific date (1906-04-18): {formatted_specific}")
        
    except Exception as e:
        print(f"❌ Error in date formatting: {e}")

if __name__ == '__main__':
    test_search_fixes()
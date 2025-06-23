#!/usr/bin/env python3
"""
Quick test of Discovery Manager functionality without heavy API calls.
"""

import sys
import tempfile
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from newsagger.config import Config
from newsagger.api_client import LocApiClient
from newsagger.processor import NewsDataProcessor
from newsagger.storage import NewsStorage
from newsagger.discovery import DiscoveryManager

def test_discovery_quick():
    """Quick test of Discovery Manager core functionality."""
    print("⚡ Quick Discovery Manager Test...\n")
    
    config = Config()
    config.request_delay = 0.1  # Fast for testing
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        # Initialize components
        api_client = LocApiClient(**config.get_api_config())
        processor = NewsDataProcessor()
        storage = NewsStorage(tmp_db.name)
        discovery = DiscoveryManager(api_client, processor, storage)
        
        print("✅ Discovery Manager initialized")
        
        # Test helper methods
        print("\n🔧 Testing helper methods...")
        
        # Test data conversion
        test_newspaper = {
            'lccn': 'sn84038012',
            'title': 'The San Francisco Call',
            'state': 'California',
            'place_of_publication': ['San Francisco, Calif.', 'Los Angeles, Calif.'],
            'start_year': '1895',
            'end_year': '1913',
            'frequency': 'Daily',
            'language': ['English', 'Spanish'],
            'subject': ['General News', 'Politics'],
            'url': 'https://example.com'
        }
        
        periodical = discovery._convert_newspaper_to_periodical(test_newspaper)
        print(f"✅ Converted newspaper to periodical:")
        print(f"   📄 {periodical['title']} ({periodical['city']}, {periodical['state']})")
        print(f"   📅 {periodical['start_year']}-{periodical['end_year']}")
        print(f"   🗣️ {periodical['language']}, 📰 {periodical['frequency']}")
        
        # Store the test periodical
        stored = storage.store_periodicals([periodical])
        print(f"✅ Stored {stored} test periodical")
        
        # Test facet creation (without API calls)
        print("\n📊 Testing facet creation...")
        
        # Manually create some facets
        facet_data = [
            ('date_range', '1906/1906', '', 50000),
            ('date_range', '1907/1907', '', 45000),
            ('state', 'California', '', 100000),
            ('subject', 'earthquake', 'earthquake', 25000)
        ]
        
        facet_ids = []
        for facet_type, facet_value, query, estimated in facet_data:
            facet_id = storage.create_search_facet(facet_type, facet_value, query, estimated)
            facet_ids.append(facet_id)
            print(f"✅ Created {facet_type} facet: {facet_value}")
        
        # Test facet completion simulation
        print("\n📈 Simulating facet discovery completion...")
        for facet_id in facet_ids[:2]:  # Complete first 2 facets
            storage.update_facet_discovery(
                facet_id,
                actual_items=45000,
                items_discovered=45000,
                status='completed'
            )
        print(f"✅ Marked 2 facets as completed")
        
        # Test queue population
        print("\n⬇️ Testing queue population...")
        queue_count = discovery.populate_download_queue(
            priority_states=['California'],
            priority_date_ranges=['1906/1906']
        )
        print(f"✅ Added {queue_count} items to queue")
        
        # Show queue contents
        queue = storage.get_download_queue()
        print(f"📋 Queue contents ({len(queue)} items):")
        for item in queue:
            print(f"   Priority {item['priority']}: {item['queue_type']} {item['reference_id']}")
        
        # Test discovery summary
        print("\n📊 Testing discovery summary...")
        summary = discovery.get_discovery_summary()
        
        print("✅ Discovery Summary:")
        stats = summary['discovery_stats']
        print(f"   📰 Periodicals: {stats['total_periodicals']} total")
        print(f"   🔍 Facets: {stats['total_facets']} total, {stats['completed_facets']} completed")
        print(f"   ⬇️ Queue: {stats['total_queue_items']} items")
        print(f"   📊 Estimated: {stats['estimated_items']} items total")
        
        # Test priority calculation
        print("\n🎯 Testing priority calculation...")
        priority = discovery._calculate_periodical_priority(periodical)
        print(f"✅ Calculated priority for test periodical: {priority}")
        
        # Test helper parsing methods
        print("\n🔧 Testing parsing methods...")
        
        test_cases = [
            ("1906-04-18", "Date parsing"),
            ("19060418", "Date parsing (compact)"),
            ("San Francisco, California", "City extraction"),
            ("1895", "Year parsing"),
            (["English", "Spanish"], "Language extraction"),
        ]
        
        for test_input, description in test_cases:
            if description == "Date parsing":
                result = discovery._parse_issue_date(test_input)
            elif description == "Date parsing (compact)":
                result = discovery._parse_issue_date(test_input)
            elif description == "City extraction":
                result = discovery._extract_city([test_input])
            elif description == "Year parsing":
                result = discovery._parse_year(test_input)
            elif description == "Language extraction":
                result = discovery._extract_primary_language(test_input)
            
            print(f"✅ {description}: '{test_input}' → '{result}'")
        
        print(f"\n✅ Quick Discovery Manager test completed!")
        print(f"🗄️ Test database: {tmp_db.name}")

if __name__ == '__main__':
    test_discovery_quick()
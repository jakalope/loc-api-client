#!/usr/bin/env python3
"""
Test the Discovery Manager for coordinated periodical and facet tracking.
"""

import sys
import json
import tempfile
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from newsagger.config import Config
from newsagger.api_client import LocApiClient
from newsagger.processor import NewsDataProcessor
from newsagger.storage import NewsStorage
from newsagger.discovery import DiscoveryManager

def test_discovery_manager():
    """Test the Discovery Manager functionality."""
    print("🔍 Testing Discovery Manager...\n")
    
    # Setup components
    config = Config()
    config.request_delay = 3.0  # Real API timing
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        # Initialize components
        api_client = LocApiClient(**config.get_api_config())
        processor = NewsDataProcessor()
        storage = NewsStorage(tmp_db.name)
        discovery = DiscoveryManager(api_client, processor, storage)
        
        print("✅ Initialized Discovery Manager")
        
        # Test 1: Discover a small set of periodicals
        print("\n📰 Test 1: Discovering periodicals (limited to 3)...")
        try:
            discovered_count = discovery.discover_all_periodicals(max_newspapers=3)
            print(f"✅ Discovered {discovered_count} periodicals")
            
            # Show discovered periodicals
            periodicals = storage.get_periodicals()
            print(f"📋 Discovered periodicals:")
            for p in periodicals:
                print(f"   📄 {p['title']} ({p['state']}) [{p['start_year']}-{p['end_year']}]")
                
        except Exception as e:
            print(f"❌ Error discovering periodicals: {e}")
        
        # Test 2: Create date range facets
        print("\n📅 Test 2: Creating date range facets...")
        try:
            # Create facets for a few significant years
            facet_ids = discovery.create_date_range_facets(1906, 1908, facet_size_years=1)
            print(f"✅ Created {len(facet_ids)} date range facets")
            
            facets = storage.get_search_facets(facet_type='date_range')
            for facet in facets:
                print(f"   📅 {facet['facet_value']}: ~{facet['estimated_items']} items")
                
        except Exception as e:
            print(f"❌ Error creating date facets: {e}")
        
        # Test 3: Create state facets (from discovered periodicals)
        print("\n🗺️ Test 3: Creating state facets...")
        try:
            state_facet_ids = discovery.create_state_facets()
            print(f"✅ Created {len(state_facet_ids)} state facets")
            
            state_facets = storage.get_search_facets(facet_type='state')
            for facet in state_facets:
                print(f"   🗺️ {facet['facet_value']}: ~{facet['estimated_items']} items")
                
        except Exception as e:
            print(f"❌ Error creating state facets: {e}")
        
        # Test 4: Simulate facet completion and populate queue
        print("\n📊 Test 4: Simulating discovery completion and queue population...")
        try:
            # Mark some facets as completed (simulate discovery)
            facets = storage.get_search_facets()
            if facets:
                # Simulate discovery completion for first facet
                facet = facets[0]
                storage.update_facet_discovery(
                    facet['id'], 
                    actual_items=facet['estimated_items'] - 100,  # Slightly different actual
                    items_discovered=facet['estimated_items'] - 100,
                    status='completed'
                )
                print(f"✅ Marked facet {facet['facet_value']} as completed")
            
            # Populate download queue with priorities
            queue_count = discovery.populate_download_queue(
                priority_states=['California', 'New York'],
                priority_date_ranges=['1906/1906', '1908/1908']
            )
            print(f"✅ Added {queue_count} items to download queue")
            
            # Show queue
            queue = storage.get_download_queue(limit=5)
            print(f"📋 Download queue (top 5):")
            for item in queue:
                print(f"   ⏳ Priority {item['priority']}: {item['queue_type']} {item['reference_id']}")
                print(f"      💾 {item['estimated_size_mb']} MB, ⏱️ {item['estimated_time_hours']:.1f} hours")
                
        except Exception as e:
            print(f"❌ Error populating queue: {e}")
        
        # Test 5: Get comprehensive discovery summary
        print("\n📈 Test 5: Getting discovery summary...")
        try:
            summary = discovery.get_discovery_summary()
            
            print("✅ Discovery Summary:")
            print(f"   📰 Periodicals: {summary['discovery_stats']['total_periodicals']} total, "
                  f"{summary['discovery_stats']['discovered_periodicals']} discovered")
            print(f"   🔍 Facets: {summary['discovery_stats']['total_facets']} total, "
                  f"{summary['discovery_stats']['completed_facets']} completed")
            print(f"   ⬇️ Queue: {summary['discovery_stats']['total_queue_items']} items, "
                  f"{summary['discovery_stats']['queued_items']} queued")
            print(f"   📊 Estimated items: {summary['discovery_stats']['estimated_items']}")
            
            print(f"\n📋 Next downloads:")
            for i, download in enumerate(summary['next_downloads'][:3], 1):
                print(f"   {i}. {download['type']}: {download['reference']} "
                      f"(Priority {download['priority']}, {download['estimated_size_mb']} MB)")
                
        except Exception as e:
            print(f"❌ Error getting summary: {e}")
        
        # Test 6: Test periodical issue discovery (if we have a real LCCN)
        print("\n📅 Test 6: Testing issue discovery...")
        try:
            periodicals = storage.get_periodicals()
            if periodicals:
                test_lccn = periodicals[0]['lccn']
                print(f"🔍 Discovering issues for {test_lccn}...")
                
                # Note: This will make real API calls, so we'll limit it
                issues_count = discovery.discover_periodical_issues(test_lccn)
                print(f"✅ Discovered {issues_count} issues for {test_lccn}")
                
                # Show discovered issues
                issues = storage.get_periodical_issues(lccn=test_lccn)
                print(f"📋 Sample issues (first 5):")
                for issue in issues[:5]:
                    print(f"   📅 {issue['issue_date']}: {issue['pages_count']} pages")
            else:
                print("⚠️ No periodicals available for issue discovery test")
                
        except Exception as e:
            print(f"❌ Error discovering issues: {e}")
        
        print(f"\n✅ Discovery Manager test completed!")
        print(f"🗄️ Test database: {tmp_db.name}")
        
        # Final stats
        final_stats = storage.get_discovery_stats()
        print(f"\n📊 Final Statistics:")
        for key, value in final_stats.items():
            print(f"   {key}: {value}")

if __name__ == '__main__':
    test_discovery_manager()
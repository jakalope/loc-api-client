#!/usr/bin/env python3
"""
Test the enhanced storage schema for periodical and facet tracking.
"""

import sys
import json
import tempfile
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from newsagger.storage import NewsStorage

def test_enhanced_storage():
    """Test the new periodical and facet tracking features."""
    print("🗄️ Testing enhanced storage schema...\n")
    
    # Create a temporary database for testing
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        storage = NewsStorage(tmp_db.name)
        
        print("✅ Database initialized with new schema")
        
        # Test 1: Store periodicals
        print("\n📰 Test 1: Storing periodicals...")
        test_periodicals = [
            {
                'lccn': 'sn84038012',
                'title': 'The San Francisco Call',
                'state': 'California',
                'city': 'San Francisco',
                'start_year': 1895,
                'end_year': 1913,
                'frequency': 'Daily',
                'language': 'English',
                'subject': 'General News',
                'url': 'https://chroniclingamerica.loc.gov/lccn/sn84038012/'
            },
            {
                'lccn': 'sn85066387',
                'title': 'San Francisco Chronicle',
                'state': 'California', 
                'city': 'San Francisco',
                'start_year': 1865,
                'end_year': 1922,
                'frequency': 'Daily',
                'language': 'English',
                'subject': 'General News',
                'url': 'https://chroniclingamerica.loc.gov/lccn/sn85066387/'
            }
        ]
        
        stored_count = storage.store_periodicals(test_periodicals)
        print(f"✅ Stored {stored_count} periodicals")
        
        # Retrieve and display
        periodicals = storage.get_periodicals(state='California')
        print(f"✅ Retrieved {len(periodicals)} California periodicals")
        for p in periodicals:
            print(f"   📄 {p['title']} ({p['start_year']}-{p['end_year']})")
        
        # Test 2: Create search facets
        print("\n🔍 Test 2: Creating search facets...")
        facet_data = [
            ('date_range', '1906/1906', 'earthquake', 50000),
            ('date_range', '1900/1910', 'general', 2000000),
            ('state', 'California', 'state_search', 500000),
            ('subject', 'earthquake', 'disaster_news', 100000)
        ]
        
        facet_ids = []
        for facet_type, facet_value, query, estimated in facet_data:
            facet_id = storage.create_search_facet(facet_type, facet_value, query, estimated)
            facet_ids.append(facet_id)
            print(f"✅ Created facet: {facet_type}={facet_value} (ID: {facet_id})")
        
        # Retrieve facets
        facets = storage.get_search_facets()
        print(f"✅ Retrieved {len(facets)} search facets")
        
        # Test 3: Update periodical discovery progress
        print("\n🔎 Test 3: Updating discovery progress...")
        storage.update_periodical_discovery('sn84038012', total_issues=5000, issues_discovered=1200)
        storage.update_periodical_discovery('sn85066387', total_issues=8000, issues_discovered=8000, complete=True)
        print("✅ Updated periodical discovery progress")
        
        # Test 4: Update facet progress
        print("\n📊 Test 4: Updating facet progress...")
        storage.update_facet_discovery(facet_ids[0], actual_items=48562, items_discovered=20000, status='discovering')
        storage.update_facet_discovery(facet_ids[1], actual_items=1950000, items_discovered=1950000, status='completed')
        print("✅ Updated facet discovery progress")
        
        # Test 5: Store periodical issues
        print("\n📅 Test 5: Storing periodical issues...")
        test_issues = [
            ('sn84038012', '1906-04-18', 4, 24),
            ('sn84038012', '1906-04-19', 4, 20),
            ('sn85066387', '1906-04-18', 6, 36)
        ]
        
        for lccn, date, editions, pages in test_issues:
            issue_id = storage.store_periodical_issue(lccn, date, editions, pages)
            print(f"✅ Stored issue: {lccn} {date} (ID: {issue_id})")
        
        # Test 6: Add to download queue
        print("\n⬇️ Test 6: Managing download queue...")
        queue_items = [
            ('facet', str(facet_ids[0]), 1, 500, 25.0),  # High priority earthquake facet
            ('periodical', 'sn84038012', 3, 200, 10.0),   # Medium priority periodical
            ('periodical', 'sn85066387', 5, 800, 40.0)    # Lower priority periodical
        ]
        
        for queue_type, ref_id, priority, size_mb, time_hours in queue_items:
            queue_id = storage.add_to_download_queue(queue_type, ref_id, priority, size_mb, time_hours)
            print(f"✅ Added to queue: {queue_type} {ref_id} (ID: {queue_id})")
        
        # Get queue
        queue = storage.get_download_queue(status='queued')
        print(f"✅ Retrieved {len(queue)} queued items:")
        for item in queue:
            print(f"   ⏳ Priority {item['priority']}: {item['queue_type']} {item['reference_id']}")
        
        # Test 7: Update queue item status
        print("\n📈 Test 7: Updating queue progress...")
        if queue:
            storage.update_queue_item(queue[0]['id'], status='active', progress_percent=0)
            storage.update_queue_item(queue[0]['id'], progress_percent=45.5)
            print(f"✅ Updated queue item {queue[0]['id']} to 45.5% progress")
        
        # Test 8: Get comprehensive statistics
        print("\n📊 Test 8: Getting comprehensive statistics...")
        discovery_stats = storage.get_discovery_stats()
        print("✅ Discovery and download statistics:")
        for key, value in discovery_stats.items():
            print(f"   📈 {key}: {value}")
        
        # Test 9: Get original storage stats (should still work)
        print("\n📋 Test 9: Legacy storage stats...")
        storage_stats = storage.get_storage_stats()
        print("✅ Legacy storage statistics:")
        for key, value in storage_stats.items():
            print(f"   📋 {key}: {value}")
        
        print(f"\n✅ Enhanced storage schema test completed!")
        print(f"🗄️ Test database: {tmp_db.name}")
        
        # Test 10: Query examples for real usage
        print("\n🔍 Test 10: Real-world query examples...")
        
        # Find periodicals that need discovery
        undiscovered = storage.get_periodicals(discovery_complete=False)
        print(f"✅ Found {len(undiscovered)} periodicals needing discovery")
        
        # Find facets that are ready for download
        ready_facets = storage.get_search_facets(status='completed')
        print(f"✅ Found {len(ready_facets)} facets ready for download")
        
        # Get issues for a specific periodical
        sf_call_issues = storage.get_periodical_issues(lccn='sn84038012')
        print(f"✅ Found {len(sf_call_issues)} issues for SF Call")
        
        # Get next items in download queue
        next_downloads = storage.get_download_queue(status='queued', limit=3)
        print(f"✅ Next {len(next_downloads)} items to download:")
        for item in next_downloads:
            print(f"   🎯 {item['queue_type']}: {item['reference_id']} (Priority: {item['priority']})")

if __name__ == '__main__':
    test_enhanced_storage()
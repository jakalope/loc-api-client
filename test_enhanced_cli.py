#!/usr/bin/env python3
"""
Test the enhanced CLI functionality.
"""

import sys
import tempfile
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from newsagger.storage import NewsStorage
from newsagger.discovery import DiscoveryManager
from newsagger.config import Config
from newsagger.api_client import LocApiClient
from newsagger.processor import NewsDataProcessor

def test_enhanced_cli():
    """Demonstrate the enhanced CLI functionality with sample data."""
    print("🚀 Testing Enhanced CLI Functionality...\n")
    
    # Create components
    config = Config()
    config.request_delay = 0.1  # Fast for testing
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        storage = NewsStorage(tmp_db.name)
        
        # Add some sample data that would be created by the CLI commands
        print("📊 Setting up sample data...")
        
        # Add sample periodicals
        sample_periodicals = [
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
            },
            {
                'lccn': 'sn83030214',
                'title': 'New-York tribune',
                'state': 'New York',
                'city': 'New York',
                'start_year': 1866,
                'end_year': 1924,
                'frequency': 'Daily',
                'language': 'English',
                'subject': 'General News',
                'url': 'https://chroniclingamerica.loc.gov/lccn/sn83030214/'
            }
        ]
        
        stored_count = storage.store_periodicals(sample_periodicals)
        print(f"✅ Stored {stored_count} sample periodicals")
        
        # Update some with discovery progress
        storage.update_periodical_discovery('sn84038012', total_issues=5000, issues_discovered=2500)
        storage.update_periodical_discovery('sn85066387', total_issues=8000, issues_discovered=8000, complete=True)
        storage.update_periodical_discovery('sn83030214', total_issues=12000, issues_discovered=6000)
        print("✅ Updated discovery progress")
        
        # Add sample facets
        facet_data = [
            ('date_range', '1906/1906', '', 50000),
            ('date_range', '1918/1918', '', 75000),
            ('state', 'California', '', 120000),
            ('state', 'New York', '', 180000),
            ('subject', 'earthquake', 'earthquake', 25000)
        ]
        
        facet_ids = []
        for facet_type, facet_value, query, estimated in facet_data:
            facet_id = storage.create_search_facet(facet_type, facet_value, query, estimated)
            facet_ids.append(facet_id)
        print(f"✅ Created {len(facet_ids)} sample facets")
        
        # Update some facets as completed
        storage.update_facet_discovery(facet_ids[0], actual_items=48562, items_discovered=48562, status='completed')
        storage.update_facet_discovery(facet_ids[1], actual_items=73000, items_discovered=30000, status='discovering')
        storage.update_facet_discovery(facet_ids[2], actual_items=115000, items_discovered=115000, status='completed')
        print("✅ Updated facet discovery status")
        
        # Add sample queue items
        queue_data = [
            ('facet', str(facet_ids[0]), 1, 500, 25.0),
            ('facet', str(facet_ids[2]), 2, 1200, 60.0),
            ('periodical', 'sn85066387', 3, 800, 40.0),
            ('periodical', 'sn84038012', 5, 600, 30.0)
        ]
        
        for queue_type, ref_id, priority, size_mb, time_hours in queue_data:
            storage.add_to_download_queue(queue_type, ref_id, priority, size_mb, time_hours)
        print(f"✅ Added {len(queue_data)} items to download queue")
        
        # Simulate some download progress
        storage.update_queue_item(1, status='active', progress_percent=25.0)
        storage.update_queue_item(2, status='completed')
        print("✅ Updated queue progress")
        
        # Now demonstrate what the CLI commands would show
        print("\n" + "="*60)
        print("📊 DISCOVERY STATUS DEMO")
        print("="*60)
        
        # This is what the discovery-status command would show
        stats = storage.get_discovery_stats()
        
        print("🔍 Discovery & Download Status:")
        print(f"\n📰 Periodicals:")
        print(f"   Total: {stats['total_periodicals']:,}")
        print(f"   Discovered: {stats['discovered_periodicals']:,}")
        print(f"   Downloaded: {stats['downloaded_periodicals']:,}")
        
        if stats['total_periodicals'] > 0:
            discovery_pct = (stats['discovered_periodicals'] / stats['total_periodicals']) * 100
            download_pct = (stats['downloaded_periodicals'] / stats['total_periodicals']) * 100
            print(f"   Discovery progress: {discovery_pct:.1f}%")
            print(f"   Download progress: {download_pct:.1f}%")
        
        print(f"\n🔍 Search Facets:")
        print(f"   Total: {stats['total_facets']:,}")
        print(f"   Completed: {stats['completed_facets']:,}")
        print(f"   Errors: {stats['error_facets']:,}")
        
        print(f"\n📊 Estimated Content:")
        print(f"   Estimated items: {stats['estimated_items']:,}")
        print(f"   Actual items: {stats['actual_items']:,}")
        print(f"   Discovered: {stats['discovered_items']:,}")
        print(f"   Downloaded: {stats['downloaded_items']:,}")
        
        print(f"\n⬇️ Download Queue:")
        print(f"   Total items: {stats['total_queue_items']:,}")
        print(f"   Queued: {stats['queued_items']:,}")
        print(f"   Active: {stats['active_items']:,}")
        print(f"   Completed: {stats['completed_queue_items']:,}")
        print(f"   Average progress: {stats['avg_queue_progress']:.1f}%")
        
        print("\n" + "="*60)
        print("📋 FACETS LIST DEMO")
        print("="*60)
        
        # This is what the list-facets command would show
        facets = storage.get_search_facets()
        
        print(f"📋 Found {len(facets)} facets:")
        
        for facet in facets:
            status_icon = {
                'pending': '⏳',
                'discovering': '🔍',
                'downloading': '⬇️',
                'completed': '✅',
                'error': '❌'
            }.get(facet['status'], '❓')
            
            print(f"\n{status_icon} {facet['facet_type']}: {facet['facet_value']}")
            print(f"   Status: {facet['status']}")
            print(f"   Estimated: {facet['estimated_items']:,} items")
            if facet['actual_items']:
                print(f"   Actual: {facet['actual_items']:,} items")
            if facet['items_discovered']:
                print(f"   Discovered: {facet['items_discovered']:,} items")
        
        print("\n" + "="*60)
        print("⬇️ DOWNLOAD QUEUE DEMO")
        print("="*60)
        
        # This is what the show-queue command would show
        queue = storage.get_download_queue(limit=10)
        
        print(f"📋 Download Queue - Top {len(queue)} items:")
        
        for i, item in enumerate(queue, 1):
            status_icon = {
                'queued': '⏳',
                'active': '🔄',
                'paused': '⏸️',
                'completed': '✅',
                'failed': '❌'
            }.get(item['status'], '❓')
            
            print(f"\n{i}. {status_icon} Priority {item['priority']}: {item['queue_type']} {item['reference_id']}")
            print(f"   Status: {item['status']}")
            print(f"   Size: {item['estimated_size_mb']} MB")
            print(f"   Time: {item['estimated_time_hours']:.1f} hours")
            if item['progress_percent'] > 0:
                print(f"   Progress: {item['progress_percent']:.1f}%")
        
        print("\n✅ Enhanced CLI demonstration completed!")
        print(f"🗄️ Sample database: {tmp_db.name}")
        print("\nℹ️ The actual CLI commands that would produce this output:")
        print("   newsagger discovery-status")
        print("   newsagger list-facets")
        print("   newsagger show-queue")
        print("   newsagger discover --max-papers 50 --states California,New York")
        print("   newsagger create-facets --start-year 1906 --end-year 1920")
        print("   newsagger populate-queue --priority-states California --priority-dates 1906/1906")

if __name__ == '__main__':
    test_enhanced_cli()
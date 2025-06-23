"""
Command Line Interface

Provides interactive CLI for newspaper selection and download management.
"""

import click
import json
from typing import List, Dict
from tqdm import tqdm
from .config import Config
from .api_client import LocApiClient
from .processor import NewsDataProcessor
from .storage import NewsStorage
from .discovery import DiscoveryManager


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def cli(verbose):
    """Newsagger - Library of Congress News Archive Aggregator"""
    config = Config()
    if verbose:
        config.log_level = 'DEBUG'
    config.setup_logging()


@cli.command()
def list_newspapers():
    """List available newspapers with filtering options."""
    config = Config()
    client = LocApiClient(**config.get_api_config())
    processor = NewsDataProcessor()
    
    click.echo("Fetching newspaper list from Library of Congress...")
    
    with tqdm(desc="Loading newspapers") as pbar:
        newspapers = []
        for newspaper in client.get_all_newspapers():
            newspapers.append(newspaper)
            pbar.update(1)
    
    processed = processor.process_newspapers_response({'newspapers': newspapers})
    
    # Display summary
    summary = processor.get_newspaper_summary(processed)
    
    click.echo(f"\n📰 Found {summary['total_newspapers']} newspapers")
    
    if summary.get('states'):
        click.echo("\n🗺️  Top states:")
        for state, count in list(summary['states'].items())[:10]:
            click.echo(f"   {state}: {count}")
    
    if summary.get('languages'):
        click.echo("\n🌐 Languages:")
        for lang, count in summary['languages'].items():
            click.echo(f"   {lang}: {count}")
    
    if summary.get('year_range'):
        start, end = summary['year_range']
        click.echo(f"\n📅 Date range: {start} - {end}")


@cli.command()
@click.option('--state', help='Filter by state')
@click.option('--language', help='Filter by language')
@click.option('--limit', default=10, help='Number of results to show')
def search_newspapers(state, language, limit):
    """Search newspapers with filters."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    
    newspapers = storage.get_newspapers(state=state, language=language)
    
    if not newspapers:
        click.echo("No newspapers found matching criteria.")
        return
    
    click.echo(f"Found {len(newspapers)} newspapers:\n")
    
    for i, newspaper in enumerate(newspapers[:limit]):
        place = json.loads(newspaper['place_of_publication'])
        langs = json.loads(newspaper['language'])
        
        click.echo(f"{i+1}. {newspaper['title']}")
        click.echo(f"   LCCN: {newspaper['lccn']}")
        click.echo(f"   Location: {', '.join(place)}")
        click.echo(f"   Languages: {', '.join(langs)}")
        click.echo(f"   Years: {newspaper['start_year']}-{newspaper['end_year']}")
        click.echo()


@cli.command()
@click.argument('lccn')
@click.option('--date1', default='1836', help='Start date (YYYY or YYYY-MM-DD)')
@click.option('--date2', help='End date (YYYY or YYYY-MM-DD)')
@click.option('--estimate-only', is_flag=True, help='Only show download estimate')
def download_newspaper(lccn, date1, date2, estimate_only):
    """Download pages for a specific newspaper."""
    config = Config()
    client = LocApiClient(**config.get_api_config())
    processor = NewsDataProcessor()
    storage = NewsStorage(**config.get_storage_config())
    
    # Validate date range
    if not processor.validate_date_range(date1, date2 or str(config.current_year)):
        click.echo("❌ Invalid date range")
        return
    
    # Get estimate
    estimate = client.estimate_download_size((date1, date2), lccn)
    
    click.echo(f"📊 Download Estimate for {lccn}:")
    click.echo(f"   Total pages: {estimate['total_pages']:,}")
    click.echo(f"   Estimated size: {estimate['estimated_size_gb']} GB")
    click.echo(f"   Estimated time: {estimate['estimated_time_hours']:.1f} hours")
    
    if estimate_only:
        return
    
    if estimate['total_pages'] > 10000:
        if not click.confirm(f"This will download {estimate['total_pages']:,} pages. Continue?"):
            return
    
    # Create download session
    session_id = storage.create_download_session(
        f"{lccn}_{date1}_{date2}",
        {'lccn': lccn, 'date1': date1, 'date2': date2},
        estimate['total_pages']
    )
    
    # Start download with faceted search
    base_query = {
        'andtext': f'lccn:{lccn}',
        'date1': date1,
        'date2': date2
    }
    
    total_downloaded = 0
    
    with tqdm(total=estimate['total_pages'], desc="Downloading pages") as pbar:
        for result_batch in client.search_with_faceted_dates(base_query):
            pages = processor.process_search_response(result_batch)
            stored = storage.store_pages(pages)
            
            total_downloaded += stored
            pbar.update(stored)
            
            # Update session progress
            storage.update_session_progress(session_id, total_downloaded)
    
    # Complete session
    storage.complete_session(session_id)
    click.echo(f"✅ Downloaded {total_downloaded:,} pages")


@cli.command()
def status():
    """Show download status and statistics."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    stats = storage.get_storage_stats()
    
    click.echo("📊 Newsagger Status:")
    click.echo(f"   Database size: {stats['db_size_mb']} MB")
    click.echo(f"   Total newspapers: {stats['total_newspapers']:,}")
    click.echo(f"   Total pages: {stats['total_pages']:,}")
    click.echo(f"   Downloaded pages: {stats['downloaded_pages']:,}")
    click.echo(f"   Active sessions: {stats['active_sessions']}")
    
    if stats['total_pages'] > 0:
        progress = (stats['downloaded_pages'] / stats['total_pages']) * 100
        click.echo(f"   Download progress: {progress:.1f}%")


@cli.command()
@click.argument('text')
@click.option('--date1', default='1836', help='Start date')
@click.option('--date2', help='End date')
@click.option('--limit', default=100, help='Max results per facet')
def search_text(text, date1, date2, limit):
    """Search for text across newspaper archives."""
    config = Config()
    client = LocApiClient(**config.get_api_config())
    processor = NewsDataProcessor()
    
    base_query = {
        'andtext': text,
        'date1': date1,
        'date2': date2,
        'rows': min(limit, 1000)
    }
    
    click.echo(f"🔍 Searching for '{text}' from {date1} to {date2 or 'present'}...")
    
    total_results = 0
    
    with tqdm(desc="Searching") as pbar:
        for result_batch in client.search_with_faceted_dates(base_query, max_results_per_facet=limit):
            pages = processor.process_search_response(result_batch)
            
            for page in pages:
                click.echo(f"📰 {page.title} - {page.date}")
                click.echo(f"   URL: {page.page_url}")
                click.echo()
                
                total_results += 1
                pbar.update(1)
                
                if total_results >= limit:
                    break
            
            if total_results >= limit:
                break
    
    click.echo(f"Found {total_results} results")


# ===== ENHANCED DISCOVERY AND TRACKING COMMANDS =====

@cli.command()
@click.option('--max-papers', default=None, type=int, help='Limit discovery to N newspapers')
@click.option('--states', help='Comma-separated list of states to prioritize')
def discover():
    """Discover and catalog available periodicals from LOC."""
    config = Config()
    client = LocApiClient(**config.get_api_config())
    processor = NewsDataProcessor()
    storage = NewsStorage(**config.get_storage_config())
    discovery = DiscoveryManager(client, processor, storage)
    
    click.echo("🔍 Starting periodical discovery...")
    
    try:
        discovered_count = discovery.discover_all_periodicals(max_newspapers=max_papers)
        click.echo(f"✅ Discovered {discovered_count} periodicals")
        
        # Show summary
        periodicals = storage.get_periodicals()
        if periodicals:
            states = {}
            for p in periodicals:
                state = p.get('state', 'Unknown')
                states[state] = states.get(state, 0) + 1
            
            click.echo(f"\n📊 Periodicals by state:")
            for state, count in sorted(states.items(), key=lambda x: x[1], reverse=True)[:10]:
                click.echo(f"   {state}: {count}")
        
        # Create facets if states specified
        if states:
            priority_states = [s.strip() for s in states.split(',')]
            click.echo(f"\n🗺️ Creating facets for priority states: {', '.join(priority_states)}")
            facet_ids = discovery.create_state_facets(priority_states)
            click.echo(f"✅ Created {len(facet_ids)} state facets")
        
    except Exception as e:
        click.echo(f"❌ Discovery failed: {e}")


@cli.command()
@click.option('--start-year', default=1900, type=int, help='Start year for facets')
@click.option('--end-year', default=1920, type=int, help='End year for facets') 
@click.option('--facet-size', default=1, type=int, help='Years per facet')
def create_facets(start_year, end_year, facet_size):
    """Create date range facets for systematic downloading."""
    config = Config()
    client = LocApiClient(**config.get_api_config())
    processor = NewsDataProcessor()
    storage = NewsStorage(**config.get_storage_config())
    discovery = DiscoveryManager(client, processor, storage)
    
    click.echo(f"📅 Creating date facets from {start_year} to {end_year} ({facet_size} year(s) each)...")
    
    try:
        with tqdm(total=(end_year - start_year + 1)) as pbar:
            facet_ids = discovery.create_date_range_facets(start_year, end_year, facet_size)
            pbar.update(end_year - start_year + 1)
        
        click.echo(f"✅ Created {len(facet_ids)} date range facets")
        
        # Show created facets
        facets = storage.get_search_facets(facet_type='date_range')
        click.echo(f"\n📋 Created facets:")
        for facet in facets[-len(facet_ids):]:  # Show just the newly created ones
            click.echo(f"   📅 {facet['facet_value']}: ~{facet['estimated_items']:,} items")
        
    except Exception as e:
        click.echo(f"❌ Facet creation failed: {e}")


@cli.command()
@click.option('--priority-states', help='Comma-separated priority states')
@click.option('--priority-dates', help='Comma-separated priority date ranges (e.g., "1906/1906,1929/1929")')
def populate_queue(priority_states, priority_dates):
    """Populate download queue with discovered content."""
    config = Config()
    client = LocApiClient(**config.get_api_config())
    processor = NewsDataProcessor()
    storage = NewsStorage(**config.get_storage_config())
    discovery = DiscoveryManager(client, processor, storage)
    
    click.echo("⬇️ Populating download queue...")
    
    try:
        states_list = priority_states.split(',') if priority_states else None
        dates_list = priority_dates.split(',') if priority_dates else None
        
        queue_count = discovery.populate_download_queue(
            priority_states=states_list,
            priority_date_ranges=dates_list
        )
        
        click.echo(f"✅ Added {queue_count} items to download queue")
        
        # Show top queue items
        queue = storage.get_download_queue(status='queued', limit=10)
        if queue:
            click.echo(f"\n📋 Next downloads (top 10):")
            for i, item in enumerate(queue, 1):
                click.echo(f"   {i}. Priority {item['priority']}: {item['queue_type']} {item['reference_id']}")
                click.echo(f"      💾 {item['estimated_size_mb']} MB, ⏱️ {item['estimated_time_hours']:.1f} hours")
        
    except Exception as e:
        click.echo(f"❌ Queue population failed: {e}")


@cli.command()
def discovery_status():
    """Show comprehensive discovery and download progress."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    
    try:
        stats = storage.get_discovery_stats()
        
        click.echo("🔍 Discovery & Download Status:")
        click.echo(f"\n📰 Periodicals:")
        click.echo(f"   Total: {stats['total_periodicals']:,}")
        click.echo(f"   Discovered: {stats['discovered_periodicals']:,}")
        click.echo(f"   Downloaded: {stats['downloaded_periodicals']:,}")
        
        if stats['total_periodicals'] > 0:
            discovery_pct = (stats['discovered_periodicals'] / stats['total_periodicals']) * 100
            download_pct = (stats['downloaded_periodicals'] / stats['total_periodicals']) * 100
            click.echo(f"   Discovery progress: {discovery_pct:.1f}%")
            click.echo(f"   Download progress: {download_pct:.1f}%")
        
        click.echo(f"\n🔍 Search Facets:")
        click.echo(f"   Total: {stats['total_facets']:,}")
        click.echo(f"   Completed: {stats['completed_facets']:,}")
        click.echo(f"   Errors: {stats['error_facets']:,}")
        
        click.echo(f"\n📊 Estimated Content:")
        click.echo(f"   Estimated items: {stats['estimated_items']:,}")
        click.echo(f"   Actual items: {stats['actual_items']:,}")
        click.echo(f"   Discovered: {stats['discovered_items']:,}")
        click.echo(f"   Downloaded: {stats['downloaded_items']:,}")
        
        if stats['actual_items'] > 0:
            discovery_item_pct = (stats['discovered_items'] / stats['actual_items']) * 100
            download_item_pct = (stats['downloaded_items'] / stats['actual_items']) * 100
            click.echo(f"   Discovery progress: {discovery_item_pct:.1f}%")
            click.echo(f"   Download progress: {download_item_pct:.1f}%")
        
        click.echo(f"\n⬇️ Download Queue:")
        click.echo(f"   Total items: {stats['total_queue_items']:,}")
        click.echo(f"   Queued: {stats['queued_items']:,}")
        click.echo(f"   Active: {stats['active_items']:,}")
        click.echo(f"   Completed: {stats['completed_queue_items']:,}")
        click.echo(f"   Average progress: {stats['avg_queue_progress']:.1f}%")
        
        # Show undiscovered periodicals
        undiscovered = storage.get_periodicals(discovery_complete=False)
        if undiscovered:
            click.echo(f"\n🔍 Next to discover ({len(undiscovered)} periodicals):")
            for p in undiscovered[:5]:
                click.echo(f"   📄 {p['title']} ({p['state']})")
            if len(undiscovered) > 5:
                click.echo(f"   ... and {len(undiscovered) - 5} more")
        
        # Show ready facets
        ready_facets = storage.get_search_facets(status='completed')
        if ready_facets:
            click.echo(f"\n✅ Ready for download ({len(ready_facets)} facets):")
            for facet in ready_facets[:5]:
                click.echo(f"   📅 {facet['facet_type']}: {facet['facet_value']} ({facet['actual_items']:,} items)")
            if len(ready_facets) > 5:
                click.echo(f"   ... and {len(ready_facets) - 5} more")
        
    except Exception as e:
        click.echo(f"❌ Failed to get discovery status: {e}")


@cli.command()
@click.option('--facet-type', help='Filter by facet type')
@click.option('--status', help='Filter by status')
def list_facets(facet_type, status):
    """List search facets and their status."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    
    try:
        facets = storage.get_search_facets(facet_type=facet_type, status=status)
        
        if not facets:
            click.echo("No facets found matching criteria.")
            return
        
        click.echo(f"📋 Found {len(facets)} facets:")
        
        for facet in facets:
            status_icon = {
                'pending': '⏳',
                'discovering': '🔍',
                'downloading': '⬇️',
                'completed': '✅',
                'error': '❌'
            }.get(facet['status'], '❓')
            
            click.echo(f"\n{status_icon} {facet['facet_type']}: {facet['facet_value']}")
            click.echo(f"   Status: {facet['status']}")
            click.echo(f"   Estimated: {facet['estimated_items']:,} items")
            if facet['actual_items']:
                click.echo(f"   Actual: {facet['actual_items']:,} items")
            if facet['items_discovered']:
                click.echo(f"   Discovered: {facet['items_discovered']:,} items")
            if facet['items_downloaded']:
                click.echo(f"   Downloaded: {facet['items_downloaded']:,} items")
            if facet['error_message']:
                click.echo(f"   Error: {facet['error_message']}")
        
    except Exception as e:
        click.echo(f"❌ Failed to list facets: {e}")


@cli.command()
@click.option('--status', help='Filter by status')
@click.option('--limit', default=20, help='Number of items to show')
def show_queue(status, limit):
    """Show download queue items."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    
    try:
        queue = storage.get_download_queue(status=status, limit=limit)
        
        if not queue:
            status_msg = f" with status '{status}'" if status else ""
            click.echo(f"No queue items found{status_msg}.")
            return
        
        status_msg = f" ({status})" if status else ""
        click.echo(f"📋 Download Queue{status_msg} - Top {len(queue)} items:")
        
        for i, item in enumerate(queue, 1):
            status_icon = {
                'queued': '⏳',
                'active': '🔄',
                'paused': '⏸️',
                'completed': '✅',
                'failed': '❌'
            }.get(item['status'], '❓')
            
            click.echo(f"\n{i}. {status_icon} Priority {item['priority']}: {item['queue_type']} {item['reference_id']}")
            click.echo(f"   Status: {item['status']}")
            click.echo(f"   Size: {item['estimated_size_mb']} MB")
            click.echo(f"   Time: {item['estimated_time_hours']:.1f} hours")
            if item['progress_percent'] > 0:
                click.echo(f"   Progress: {item['progress_percent']:.1f}%")
            if item['error_message']:
                click.echo(f"   Error: {item['error_message']}")
        
    except Exception as e:
        click.echo(f"❌ Failed to show queue: {e}")


if __name__ == '__main__':
    cli()
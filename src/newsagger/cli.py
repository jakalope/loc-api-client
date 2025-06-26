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
from .downloader import DownloadProcessor


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
def discover(max_papers, states):
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
            state_counts = {}
            for p in periodicals:
                state = p.get('state', 'Unknown')
                state_counts[state] = state_counts.get(state, 0) + 1
            
            click.echo(f"\n📊 Periodicals by state:")
            for state, count in sorted(state_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
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
@click.option('--estimate-items', is_flag=True, help='Estimate items per facet (makes API calls, may trigger rate limiting)')
@click.option('--rate-limit-delay', default=5.0, type=float, help='Extra delay between API calls (seconds)')
def create_facets(start_year, end_year, facet_size, estimate_items, rate_limit_delay):
    """Create date range facets for systematic downloading.
    
    Progress is saved automatically - you can safely interrupt (Ctrl+C) and resume later.
    Use 'check-facet-progress' to see current status for a date range.
    """
    config = Config()
    client = LocApiClient(**config.get_api_config())
    processor = NewsDataProcessor()
    storage = NewsStorage(**config.get_storage_config())
    discovery = DiscoveryManager(client, processor, storage)
    
    total_facets = len(range(start_year, end_year + 1, facet_size))
    
    if estimate_items:
        click.echo(f"⚠️  WARNING: Will make {total_facets} API calls for estimation")
        click.echo(f"   This may trigger rate limiting (1 hour timeout)")
        click.echo(f"   Using {rate_limit_delay}s delay between calls")
        if not click.confirm("Continue with estimation?"):
            click.echo("Cancelled. Use without --estimate-items to skip estimation.")
            return
    
    # Check for existing facets to enable resumption
    existing_facets = storage.get_search_facets(facet_type='date_range')
    existing_ranges = {f['facet_value'] for f in existing_facets}
    
    # Calculate which facets need to be created
    all_ranges = []
    for year in range(start_year, end_year + 1, facet_size):
        facet_end_year = min(year + facet_size - 1, end_year)
        facet_value = f"{year}/{facet_end_year}" if year != facet_end_year else f"{year}/{year}"
        all_ranges.append(facet_value)
    
    missing_ranges = [r for r in all_ranges if r not in existing_ranges]
    
    if existing_ranges:
        click.echo(f"🔄 Found {len(existing_ranges)} existing facets, {len(missing_ranges)} to create")
        if missing_ranges:
            click.echo(f"   Will create: {missing_ranges[0]} to {missing_ranges[-1]}")
        else:
            click.echo("✅ All facets already exist!")
            return
    
    click.echo(f"📅 Creating {len(missing_ranges)} date facets from {start_year} to {end_year} ({facet_size} year(s) each)...")
    click.echo("💡 Tip: Press Ctrl+C to safely interrupt - progress is saved automatically")
    
    try:
        facet_ids = discovery.create_date_range_facets(
            start_year, 
            end_year, 
            facet_size_years=facet_size,
            estimate_items=estimate_items,
            rate_limit_delay=rate_limit_delay if estimate_items else None
        )
        
        click.echo(f"✅ Created {len(facet_ids)} new date range facets")
        if len(facet_ids) > 0:
            click.echo(f"📊 Total facets now: {len(existing_ranges) + len(facet_ids)}")
        else:
            click.echo("📊 No new facets created (all already existed)")
            
    except KeyboardInterrupt:
        click.echo(f"\n⚠️  Interrupted by user")
        # Check how many were created before interruption
        current_facets = storage.get_search_facets(facet_type='date_range')
        current_ranges = {f['facet_value'] for f in current_facets}
        newly_created = len(current_ranges) - len(existing_ranges)
        
        if newly_created > 0:
            click.echo(f"✅ Saved {newly_created} facets before interruption")
            click.echo(f"📊 Progress: {len(current_ranges)}/{len(all_ranges)} facets created")
            click.echo("🔄 Run the same command again to resume from where you left off")
        else:
            click.echo("❌ No facets were created before interruption")
        return
        
    except Exception as e:
        click.echo(f"❌ Facet creation failed: {e}")
        return
    
    # Show sample of created facets
    if len(facet_ids) > 0:
        facets = storage.get_search_facets(facet_type='date_range')
        recent_facets = [f for f in facets if f['id'] in facet_ids]
        
        click.echo(f"\n📋 Sample of newly created facets:")
        for facet in recent_facets[:5]:  # Show first 5
            if estimate_items:
                click.echo(f"   📅 {facet['facet_value']}: ~{facet['estimated_items']:,} items")
            else:
                click.echo(f"   📅 {facet['facet_value']}: (estimation skipped)")
        
        if len(recent_facets) > 5:
            click.echo(f"   ... and {len(recent_facets)-5} more")
        
        if not estimate_items:
            click.echo(f"\n💡 Tip: Use 'newsagger estimate-facets' to get item estimates later")


@cli.command()
@click.option('--start-year', default=1900, type=int, help='Start year to check progress for')
@click.option('--end-year', default=1920, type=int, help='End year to check progress for')
@click.option('--facet-size', default=1, type=int, help='Years per facet')
def check_facet_progress(start_year, end_year, facet_size):
    """Check progress of facet creation for a date range."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    
    # Calculate expected facets
    all_ranges = []
    for year in range(start_year, end_year + 1, facet_size):
        facet_end_year = min(year + facet_size - 1, end_year)
        facet_value = f"{year}/{facet_end_year}" if year != facet_end_year else f"{year}/{year}"
        all_ranges.append(facet_value)
    
    # Check existing facets
    existing_facets = storage.get_search_facets(facet_type='date_range')
    existing_ranges = {f['facet_value'] for f in existing_facets}
    
    # Calculate progress
    completed_ranges = [r for r in all_ranges if r in existing_ranges]
    missing_ranges = [r for r in all_ranges if r not in existing_ranges]
    
    progress_percent = (len(completed_ranges) / len(all_ranges)) * 100
    
    click.echo(f"📊 Facet Creation Progress ({start_year}-{end_year})")
    click.echo(f"   Total expected: {len(all_ranges)} facets")
    click.echo(f"   Created: {len(completed_ranges)} facets ({progress_percent:.1f}%)")
    click.echo(f"   Missing: {len(missing_ranges)} facets")
    
    if missing_ranges:
        click.echo(f"\n❌ Missing ranges:")
        # Group consecutive ranges for cleaner display
        groups = []
        current_group = [missing_ranges[0]]
        
        for i in range(1, len(missing_ranges)):
            current_year = int(missing_ranges[i].split('/')[0])
            prev_year = int(missing_ranges[i-1].split('/')[0])
            
            if current_year == prev_year + facet_size:
                current_group.append(missing_ranges[i])
            else:
                groups.append(current_group)
                current_group = [missing_ranges[i]]
        groups.append(current_group)
        
        for group in groups:
            if len(group) == 1:
                click.echo(f"   📅 {group[0]}")
            else:
                click.echo(f"   📅 {group[0]} to {group[-1]} ({len(group)} facets)")
        
        click.echo(f"\n🔄 Run 'newsagger create-facets {start_year} {end_year}' to resume")
    else:
        click.echo(f"\n✅ All facets created for {start_year}-{end_year}")


@cli.command()
def status():
    """Show overall progress status of all operations."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    
    click.echo("📊 Newsagger Status Overview")
    
    # Get basic storage stats
    storage_stats = storage.get_storage_stats()
    discovery_stats = storage.get_discovery_stats()
    
    click.echo(f"\n📚 Storage:")
    click.echo(f"   Newspapers: {storage_stats['total_newspapers']:,}")
    click.echo(f"   Pages discovered: {storage_stats['total_pages']:,}")
    click.echo(f"   Pages downloaded: {storage_stats['downloaded_pages']:,}")
    click.echo(f"   Database size: {storage_stats['db_size_mb']} MB")
    
    # Periodicals discovery
    click.echo(f"\n🏛️ Periodicals:")
    click.echo(f"   Total: {discovery_stats['total_periodicals']:,}")
    click.echo(f"   Discovery complete: {discovery_stats['discovered_periodicals']:,}")
    click.echo(f"   Download complete: {discovery_stats['downloaded_periodicals']:,}")
    
    # Facets status
    click.echo(f"\n📅 Search Facets:")
    click.echo(f"   Total: {discovery_stats['total_facets']:,}")
    click.echo(f"   Completed: {discovery_stats['completed_facets']:,}")
    click.echo(f"   Errors: {discovery_stats['error_facets']:,}")
    
    # Estimate accuracy
    if discovery_stats['total_facets'] > 0:
        estimated_items = discovery_stats.get('estimated_items', 0)
        actual_items = discovery_stats.get('actual_items', 0)
        if estimated_items > 0 and actual_items > 0:
            accuracy = (actual_items / estimated_items) * 100
            click.echo(f"   Estimate accuracy: {accuracy:.1f}% ({actual_items:,}/{estimated_items:,})")
    
    # Download queue
    click.echo(f"\n📥 Download Queue:")
    click.echo(f"   Total items: {discovery_stats['total_queue_items']:,}")
    click.echo(f"   Queued: {discovery_stats['queued_items']:,}")
    click.echo(f"   Active: {discovery_stats['active_items']:,}")
    click.echo(f"   Completed: {discovery_stats['completed_queue_items']:,}")
    
    # Quick recommendations
    click.echo(f"\n💡 Quick Actions:")
    
    if discovery_stats['total_facets'] == 0:
        click.echo("   • Run 'newsagger create-facets' to create date facets")
    elif discovery_stats['completed_facets'] == 0:
        click.echo("   • Run 'newsagger auto-discover-facets' to discover content")
    elif discovery_stats['queued_items'] == 0:
        click.echo("   • Run 'newsagger auto-enqueue' to queue content for download")
    elif discovery_stats['queued_items'] > 0:
        click.echo("   • Run 'newsagger process-downloads' to start downloading")
    
    # Show current year range of facets if any exist
    facets = storage.get_search_facets(facet_type='date_range')
    if facets:
        years = []
        for facet in facets:
            start_year = facet['facet_value'].split('/')[0]
            years.append(int(start_year))
        if years:
            min_year, max_year = min(years), max(years)
            click.echo(f"   • Current facet range: {min_year}-{max_year}")
            click.echo(f"   • Use 'newsagger check-facet-progress {min_year} {max_year}' for details")


@cli.command()
@click.option('--facet-type', default='date_range', help='Type of facets to estimate (date_range, state)')
@click.option('--rate-limit-delay', default=8.0, type=float, help='Delay between API calls (seconds)')
@click.option('--max-facets', default=None, type=int, help='Maximum number of facets to estimate')
@click.option('--force-reestimate', is_flag=True, help='Re-estimate facets that already have estimates')
def estimate_facets(facet_type, rate_limit_delay, max_facets, force_reestimate):
    """Estimate item counts for existing facets that don't have estimates."""
    config = Config()
    client = LocApiClient(**config.get_api_config())
    storage = NewsStorage(**config.get_storage_config())
    
    # Get facets without estimates or force re-estimation
    all_facets = storage.get_search_facets(facet_type=facet_type)
    if force_reestimate:
        facets_to_estimate = all_facets
    else:
        facets_to_estimate = [f for f in all_facets if f['estimated_items'] == 0]
    
    if not facets_to_estimate:
        if force_reestimate:
            click.echo(f"✅ No {facet_type} facets found")
        else:
            click.echo(f"✅ All {facet_type} facets already have estimates")
            click.echo("   Use --force-reestimate to update existing estimates")
        return
    
    if max_facets:
        facets_to_estimate = facets_to_estimate[:max_facets]
    
    total_calls = len(facets_to_estimate)
    estimated_time = (total_calls * rate_limit_delay) / 60
    
    if force_reestimate:
        click.echo(f"📊 Re-estimating {total_calls} {facet_type} facets")
    else:
        click.echo(f"📊 Found {total_calls} {facet_type} facets without estimates")
    click.echo(f"⚠️  This will make {total_calls} API calls over ~{estimated_time:.1f} minutes")
    click.echo(f"   Using {rate_limit_delay}s delay between calls to avoid rate limiting")
    
    if not click.confirm("Continue with estimation?"):
        click.echo("Cancelled.")
        return
    
    try:
        with tqdm(total=total_calls, desc="Estimating facets") as pbar:
            for i, facet in enumerate(facets_to_estimate):
                pbar.set_description(f"Estimating {facet['facet_value']}")
                
                try:
                    if facet_type == 'date_range':
                        start_year, end_year = facet['facet_value'].split('/')
                        estimate = client.estimate_download_size((start_year, end_year))
                        estimated_items = estimate.get('total_pages', 0)
                    else:
                        # For other facet types, do a sample search
                        sample = client.search_pages(andtext=facet['facet_value'], rows=1)
                        estimated_items = sample.get('totalItems', 0)
                    
                    # Update the facet with the new estimate
                    # We need to update the estimated_items field in the database directly
                    # since update_facet_discovery doesn't have an estimated_items parameter
                    import sqlite3
                    with sqlite3.connect(storage.db_path) as conn:
                        conn.execute("""
                            UPDATE search_facets 
                            SET estimated_items = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        """, (estimated_items, facet['id']))
                        conn.commit()
                    
                    pbar.set_postfix(items=f"{estimated_items:,}")
                    
                    # Rate limiting delay
                    if i < total_calls - 1:  # Don't delay after last item
                        import time
                        time.sleep(rate_limit_delay)
                
                except Exception as e:
                    click.echo(f"\n⚠️ Failed to estimate {facet['facet_value']}: {e}")
                
                pbar.update(1)
        
        click.echo(f"\n✅ Estimation complete for {total_calls} facets")
        
    except Exception as e:
        click.echo(f"❌ Estimation failed: {e}")


@cli.command()
def fix_wildly_inaccurate_estimates():
    """Fix facets with wildly inaccurate estimates (21M+ items) using improved estimation."""
    config = Config()
    client = LocApiClient(**config.get_api_config())
    storage = NewsStorage(**config.get_storage_config())
    
    # Find facets with obviously wrong estimates (anything over 1 million is likely wrong)
    all_facets = storage.get_search_facets()
    bad_estimates = [f for f in all_facets if f['estimated_items'] > 1000000]
    
    if not bad_estimates:
        click.echo("✅ No facets with wildly inaccurate estimates found")
        return
    
    click.echo(f"🔍 Found {len(bad_estimates)} facets with inaccurate estimates (>1M items)")
    click.echo("These likely have the old broken estimate of ~21M items")
    
    # Show examples
    for facet in bad_estimates[:5]:
        click.echo(f"   📅 {facet['facet_value']}: {facet['estimated_items']:,} items")
    if len(bad_estimates) > 5:
        click.echo(f"   ... and {len(bad_estimates)-5} more")
    
    estimated_time = (len(bad_estimates) * 8) / 60  # 8 seconds per estimate
    click.echo(f"\n⚠️  This will make {len(bad_estimates)} API calls over ~{estimated_time:.1f} minutes")
    click.echo("   Using 8s delay between calls to avoid rate limiting")
    
    if not click.confirm("Fix these estimates?"):
        click.echo("Cancelled.")
        return
    
    try:
        import sqlite3
        import time
        
        with tqdm(total=len(bad_estimates), desc="Fixing estimates") as pbar:
            for i, facet in enumerate(bad_estimates):
                pbar.set_description(f"Fixing {facet['facet_value']}")
                
                try:
                    if facet['facet_type'] == 'date_range':
                        start_year, end_year = facet['facet_value'].split('/')
                        estimate = client.estimate_download_size((start_year, end_year))
                        new_estimate = estimate.get('total_pages', 0)
                    else:
                        # For other facet types, use basic sampling
                        new_estimate = 1000  # Conservative default for state facets
                    
                    # Update the estimate directly
                    with sqlite3.connect(storage.db_path) as conn:
                        conn.execute("""
                            UPDATE search_facets 
                            SET estimated_items = ?, updated_at = CURRENT_TIMESTAMP
                            WHERE id = ?
                        """, (new_estimate, facet['id']))
                        conn.commit()
                    
                    pbar.set_postfix(items=f"{new_estimate:,}")
                    
                    # Rate limiting delay  
                    if i < len(bad_estimates) - 1:
                        time.sleep(8.0)
                
                except Exception as e:
                    click.echo(f"\n⚠️ Failed to fix {facet['facet_value']}: {e}")
                
                pbar.update(1)
        
        click.echo(f"\n✅ Fixed estimates for {len(bad_estimates)} facets")
        click.echo("   Estimates should now be realistic (hundreds to low thousands)")
        
    except Exception as e:
        click.echo(f"❌ Fix failed: {e}")


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
@click.option('--auto-enqueue', is_flag=True, help='Automatically enqueue discovered content')
@click.option('--batch-size', default=100, help='Items per discovery batch')
@click.option('--max-items', default=None, type=int, help='Maximum items to discover per facet')
def auto_discover_facets(auto_enqueue, batch_size, max_items):
    """Systematically discover content for all pending facets."""
    config = Config()
    client = LocApiClient(**config.get_api_config())
    processor = NewsDataProcessor()
    storage = NewsStorage(**config.get_storage_config())
    discovery = DiscoveryManager(client, processor, storage)
    
    click.echo("🔍 Starting systematic facet discovery...")
    
    try:
        # Get all pending facets
        facets = storage.get_search_facets(status='pending')
        if not facets:
            click.echo("✅ No pending facets found. Create facets first with 'create-facets' command.")
            return
        
        click.echo(f"📋 Found {len(facets)} pending facets to discover")
        
        total_discovered = 0
        total_enqueued = 0
        
        with tqdm(desc="Processing facets", total=len(facets)) as pbar:
            for facet in facets:
                pbar.set_description(f"Discovering {facet['facet_type']}: {facet['facet_value']}")
                
                # Discover content for this facet
                discovered_count = discovery.discover_facet_content(
                    facet['id'], 
                    batch_size=batch_size,
                    max_items=max_items
                )
                total_discovered += discovered_count
                
                # Auto-enqueue if requested
                if auto_enqueue and discovered_count > 0:
                    enqueued = discovery.enqueue_facet_content(facet['id'])
                    total_enqueued += enqueued
                    pbar.set_postfix(discovered=total_discovered, enqueued=total_enqueued)
                else:
                    pbar.set_postfix(discovered=total_discovered)
                
                pbar.update(1)
        
        click.echo(f"\n✅ Discovery complete!")
        click.echo(f"   📄 Total items discovered: {total_discovered:,}")
        if auto_enqueue:
            click.echo(f"   ⬇️ Total items enqueued: {total_enqueued:,}")
        
        # Show updated stats
        stats = storage.get_discovery_stats()
        click.echo(f"\n📊 Updated Stats:")
        click.echo(f"   Completed facets: {stats['completed_facets']}/{stats['total_facets']}")
        click.echo(f"   Total discovered items: {stats['discovered_items']:,}")
        
        if not auto_enqueue and total_discovered > 0:
            click.echo(f"\n💡 Run 'newsagger auto-enqueue' to queue discovered content for download.")
            
    except Exception as e:
        click.echo(f"❌ Auto-discovery failed: {e}")


@cli.command()
@click.option('--year', type=int, help='Specific year to test (e.g. 1906)')
@click.option('--state', help='Specific state to test (e.g. California)')
@click.option('--max-items', default=20, type=int, help='Maximum items to discover')
def test_discovery(year, state, max_items):
    """Test discovery with a small, focused dataset."""
    config = Config()
    client = LocApiClient(**config.get_api_config())
    processor = NewsDataProcessor()
    storage = NewsStorage(**config.get_storage_config())
    discovery = DiscoveryManager(client, processor, storage)
    
    if year:
        click.echo(f"🔍 Testing discovery for year {year} (max {max_items} items)...")
        
        # Create a temporary facet for testing
        facet_id = storage.create_search_facet(
            'date_range', f'{year}/{year}', '', max_items
        )
        
        try:
            discovered = discovery.discover_facet_content(facet_id, max_items=max_items, batch_size=20)
            click.echo(f"✅ Successfully discovered {discovered} items for {year}")
            
            # Show sample of what was found
            if discovered > 0:
                pages = storage.get_pages_for_facet(facet_id)[:5]
                click.echo(f"\n📄 Sample pages found:")
                for page in pages:
                    click.echo(f"   • {page['title']} - {page['date']}")
                    
        except Exception as e:
            click.echo(f"❌ Test discovery failed: {e}")
    
    elif state:
        click.echo(f"🔍 Testing discovery for {state} newspapers (max {max_items} items)...")
        
        # First, check how many periodicals we have for this state
        periodicals = storage.get_periodicals(state=state)
        if not periodicals:
            click.echo(f"⚠️ No periodicals found for state '{state}'")
            return
        
        click.echo(f"📰 Found {len(periodicals)} periodicals in {state}")
        
        # Create a temporary facet for testing
        facet_id = storage.create_search_facet(
            'state', state, '', max_items
        )
        
        try:
            discovered = discovery.discover_facet_content(facet_id, max_items=max_items, batch_size=10)
            click.echo(f"✅ Successfully discovered {discovered} items for {state}")
            
        except Exception as e:
            click.echo(f"❌ Test discovery failed: {e}")
    
    else:
        click.echo("❌ Please specify either --year or --state for testing")


@cli.command()
@click.option('--priority-facets', help='Only enqueue specific facet IDs (comma-separated)')
@click.option('--max-size-gb', default=None, type=float, help='Maximum total queue size in GB')
@click.option('--dry-run', is_flag=True, help='Show what would be enqueued without actually doing it')
def auto_enqueue(priority_facets, max_size_gb, dry_run):
    """Automatically enqueue all discovered content for download."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    discovery = DiscoveryManager(None, None, storage)
    
    action = "Would enqueue" if dry_run else "Enqueuing"
    click.echo(f"⬇️ {action} discovered content...")
    
    try:
        # Get facets with discovered content
        if priority_facets:
            facet_ids = [int(fid.strip()) for fid in priority_facets.split(',')]
            facets = [storage.get_search_facet(fid) for fid in facet_ids]
            facets = [f for f in facets if f]  # Remove None values
        else:
            facets = storage.get_search_facets(status=['completed', 'discovering'])
            facets = [f for f in facets if f['items_discovered'] > 0]
        
        if not facets:
            click.echo("✅ No facets with discovered content found.")
            return
        
        click.echo(f"📋 Found {len(facets)} facets with discovered content")
        
        total_items = 0
        total_size_mb = 0
        items_by_facet = {}
        
        # Calculate what would be enqueued
        for facet in facets:
            if max_size_gb and (total_size_mb / 1024) >= max_size_gb:
                click.echo(f"⚠️ Reached size limit of {max_size_gb} GB")
                break
                
            discovered = facet['items_discovered'] - facet.get('items_downloaded', 0)
            if discovered > 0:
                # Estimate size (rough estimate: 1MB per item average)
                estimated_size = discovered * 1.0  # MB
                if max_size_gb and (total_size_mb + estimated_size) / 1024 > max_size_gb:
                    # Partial enqueue to stay under limit
                    remaining_mb = (max_size_gb * 1024) - total_size_mb
                    discovered = int(remaining_mb / 1.0)
                    estimated_size = remaining_mb
                
                items_by_facet[facet['id']] = {
                    'items': discovered,
                    'size_mb': estimated_size,
                    'facet': facet
                }
                total_items += discovered
                total_size_mb += estimated_size
        
        if dry_run:
            click.echo(f"\n📊 Would enqueue {total_items:,} items ({total_size_mb/1024:.1f} GB):")
            for facet_id, info in items_by_facet.items():
                facet = info['facet']
                click.echo(f"   📄 {facet['facet_type']}: {facet['facet_value']}")
                click.echo(f"      {info['items']:,} items, {info['size_mb']/1024:.1f} GB")
            return
        
        # Actually enqueue the content
        enqueued_total = 0
        with tqdm(desc="Enqueuing content", total=len(items_by_facet)) as pbar:
            for facet_id, info in items_by_facet.items():
                facet = info['facet']
                pbar.set_description(f"Enqueuing {facet['facet_type']}: {facet['facet_value']}")
                
                enqueued = discovery.enqueue_facet_content(
                    facet_id,
                    max_items=info['items']
                )
                enqueued_total += enqueued
                pbar.update(1)
        
        click.echo(f"\n✅ Enqueuing complete!")
        click.echo(f"   ⬇️ Total items enqueued: {enqueued_total:,}")
        click.echo(f"   💾 Estimated total size: {total_size_mb/1024:.1f} GB")
        
        # Show queue stats
        queue_stats = storage.get_download_queue_stats()
        click.echo(f"\n📊 Download Queue:")
        click.echo(f"   Queued items: {queue_stats.get('queued', 0):,}")
        click.echo(f"   Total estimated size: {queue_stats.get('total_size_mb', 0)/1024:.1f} GB")
        
        click.echo(f"\n💡 Run 'newsagger show-queue' to see queued downloads.")
        
    except Exception as e:
        click.echo(f"❌ Auto-enqueue failed: {e}")


@cli.command()
@click.option('--start-year', default=1900, type=int, help='Start year')
@click.option('--end-year', default=1920, type=int, help='End year')
@click.option('--states', help='Comma-separated states to focus on')
@click.option('--auto-discover', is_flag=True, help='Automatically discover content after creating facets')
@click.option('--auto-enqueue', is_flag=True, help='Automatically enqueue discovered content')
@click.option('--max-size-gb', default=10.0, type=float, help='Maximum download queue size in GB')
def setup_download_workflow(start_year, end_year, states, auto_discover, auto_enqueue, max_size_gb):
    """Set up complete automated download workflow from scratch.
    
    All progress is saved automatically - you can safely interrupt and resume.
    """
    config = Config()
    client = LocApiClient(**config.get_api_config())
    processor = NewsDataProcessor()
    storage = NewsStorage(**config.get_storage_config())
    discovery = DiscoveryManager(client, processor, storage)
    
    click.echo("🚀 Setting up automated download workflow...")
    click.echo(f"   📅 Years: {start_year} - {end_year}")
    if states:
        click.echo(f"   🗺️ States: {states}")
    click.echo(f"   💾 Max queue size: {max_size_gb} GB")
    click.echo("💡 Tip: Progress is saved automatically - you can safely interrupt (Ctrl+C) and resume")
    
    try:
        # Step 1: Discover periodicals if needed
        periodicals = storage.get_periodicals()
        if not periodicals:
            click.echo("\n🔍 Step 1: Discovering periodicals...")
            discovered_count = discovery.discover_all_periodicals()
            click.echo(f"✅ Discovered {discovered_count} periodicals")
        else:
            click.echo(f"\n✅ Step 1: Using existing {len(periodicals)} periodicals")
        
        # Step 2: Create facets (without estimation to avoid rate limiting)
        click.echo(f"\n📅 Step 2: Creating date facets...")
        facet_ids = discovery.create_date_range_facets(
            start_year, end_year, facet_size_years=1, estimate_items=False
        )
        click.echo(f"✅ Created {len(facet_ids)} date facets (estimation skipped to avoid rate limiting)")
        
        if states:
            click.echo(f"\n🗺️ Step 2b: Creating state facets...")
            state_list = [s.strip() for s in states.split(',')]
            state_facet_ids = discovery.create_state_facets(state_list)
            click.echo(f"✅ Created {len(state_facet_ids)} state facets")
            facet_ids.extend(state_facet_ids)
        
        if auto_discover:
            # Step 3: Auto-discover content
            click.echo(f"\n🔍 Step 3: Auto-discovering content...")
            # Get all pending facets and discover their content
            facets = storage.get_search_facets(status='pending')
            total_discovered = 0
            with tqdm(desc="Auto-discovering", total=len(facets)) as pbar:
                for facet in facets:
                    discovered = discovery.discover_facet_content(facet['id'], batch_size=100)
                    total_discovered += discovered
                    pbar.update(1)
            click.echo(f"✅ Auto-discovered {total_discovered:,} items")
            
        if auto_enqueue:
            # Step 4: Auto-enqueue content
            click.echo(f"\n⬇️ Step 4: Auto-enqueuing content...")
            facets = storage.get_search_facets(status=['completed', 'discovering'])
            facets = [f for f in facets if f['items_discovered'] > 0]
            total_enqueued = 0
            with tqdm(desc="Auto-enqueuing", total=len(facets)) as pbar:
                for facet in facets:
                    enqueued = discovery.enqueue_facet_content(facet['id'])
                    total_enqueued += enqueued
                    pbar.update(1)
            click.echo(f"✅ Auto-enqueued {total_enqueued:,} items")
        
        click.echo(f"\n🎉 Workflow setup complete!")
        click.echo(f"💡 Next steps:")
        if not auto_discover:
            click.echo(f"   - Run 'newsagger auto-discover-facets --auto-enqueue' to discover and queue content")
        elif not auto_enqueue:
            click.echo(f"   - Run 'newsagger auto-enqueue' to queue discovered content")
        else:
            click.echo(f"   - Run 'newsagger show-queue' to see your download queue")
            click.echo(f"   - Implement actual download logic to process the queue")
        
    except Exception as e:
        click.echo(f"❌ Workflow setup failed: {e}")


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


# ===== DOWNLOAD PROCESSING COMMANDS =====

@cli.command()
@click.option('--max-items', default=None, type=int, help='Maximum items to download')
@click.option('--max-size-mb', default=None, type=float, help='Maximum total download size in MB')
@click.option('--download-dir', default='./downloads', help='Directory to store downloaded files')
@click.option('--file-types', default='pdf,jp2,ocr,metadata', help='Comma-separated file types to download (pdf,jp2,ocr,metadata)')
@click.option('--dry-run', is_flag=True, help='Show what would be downloaded without actually doing it')
def process_downloads(max_items, max_size_mb, download_dir, file_types, dry_run):
    """Process the download queue and download files."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    client = LocApiClient(**config.get_api_config())
    
    # Parse file types
    file_types_list = [ft.strip().lower() for ft in file_types.split(',')]
    valid_types = {'pdf', 'jp2', 'ocr', 'metadata'}
    invalid_types = set(file_types_list) - valid_types
    if invalid_types:
        click.echo(f"❌ Invalid file types: {', '.join(invalid_types)}")
        click.echo(f"Valid types: {', '.join(sorted(valid_types))}")
        return
    
    downloader = DownloadProcessor(storage, client, download_dir, file_types_list)
    
    action = "Would process" if dry_run else "Processing"
    click.echo(f"📥 {action} download queue...")
    if max_items:
        click.echo(f"   📊 Max items: {max_items}")
    if max_size_mb:
        click.echo(f"   💾 Max size: {max_size_mb} MB")
    click.echo(f"   📁 Download directory: {download_dir}")
    click.echo(f"   📄 File types: {', '.join(file_types_list)}")
    
    try:
        stats = downloader.process_queue(
            max_items=max_items,
            max_size_mb=max_size_mb,
            dry_run=dry_run
        )
        
        if dry_run:
            click.echo(f"\n📊 Dry Run Results:")
            click.echo(f"   Would download: {stats.get('would_download', 0)} items")
            click.echo(f"   Estimated size: {stats.get('estimated_size_mb', 0):.1f} MB")
        else:
            click.echo(f"\n✅ Download processing complete!")
            click.echo(f"   📥 Downloaded: {stats['downloaded']} items")
            click.echo(f"   ❌ Errors: {stats['errors']}")
            click.echo(f"   ⏭️ Skipped: {stats['skipped']}")
            click.echo(f"   💾 Total size: {stats['total_size_mb']:.1f} MB")
            if 'duration_minutes' in stats:
                click.echo(f"   ⏱️ Duration: {stats['duration_minutes']:.1f} minutes")
            
            # Show updated queue stats
            queue_stats = storage.get_download_queue_stats()
            click.echo(f"\n📊 Queue Status:")
            click.echo(f"   Queued: {queue_stats.get('queued', 0)}")
            click.echo(f"   Completed: {queue_stats.get('completed', 0)}")
            click.echo(f"   Failed: {queue_stats.get('failed', 0)}")
        
    except Exception as e:
        click.echo(f"❌ Download processing failed: {e}")


@cli.command()
@click.argument('item_id')
@click.option('--download-dir', default='./downloads', help='Directory to store downloaded files')
@click.option('--file-types', default='pdf,jp2,ocr,metadata', help='Comma-separated file types to download (pdf,jp2,ocr,metadata)')
def download_page(item_id, download_dir, file_types):
    """Download a specific page by item ID."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    client = LocApiClient(**config.get_api_config())
    
    # Parse file types
    file_types_list = [ft.strip().lower() for ft in file_types.split(',')]
    valid_types = {'pdf', 'jp2', 'ocr', 'metadata'}
    invalid_types = set(file_types_list) - valid_types
    if invalid_types:
        click.echo(f"❌ Invalid file types: {', '.join(invalid_types)}")
        click.echo(f"Valid types: {', '.join(sorted(valid_types))}")
        return
    
    downloader = DownloadProcessor(storage, client, download_dir, file_types_list)
    
    click.echo(f"📥 Downloading page {item_id}...")
    
    try:
        result = downloader._download_page(item_id)
        
        if result['success']:
            if result.get('skipped'):
                click.echo(f"⏭️ Page already downloaded")
            else:
                click.echo(f"✅ Downloaded {len(result.get('files', []))} files")
                click.echo(f"   💾 Size: {result.get('size_mb', 0):.1f} MB")
                click.echo(f"   📁 Files: {', '.join(result.get('files', []))}")
        else:
            click.echo(f"❌ Download failed: {result.get('error', 'Unknown error')}")
    
    except Exception as e:
        click.echo(f"❌ Download failed: {e}")


@cli.command()
def resume_downloads():
    """Resume failed downloads by resetting them to queued status."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    client = LocApiClient(**config.get_api_config())
    downloader = DownloadProcessor(storage, client)
    
    click.echo("🔄 Resuming failed downloads...")
    
    try:
        result = downloader.resume_failed_downloads()
        
        if result['resumed'] > 0:
            click.echo(f"✅ Reset {result['resumed']} failed downloads to queued status")
            click.echo(f"💡 Run 'newsagger process-downloads' to retry them")
        else:
            click.echo("✅ No failed downloads to resume")
    
    except Exception as e:
        click.echo(f"❌ Failed to resume downloads: {e}")


@cli.command()
def reset_stuck_downloads():
    """Reset stuck active downloads back to queued status."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    client = LocApiClient(**config.get_api_config())
    downloader = DownloadProcessor(storage, client)
    
    click.echo("🔧 Resetting stuck downloads...")
    
    try:
        result = downloader.reset_stuck_downloads()
        
        if result['reset'] > 0:
            click.echo(f"✅ Reset {result['reset']} stuck downloads to queued status")
            click.echo(f"💡 Run 'newsagger process-downloads' to retry them")
        else:
            click.echo("✅ No stuck downloads to reset")
    
    except Exception as e:
        click.echo(f"❌ Failed to reset stuck downloads: {e}")


@cli.command()
@click.option('--download-dir', default='./downloads', help='Directory to check')
def download_stats(download_dir):
    """Show comprehensive download statistics."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    client = LocApiClient(**config.get_api_config())
    downloader = DownloadProcessor(storage, client, download_dir=download_dir)
    
    try:
        stats = downloader.get_download_stats()
        
        click.echo("📊 Download Statistics:")
        
        # Queue stats
        queue_stats = stats['queue_stats']
        click.echo(f"\n⬇️ Download Queue:")
        click.echo(f"   Total items: {queue_stats.get('total_items', 0)}")
        click.echo(f"   Queued: {queue_stats.get('queued', 0)}")
        click.echo(f"   Active: {queue_stats.get('active', 0)}")
        click.echo(f"   Completed: {queue_stats.get('completed', 0)}")
        click.echo(f"   Failed: {queue_stats.get('failed', 0)}")
        click.echo(f"   Total estimated size: {queue_stats.get('total_size_mb', 0)/1024:.1f} GB")
        
        # Storage stats
        storage_stats = stats['storage_stats']
        click.echo(f"\n💾 Database:")
        click.echo(f"   Total pages: {storage_stats.get('total_pages', 0):,}")
        click.echo(f"   Downloaded pages: {storage_stats.get('downloaded_pages', 0):,}")
        if storage_stats.get('total_pages', 0) > 0:
            download_pct = (storage_stats.get('downloaded_pages', 0) / storage_stats.get('total_pages', 1)) * 100
            click.echo(f"   Download progress: {download_pct:.1f}%")
        
        # Disk usage
        click.echo(f"\n💿 Local Storage:")
        click.echo(f"   Download directory: {stats['download_directory']}")
        click.echo(f"   Files on disk: {stats['files_on_disk']:,}")
        click.echo(f"   Disk usage: {stats['disk_usage_mb']:.1f} MB ({stats['disk_usage_mb']/1024:.2f} GB)")
        
    except Exception as e:
        click.echo(f"❌ Failed to get download stats: {e}")


@cli.command()
@click.option('--download-dir', default='./downloads', help='Directory to clean')
def cleanup_downloads(download_dir):
    """Clean up incomplete or corrupted download files."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    client = LocApiClient(**config.get_api_config())
    downloader = DownloadProcessor(storage, client, download_dir=download_dir)
    
    click.echo("🧹 Cleaning up incomplete downloads...")
    
    try:
        result = downloader.cleanup_incomplete_downloads()
        
        if result['cleaned_files'] > 0:
            click.echo(f"✅ Cleaned up {result['cleaned_files']} files")
            click.echo(f"   💾 Freed space: {result['freed_space_mb']:.1f} MB")
        else:
            click.echo("✅ No cleanup needed - all files appear complete")
    
    except Exception as e:
        click.echo(f"❌ Cleanup failed: {e}")


@cli.command()
@click.option('--priority', default=None, type=int, help='Only download items with this priority')
@click.option('--queue-type', help='Only download items of this type (page, facet, periodical)')
@click.option('--max-items', default=10, type=int, help='Maximum items to download')
@click.option('--download-dir', default='./downloads', help='Directory to store files')
@click.option('--file-types', default='pdf,jp2,ocr,metadata', help='Comma-separated file types to download (pdf,jp2,ocr,metadata)')
def download_priority(priority, queue_type, max_items, download_dir, file_types):
    """Download items from queue with specific priority or type."""
    config = Config()
    storage = NewsStorage(**config.get_storage_config())
    client = LocApiClient(**config.get_api_config())
    
    # Parse file types
    file_types_list = [ft.strip().lower() for ft in file_types.split(',')]
    valid_types = {'pdf', 'jp2', 'ocr', 'metadata'}
    invalid_types = set(file_types_list) - valid_types
    if invalid_types:
        click.echo(f"❌ Invalid file types: {', '.join(invalid_types)}")
        click.echo(f"Valid types: {', '.join(sorted(valid_types))}")
        return
    
    downloader = DownloadProcessor(storage, client, download_dir, file_types_list)
    
    # Filter queue items
    all_queue = storage.get_download_queue(status='queued')
    filtered_queue = []
    
    for item in all_queue:
        if priority is not None and item['priority'] != priority:
            continue
        if queue_type and item['queue_type'] != queue_type:
            continue
        filtered_queue.append(item)
    
    if not filtered_queue:
        click.echo("No matching items found in queue")
        return
    
    # Limit items
    filtered_queue = filtered_queue[:max_items]
    
    filter_desc = []
    if priority is not None:
        filter_desc.append(f"priority {priority}")
    if queue_type:
        filter_desc.append(f"type {queue_type}")
    
    filter_str = " and ".join(filter_desc) if filter_desc else "all criteria"
    click.echo(f"📥 Downloading {len(filtered_queue)} items matching {filter_str}...")
    
    try:
        # Temporarily modify queue to only include filtered items
        # Mark others as paused temporarily
        paused_items = []
        for item in all_queue:
            if item not in filtered_queue and item['status'] == 'queued':
                storage.update_queue_item(item['id'], status='paused')
                paused_items.append(item['id'])
        
        # Process the filtered downloads
        stats = downloader.process_queue(max_items=len(filtered_queue))
        
        # Restore paused items
        for item_id in paused_items:
            storage.update_queue_item(item_id, status='queued')
        
        click.echo(f"\n✅ Priority download complete!")
        click.echo(f"   📥 Downloaded: {stats['downloaded']} items")
        click.echo(f"   ❌ Errors: {stats['errors']}")
        click.echo(f"   💾 Total size: {stats['total_size_mb']:.1f} MB")
        
    except Exception as e:
        click.echo(f"❌ Priority download failed: {e}")


if __name__ == '__main__':
    cli()
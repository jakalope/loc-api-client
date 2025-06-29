#!/usr/bin/env python3
"""
Consolidated Batch Audit Tool

Single tool for comprehensive batch discovery and download analysis.
Shows both discovery completion and download status per batch.
"""

import sys
import argparse
from pathlib import Path
from typing import Dict, List

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from newsagger.storage import NewsStorage
from newsagger.rate_limited_client import LocApiClient
from newsagger.batch_utils import BatchMapper, BatchSessionTracker
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.text import Text


class ConsolidatedBatchAuditor:
    """Consolidated audit tool for batch discovery and downloads."""
    
    def __init__(self, db_path: str, downloads_dir: str = "downloads"):
        """Initialize auditor."""
        self.storage = NewsStorage(db_path)
        self.api_client = LocApiClient()
        self.batch_mapper = BatchMapper(self.storage, self.api_client)
        self.session_tracker = BatchSessionTracker(self.storage)
        self.downloads_dir = downloads_dir
        self.console = Console()
    
    def audit_all_batches(self) -> Dict[str, Dict]:
        """Audit all batches from discovery sessions."""
        self.console.print("[cyan]Finding batches from discovery sessions and pages...[/cyan]")
        
        session_batches = self.batch_mapper.get_session_batches()
        batch_names = self.batch_mapper.get_all_session_batch_names()
        
        if not batch_names:
            self.console.print("[yellow]No batches found in discovery sessions.[/yellow]")
            return {}
        
        self.console.print(f"[green]Found {len(batch_names)} unique batches[/green]")
        
        # Show what we found
        if hasattr(self, 'debug') and self.debug:
            self.console.print(f"[dim]Batch names: {', '.join(batch_names)}[/dim]")
        
        # Show session context
        if session_batches:
            self._show_session_context(session_batches)
        
        # Audit all batches
        self.console.print(f"\n[cyan]Auditing {len(batch_names)} batches...[/cyan]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console
        ) as progress:
            
            task = progress.add_task("Analyzing batches...", total=len(batch_names))
            results = {}
            
            for batch_name in batch_names:
                results[batch_name] = self.batch_mapper.get_batch_download_status(
                    batch_name, self.downloads_dir
                )
                progress.update(task, advance=1)
        
        return results
    
    def audit_specific_batches(self, batch_names: List[str]) -> Dict[str, Dict]:
        """Audit specific batches."""
        self.console.print(f"[cyan]Auditing {len(batch_names)} specified batches...[/cyan]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=self.console
        ) as progress:
            
            task = progress.add_task("Analyzing batches...", total=len(batch_names))
            results = {}
            
            for batch_name in batch_names:
                results[batch_name] = self.batch_mapper.get_batch_download_status(
                    batch_name, self.downloads_dir
                )
                progress.update(task, advance=1)
        
        return results
    
    def _show_session_context(self, session_batches: List[Dict]):
        """Show batch discovery session context."""
        if not session_batches:
            return
        
        session_table = Table(title="Batch Discovery Sessions", show_header=True)
        session_table.add_column("Session", style="cyan")
        session_table.add_column("Current Batch", style="yellow") 
        session_table.add_column("Progress", justify="center")
        session_table.add_column("Status", justify="center")
        session_table.add_column("Last Updated", style="dim")
        
        for batch_info in session_batches:
            # Calculate batch progress
            current_idx = batch_info.get('current_batch_index', 0) 
            total_batches = batch_info.get('total_batches', 0)
            if total_batches > 0:
                progress_text = f"{current_idx + 1}/{total_batches}"
            else:
                progress_text = "N/A"
            
            # Status styling
            status = batch_info['status']
            if status == 'active':
                status_display = f"[green]{status.upper()}[/green]"
            elif status == 'captcha_blocked':
                status_display = f"[red]{status.upper()}[/red]"
            elif status == 'completed':
                status_display = f"[blue]{status.upper()}[/blue]"
            else:
                status_display = status.upper()
            
            # Format timestamp
            updated = batch_info.get('updated_at', '')
            if updated:
                updated_short = updated.split('.')[0].replace('T', ' ')[-8:]  # Last 8 chars (HH:MM:SS)
            else:
                updated_short = 'Unknown'
            
            session_table.add_row(
                batch_info['session_name'],
                batch_info['current_batch_name'] or 'None',
                progress_text,
                status_display,
                updated_short
            )
        
        self.console.print(session_table)
    
    def create_comprehensive_report(self, results: Dict[str, Dict]):
        """Create comprehensive audit report."""
        if not results:
            self.console.print("[yellow]No batch results to display.[/yellow]")
            return
        
        # Show overall batch progress first
        self._show_overall_batch_progress(results)
        
        # Calculate summary statistics
        total_batches = len(results)
        discovery_complete = sum(1 for r in results.values() if r.get('is_discovery_complete', False))
        download_complete = sum(1 for r in results.values() if r.get('is_download_complete', False))
        both_complete = sum(1 for r in results.values() 
                           if r.get('is_discovery_complete', False) and r.get('is_download_complete', False))
        error_batches = sum(1 for r in results.values() if 'error' in r)
        
        # Aggregate totals
        total_expected_pages = sum(r.get('expected_pages', 0) for r in results.values() if 'expected_pages' in r)
        total_discovered_pages = sum(r.get('discovered_pages', 0) for r in results.values() if 'discovered_pages' in r)
        total_downloaded_pages = sum(r.get('downloaded_pages', 0) for r in results.values() if 'downloaded_pages' in r)
        total_filesystem_files = sum(r.get('filesystem_files', 0) for r in results.values() if 'filesystem_files' in r)
        total_filesystem_size = sum(r.get('filesystem_size_mb', 0) for r in results.values() if 'filesystem_size_mb' in r)
        
        # Calculate percentages
        overall_discovery = (total_discovered_pages / total_expected_pages * 100) if total_expected_pages > 0 else 0
        overall_download_of_discovered = (total_downloaded_pages / total_discovered_pages * 100) if total_discovered_pages > 0 else 0
        overall_download_of_expected = (total_downloaded_pages / total_expected_pages * 100) if total_expected_pages > 0 else 0
        
        # Summary panel
        summary_lines = [
            f"[cyan]Batches Analyzed:[/cyan] {total_batches}",
            f"[green]Discovery Complete:[/green] {discovery_complete}",
            f"[blue]Download Complete:[/blue] {download_complete}",
            f"[bold green]Both Complete:[/bold green] {both_complete}",
            ""
        ]
        
        if error_batches > 0:
            summary_lines.append(f"[red]Errors:[/red] {error_batches}")
            summary_lines.append("")
        
        summary_lines.extend([
            f"[cyan]Expected Pages:[/cyan] {total_expected_pages:,}",
            f"[yellow]Discovered:[/yellow] {total_discovered_pages:,} ({overall_discovery:.1f}%)",
            f"[green]Downloaded (DB):[/green] {total_downloaded_pages:,} ({overall_download_of_discovered:.1f}% of discovered)",
            f"[blue]Files on Disk:[/blue] {total_filesystem_files:,} ({total_filesystem_size:.1f} MB)"
        ])
        
        self.console.print(Panel("\n".join(summary_lines), title="Batch Audit Summary"))
        
        # Detailed table
        table = Table(title="Batch Discovery and Download Status", show_lines=True)
        table.add_column("Batch", style="cyan", width=18)
        table.add_column("Discovery", justify="center", width=10)
        table.add_column("Download", justify="center", width=10)
        table.add_column("Issues", justify="center", width=12)
        table.add_column("Pages", justify="center", width=14)
        table.add_column("Files", justify="center", width=12)
        table.add_column("Status", justify="center", width=12)
        
        # Sort by completion (discovery + download)
        def sort_key(item):
            batch_name, result = item
            if 'error' in result:
                return -1
            discovery_pct = result.get('discovery_page_pct', 0)
            download_pct = result.get('download_pct_of_discovered', 0)
            return discovery_pct + download_pct
        
        sorted_results = sorted(results.items(), key=sort_key, reverse=True)
        
        for batch_name, result in sorted_results:
            if 'error' in result:
                table.add_row(
                    batch_name[:17],
                    "[red]ERROR[/red]",
                    "[red]ERROR[/red]", 
                    "[red]ERROR[/red]",
                    "[red]ERROR[/red]",
                    "[red]ERROR[/red]",
                    f"[red]API Error[/red]"
                )
                continue
            
            # Discovery percentage
            discovery_pct = result.get('discovery_page_pct', 0)
            if discovery_pct >= 99:
                discovery_text = f"[green]{discovery_pct:.1f}%[/green]"
            elif discovery_pct >= 50:
                discovery_text = f"[yellow]{discovery_pct:.1f}%[/yellow]"
            else:
                discovery_text = f"[red]{discovery_pct:.1f}%[/red]"
            
            # Download percentage
            download_pct = result.get('download_pct_of_discovered', 0)
            if download_pct >= 99:
                download_text = f"[green]{download_pct:.1f}%[/green]"
            elif download_pct >= 50:
                download_text = f"[yellow]{download_pct:.1f}%[/yellow]"
            else:
                download_text = f"[red]{download_pct:.1f}%[/red]"
            
            # Issues
            discovered_issues = result.get('discovered_issues', 0)
            expected_issues = result.get('expected_issues', 0)
            issues_text = f"{discovered_issues}/{expected_issues}"
            
            # Pages
            discovered_pages = result.get('discovered_pages', 0)
            expected_pages = result.get('expected_pages', 0)
            pages_text = f"{discovered_pages:,}/{expected_pages:,}"
            
            # Files on disk
            filesystem_files = result.get('filesystem_files', 0)
            filesystem_size = result.get('filesystem_size_mb', 0)
            if filesystem_size > 1000:
                files_text = f"{filesystem_files:,}\n({filesystem_size/1024:.1f}GB)"
            else:
                files_text = f"{filesystem_files:,}\n({filesystem_size:.0f}MB)"
            
            # Overall status
            is_discovery_complete = result.get('is_discovery_complete', False)
            is_download_complete = result.get('is_download_complete', False)
            
            if is_discovery_complete and is_download_complete:
                status = "[bold green]COMPLETE[/bold green]"
            elif is_discovery_complete and filesystem_files > 0:
                status = "[green]DISCOVERED\n[blue]+FILES[/blue][/green]"
            elif is_discovery_complete:
                status = "[blue]DISCOVERED[/blue]"
            elif discovery_pct > 0:
                status = "[yellow]PARTIAL[/yellow]"
            else:
                status = "[red]NOT STARTED[/red]"
            
            table.add_row(
                batch_name[:17],
                discovery_text,
                download_text,
                issues_text,
                pages_text,
                files_text,
                status
            )
        
        self.console.print("\n")
        self.console.print(table)
        
        # Show download summary
        download_summary = self.batch_mapper.get_download_summary(self.downloads_dir)
        if download_summary['total_files'] > 0:
            self.console.print(f"\n[cyan]Download Directory Summary ({self.downloads_dir}):[/cyan]")
            self.console.print(f"  [green]{download_summary['total_files']:,} files[/green] across [yellow]{download_summary['lccn_count']} LCCNs[/yellow]")
            self.console.print(f"  [blue]{download_summary['total_size_mb']:.1f} MB[/blue] ([bold blue]{download_summary['total_size_mb']/1024:.1f} GB[/bold blue])")
            
            # Show top LCCNs by size
            if download_summary['lccn_details']:
                sorted_lccns = sorted(
                    download_summary['lccn_details'].items(), 
                    key=lambda x: x[1]['size_mb'], 
                    reverse=True
                )
                
                self.console.print(f"\n[cyan]Top LCCNs by size:[/cyan]")
                for lccn, details in sorted_lccns[:8]:  # Show top 8
                    size_gb = details['size_mb'] / 1024
                    files = details['files']
                    if size_gb >= 1:
                        self.console.print(f"  [yellow]{lccn}[/yellow]: {files:,} files ({size_gb:.1f} GB)")
                    else:
                        self.console.print(f"  [yellow]{lccn}[/yellow]: {files:,} files ({details['size_mb']:.0f} MB)")
                
                if len(sorted_lccns) > 8:
                    remaining = len(sorted_lccns) - 8
                    self.console.print(f"  [dim]... and {remaining} more LCCNs[/dim]")
    
    def _show_overall_batch_progress(self, results: Dict[str, Dict]):
        """Show overall progress toward processing all available batches."""
        # Get total batches available from API
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("Fetching all available batches from API..."),
                console=self.console
            ) as progress:
                progress.add_task("Loading...", total=None)
                all_batches = list(self.api_client.get_all_batches())
            
            total_available = len(all_batches)
            currently_processed = len(results)
            
            # Count completion status
            fully_discovered = sum(1 for r in results.values() if r.get('is_discovery_complete', False))
            with_downloads = sum(1 for r in results.values() if r.get('filesystem_files', 0) > 0)
            
            # Calculate percentages
            batch_progress_pct = (currently_processed / total_available * 100) if total_available > 0 else 0
            discovery_completion_pct = (fully_discovered / total_available * 100) if total_available > 0 else 0
            
            # Create progress visualization
            progress_text = Text()
            progress_text.append("Batch Discovery Progress: ", style="cyan")
            progress_text.append(f"{currently_processed}/{total_available}", style="bold yellow")
            progress_text.append(f" ({batch_progress_pct:.1f}%)", style="yellow")
            
            # Show progress bar representation
            bar_width = 40
            filled = int(batch_progress_pct / 100 * bar_width)
            bar = "█" * filled + "░" * (bar_width - filled)
            
            progress_lines = [
                f"[cyan]📊 Overall Batch Progress[/cyan]",
                "",
                f"[white]{bar}[/white]",
                f"[yellow]Batches Started:[/yellow] {currently_processed}/{total_available} ({batch_progress_pct:.1f}%)",
                f"[green]Fully Discovered:[/green] {fully_discovered}/{total_available} ({discovery_completion_pct:.1f}%)",
                f"[blue]With Downloads:[/blue] {with_downloads}/{total_available}",
                "",
                f"[dim]Remaining to discover: {total_available - currently_processed} batches[/dim]"
            ]
            
            # Estimate completion time if we have active sessions
            active_sessions = self.session_tracker.get_active_sessions()
            if active_sessions:
                # Calculate average discovery rate
                total_discovered_pages = sum(r.get('discovered_pages', 0) for r in results.values())
                avg_pages_per_batch = total_discovered_pages / currently_processed if currently_processed > 0 else 0
                remaining_batches = total_available - currently_processed
                
                if avg_pages_per_batch > 0:
                    # Get current rate from most recent session
                    latest_session = max(active_sessions, key=lambda s: s.get('updated_at', ''))
                    session_details = self.session_tracker.get_session_progress(latest_session['session_name'])
                    
                    if session_details and session_details.get('pages_per_hour', 0) > 0:
                        estimated_hours_per_batch = avg_pages_per_batch / session_details['pages_per_hour']
                        estimated_total_hours = estimated_hours_per_batch * remaining_batches
                        
                        if estimated_total_hours < 24:
                            eta_text = f"{estimated_total_hours:.1f} hours"
                        else:
                            eta_text = f"{estimated_total_hours/24:.1f} days"
                        
                        progress_lines.append(f"[dim]Estimated time to complete all batches: ~{eta_text}[/dim]")
            
            self.console.print(Panel("\n".join(progress_lines), title="🎯 Progress Toward All 25 Batches"))
            self.console.print()
            
        except Exception as e:
            # Fallback if API call fails
            progress_lines = [
                f"[cyan]📊 Batch Progress (API unavailable)[/cyan]",
                "",
                f"[yellow]Batches Processed:[/yellow] {len(results)}",
                f"[green]Fully Discovered:[/green] {sum(1 for r in results.values() if r.get('is_discovery_complete', False))}",
                f"[blue]With Downloads:[/blue] {sum(1 for r in results.values() if r.get('filesystem_files', 0) > 0)}",
                "",
                f"[red]Note:[/red] Could not fetch total batch count from API: {str(e)[:100]}"
            ]
            
            self.console.print(Panel("\n".join(progress_lines), title="📊 Batch Progress"))
            self.console.print()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Consolidated batch discovery and download audit tool"
    )
    parser.add_argument(
        '--db-path',
        type=str, 
        default='data/newsagger.db',
        help='Path to database file (default: data/newsagger.db)'
    )
    parser.add_argument(
        '--downloads-dir',
        type=str,
        default='downloads',
        help='Path to downloads directory (default: downloads)'
    )
    parser.add_argument(
        '--batches',
        type=str,
        nargs='+',
        help='Specific batch names to audit'
    )
    parser.add_argument(
        '--sessions-only',
        action='store_true',
        help='Show only session information, no batch analysis'
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Show debug information about batch discovery'
    )
    parser.add_argument(
        '--from-log',
        type=str,
        nargs='+',
        help='Audit batches from your log output (e.g., nbu_bluejay_ver02 dlc_ferguson_ver01)'
    )
    parser.add_argument(
        '--show-progress',
        action='store_true',
        help='Show overall progress toward processing all available batches'
    )
    
    args = parser.parse_args()
    
    # Check if database exists
    if not Path(args.db_path).exists():
        print(f"Error: Database file '{args.db_path}' not found.")
        sys.exit(1)
    
    # Create auditor
    auditor = ConsolidatedBatchAuditor(args.db_path, args.downloads_dir)
    auditor.debug = args.debug
    
    if args.show_progress:
        # Show only overall progress
        results = auditor.audit_all_batches()
        auditor._show_overall_batch_progress(results)
        
    elif args.from_log:
        # Audit batches from log output
        results = auditor.audit_specific_batches(args.from_log)
        auditor.create_comprehensive_report(results)
        
    elif args.sessions_only:
        # Show only session context
        session_batches = auditor.batch_mapper.get_session_batches()
        auditor._show_session_context(session_batches)
        
    elif args.batches:
        # Audit specific batches
        results = auditor.audit_specific_batches(args.batches)
        auditor.create_comprehensive_report(results)
        
    else:
        # Audit all session batches
        results = auditor.audit_all_batches()
        auditor.create_comprehensive_report(results)


if __name__ == '__main__':
    main()
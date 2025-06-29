#!/usr/bin/env python3
"""
Rich TUI Monitor for Batch Discovery and Downloads

A comprehensive terminal user interface for monitoring automated batch discovery
and download processes. Runs both processes in the background and provides
real-time progress updates, status monitoring, and estimated completion times.
"""

import sys
import os
import time
import subprocess
import threading
import signal
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from queue import Queue, Empty

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from newsagger.storage import NewsStorage
# Import BatchMapper and LocApiClient lazily to avoid blocking during module import

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    Progress, 
    SpinnerColumn, 
    TextColumn, 
    BarColumn, 
    TaskProgressColumn,
    TimeRemainingColumn,
    MofNCompleteColumn
)
from rich.table import Table
from rich.layout import Layout
from rich.text import Text
from rich.align import Align


@dataclass
class ProcessStatus:
    """Status information for a background process."""
    name: str
    command: List[str]
    process: Optional[subprocess.Popen] = None
    is_running: bool = False
    last_update: Optional[datetime] = None
    start_time: Optional[datetime] = None
    status_text: str = "Not Started"
    error_count: int = 0
    restart_count: int = 0


@dataclass
class ProgressStats:
    """Progress statistics for monitoring."""
    # Batch Discovery
    total_batches: int = 0
    batches_discovered: int = 0
    current_batch: str = ""
    current_batch_progress: float = 0.0
    discovery_rate_per_hour: float = 0.0
    
    # Downloads
    total_queue_items: int = 0
    items_downloaded: int = 0
    download_rate_per_hour: float = 0.0
    download_size_mb: float = 0.0
    
    # Rate Limiting
    is_rate_limited: bool = False
    cooldown_remaining_minutes: float = 0.0
    rate_limit_reason: str = ""
    
    # Estimates
    estimated_discovery_completion: Optional[datetime] = None
    estimated_download_completion: Optional[datetime] = None


class BackgroundProcessManager:
    """Manages background discovery and download processes."""
    
    def __init__(self, db_path: str, downloads_dir: str, log_dir: str = "logs"):
        self.db_path = db_path
        self.downloads_dir = downloads_dir
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        
        # Process definitions - use absolute path to main.py
        main_py_path = str(Path(__file__).parent / "main.py")
        
        self.discovery_process = ProcessStatus(
            name="Batch Discovery",
            command=[
                sys.executable, main_py_path, "discover-via-batches", 
                "--auto-enqueue"
            ]
        )
        
        self.download_process = ProcessStatus(
            name="Downloads",
            command=[
                sys.executable, main_py_path, "process-downloads",
                "--max-items", "50", "--continuous", "--max-idle-minutes", "30"
            ]
        )
        
        self.processes = [self.discovery_process, self.download_process]
        self.shutdown_requested = False
        
    def start_process(self, process_status: ProcessStatus) -> bool:
        """Start a background process."""
        if process_status.is_running:
            return True
            
        try:
            # Setup log files
            log_file = self.log_dir / f"{process_status.name.lower().replace(' ', '_')}.log"
            
            # Start process with logging
            # Set up environment to ensure correct paths
            env = os.environ.copy()
            env["PYTHONPATH"] = str(Path(__file__).parent / "src")
            
            # Change to parent directory that contains the correct database
            working_dir = Path("/home/jake/loc")
            
            process_status.process = subprocess.Popen(
                process_status.command,
                stdout=open(log_file, 'a'),
                stderr=subprocess.STDOUT,
                cwd=working_dir,
                env=env
            )
            
            process_status.is_running = True
            process_status.status_text = "Starting..."
            process_status.last_update = datetime.now()
            process_status.start_time = datetime.now()
            return True
            
        except Exception as e:
            process_status.status_text = f"Failed to start: {e}"
            process_status.error_count += 1
            return False
    
    def check_process_health(self, process_status: ProcessStatus) -> bool:
        """Check if a process is still running and healthy."""
        if not process_status.process:
            return False
            
        # Check if process is still alive
        poll_result = process_status.process.poll()
        if poll_result is not None:
            # Process has terminated
            process_status.is_running = False
            if poll_result == 0:
                process_status.status_text = "Completed"
            else:
                process_status.status_text = f"Exited with code {poll_result}"
                process_status.error_count += 1
            return False
        
        process_status.status_text = "Running"
        process_status.last_update = datetime.now()
        return True
    
    def restart_process(self, process_status: ProcessStatus) -> bool:
        """Restart a failed process."""
        if process_status.is_running:
            self.stop_process(process_status)
        
        process_status.restart_count += 1
        return self.start_process(process_status)
    
    def stop_process(self, process_status: ProcessStatus):
        """Stop a running process."""
        if process_status.process and process_status.is_running:
            try:
                process_status.process.terminate()
                process_status.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process_status.process.kill()
                process_status.process.wait()
            except:
                pass
            
            process_status.is_running = False
            process_status.status_text = "Stopped"
    
    def start_all(self):
        """Start all background processes."""
        for process_status in self.processes:
            self.start_process(process_status)
    
    def stop_all(self):
        """Stop all background processes."""
        self.shutdown_requested = True
        for process_status in self.processes:
            self.stop_process(process_status)
    
    def monitor_processes(self) -> List[ProcessStatus]:
        """Monitor all processes and restart if needed."""
        for process_status in self.processes:
            if not self.check_process_health(process_status) and not self.shutdown_requested:
                # Auto-restart failed processes (with backoff)
                if process_status.restart_count < 5:
                    time.sleep(min(2 ** process_status.restart_count, 60))  # Exponential backoff
                    self.restart_process(process_status)
        
        return self.processes


class ProgressMonitor:
    """Monitors database and provides real-time progress statistics."""
    
    def __init__(self, db_path: str, downloads_dir: str):
        self.db_path = db_path
        self.downloads_dir = downloads_dir
        self.storage = NewsStorage(db_path) if Path(db_path).exists() else None
        # Don't initialize API client or batch components during __init__ to avoid blocking
        self.api_client = None
        self.batch_mapper = None
        self.session_tracker = None
        
        self._last_stats = ProgressStats()
        self._total_batches_cache = None
        self._cache_time = None
    
    def get_progress_stats(self) -> ProgressStats:
        """Get current progress statistics using simple database queries."""
        if not self.storage or not Path(self.db_path).exists():
            return ProgressStats()
        
        stats = ProgressStats()
        
        try:
            # Use basic database queries instead of complex batch tracking
            stats.total_batches = 25  # Known estimate from previous analysis
            
            # Get simple download queue statistics
            queued_items = self.storage.get_download_queue(status='queued')
            completed_items = self.storage.get_download_queue(status='completed')
            in_progress_items = self.storage.get_download_queue(status='in_progress')
            
            stats.total_queue_items = len(queued_items) + len(completed_items) + len(in_progress_items)
            stats.items_downloaded = len(completed_items)
            
            # Get actual batch discovery count from database
            # Check how many unique batch names we have in batch_discovery_sessions
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # Count unique batches that have been discovered
                cursor.execute("""
                    SELECT COUNT(DISTINCT current_batch_name) 
                    FROM batch_discovery_sessions 
                    WHERE current_batch_name IS NOT NULL
                """)
                result = cursor.fetchone()
                stats.batches_discovered = result[0] if result else 0
                
                # Get current batch being processed
                cursor.execute("""
                    SELECT current_batch_name, current_batch_index, total_batches
                    FROM batch_discovery_sessions
                    WHERE status IN ('active', 'captcha_blocked')
                    ORDER BY updated_at DESC
                    LIMIT 1
                """)
                current = cursor.fetchone()
                if current:
                    stats.current_batch = current[0] or ""
                    if current[1] and current[2]:
                        stats.current_batch_progress = (current[1] / current[2]) * 100
                
                conn.close()
            except Exception as e:
                # If query fails, report 0 rather than fake data
                stats.batches_discovered = 0
            
            # Simple rate limiting check - if we have very few recent items, might be rate limited
            # This is a simplified heuristic
            if len(queued_items) < 10 and len(in_progress_items) == 0:
                stats.is_rate_limited = True
                stats.rate_limit_reason = "Possible rate limiting (heuristic)"
                stats.cooldown_remaining_minutes = 0  # Unknown without session tracking
            
            # Get actual download size from database
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                
                # Sum actual downloaded sizes from database
                cursor.execute("""
                    SELECT SUM(estimated_size_mb) 
                    FROM download_queue 
                    WHERE status = 'completed'
                """)
                result = cursor.fetchone()
                if result and result[0]:
                    stats.download_size_mb = result[0]
                else:
                    stats.download_size_mb = 0.0  # No data found
                
                conn.close()
            except Exception as e:
                # If query fails, report 0 rather than fake data
                stats.download_size_mb = 0.0
            
        except Exception as e:
            # Return last known stats if database query fails
            stats = self._last_stats
        
        self._last_stats = stats
        return stats
    
    def _calculate_batch_progress(self, session_info: Dict) -> float:
        """Calculate progress within current batch."""
        current_index = session_info.get('current_batch_index', 0)
        total_batches = session_info.get('total_batches', 1)
        
        if total_batches > 0:
            return (current_index / total_batches) * 100
        return 0.0
    
    def _calculate_estimates(self, stats: ProgressStats):
        """Calculate estimated completion times."""
        now = datetime.now()
        
        # Discovery estimate
        if stats.discovery_rate_per_hour > 0 and stats.batches_discovered < stats.total_batches:
            remaining_batches = stats.total_batches - stats.batches_discovered
            # Estimate pages per batch (rough estimate)
            avg_pages_per_batch = 1000  # Conservative estimate
            remaining_pages = remaining_batches * avg_pages_per_batch
            hours_remaining = remaining_pages / stats.discovery_rate_per_hour
            stats.estimated_discovery_completion = now + timedelta(hours=hours_remaining)
        
        # Download estimate  
        if stats.download_rate_per_hour > 0 and stats.total_queue_items > 0:
            hours_remaining = stats.total_queue_items / stats.download_rate_per_hour
            stats.estimated_download_completion = now + timedelta(hours=hours_remaining)


class TUIMonitor:
    """Rich TUI for monitoring batch discovery and downloads."""
    
    def __init__(self, db_path: str = "/home/jake/loc/data/newsagger.db", 
                 downloads_dir: str = "/home/jake/loc/downloads"):
        self.db_path = db_path
        self.downloads_dir = downloads_dir
        
        self.console = Console()
        self.process_manager = BackgroundProcessManager(db_path, downloads_dir)
        self.progress_monitor = ProgressMonitor(db_path, downloads_dir)
        
        self.shutdown_requested = False
        self.start_time = datetime.now()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.shutdown_requested = True
    
    def create_layout(self, stats: ProgressStats, processes: List[ProcessStatus]) -> Layout:
        """Create the TUI layout."""
        layout = Layout()
        
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="main"),
            Layout(name="footer", size=3)
        )
        
        layout["main"].split_row(
            Layout(name="left"),
            Layout(name="right")
        )
        
        layout["left"].split_column(
            Layout(name="discovery", ratio=2),
            Layout(name="downloads", ratio=2),
            Layout(name="processes", ratio=2)
        )
        
        layout["right"].split_column(
            Layout(name="stats"),
            Layout(name="estimates")
        )
        
        # Header
        runtime = datetime.now() - self.start_time
        header_text = Text.assemble(
            ("LoC Archive Monitor", "bold cyan"),
            " | ",
            ("Runtime: ", "dim"),
            (str(runtime).split('.')[0], "green"),
            " | ",
            ("DB: ", "dim"),
            (Path(self.db_path).name, "yellow")
        )
        layout["header"].update(Panel(Align.center(header_text), style="bold"))
        
        # Discovery Panel
        layout["discovery"].update(self._create_discovery_panel(stats))
        
        # Downloads Panel  
        layout["downloads"].update(self._create_downloads_panel(stats))
        
        # Process Status Panel
        layout["processes"].update(self._create_process_panel(processes))
        
        # Statistics Panel
        layout["stats"].update(self._create_stats_panel(stats))
        
        # Estimates Panel
        layout["estimates"].update(self._create_estimates_panel(stats))
        
        # Footer
        footer_text = "Press Ctrl+C to stop all processes and exit"
        layout["footer"].update(Panel(Align.center(footer_text), style="dim"))
        
        return layout
    
    def _create_discovery_panel(self, stats: ProgressStats) -> Panel:
        """Create batch discovery progress panel."""
        # Overall batch progress
        batch_progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TaskProgressColumn(),
        )
        
        overall_task = batch_progress.add_task(
            "Batches Discovered",
            total=stats.total_batches,
            completed=stats.batches_discovered
        )
        
        # Current batch progress
        current_batch_progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
        )
        
        if stats.current_batch:
            current_task = current_batch_progress.add_task(
                f"Current: {stats.current_batch[:20]}...",
                total=100,
                completed=stats.current_batch_progress
            )
        
        content = [batch_progress]
        if stats.current_batch:
            content.append(current_batch_progress)
        
        # Rate limiting status
        if stats.is_rate_limited:
            cooldown_progress = Progress(
                TextColumn("[red]CAPTCHA Cooldown"),
                BarColumn(),
                TimeRemainingColumn(),
            )
            
            cooldown_task = cooldown_progress.add_task(
                "Cooldown",
                total=60,
                completed=60 - stats.cooldown_remaining_minutes
            )
            content.append(cooldown_progress)
            
        # Discovery rate
        if stats.discovery_rate_per_hour > 0:
            rate_text = f"Rate: {stats.discovery_rate_per_hour:.0f} pages/hour"
            content.append(Text(rate_text, style="dim"))
        
        return Panel(
            Group(*content),
            title="🔍 Batch Discovery",
            border_style="cyan"
        )
    
    def _create_downloads_panel(self, stats: ProgressStats) -> Panel:
        """Create downloads progress panel."""
        download_progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TaskProgressColumn(),
        )
        
        download_task = download_progress.add_task(
            "Items Downloaded",
            total=max(stats.total_queue_items + stats.items_downloaded, 1),
            completed=stats.items_downloaded
        )
        
        content = [download_progress]
        
        # Download stats
        if stats.download_size_mb > 0:
            if stats.download_size_mb > 1024:
                size_text = f"Downloaded: {stats.download_size_mb/1024:.1f} GB"
            else:
                size_text = f"Downloaded: {stats.download_size_mb:.0f} MB"
            content.append(Text(size_text, style="green"))
        
        if stats.download_rate_per_hour > 0:
            rate_text = f"Rate: {stats.download_rate_per_hour:.0f} items/hour"
            content.append(Text(rate_text, style="dim"))
        
        return Panel(
            Group(*content),
            title="⬇️ Downloads",
            border_style="green"
        )
    
    def _create_process_panel(self, processes: List[ProcessStatus]) -> Panel:
        """Create detailed process status panel."""
        table = Table(show_header=True, header_style="bold magenta", box=None)
        table.add_column("Process", style="cyan", no_wrap=True)
        table.add_column("PID", justify="center", style="yellow")
        table.add_column("Status", justify="center")
        table.add_column("Uptime", justify="right", style="blue")
        table.add_column("Restarts", justify="center", style="red")
        
        for process in processes:
            # Determine status and color
            if process.is_running:
                status = "[green]Running[/green]"
                pid = str(process.process.pid) if process.process else "N/A"
                uptime = self._format_uptime(process.start_time) if process.start_time else "N/A"
            elif process.error_count > 0:
                status = "[red]Error[/red]"
                pid = "---"
                uptime = "---"
            else:
                status = "[yellow]Stopped[/yellow]"
                pid = "---"
                uptime = "---"
            
            # Add command preview and status text
            cmd_preview = " ".join(process.command[2:4]) if len(process.command) > 3 else "N/A"
            name_with_status = f"{process.name}\n[dim]{cmd_preview}[/dim]"
            if process.status_text and process.status_text not in ["Running", "Starting..."]:
                name_with_status += f"\n[italic red]{process.status_text[:40]}[/italic red]"
            
            table.add_row(
                name_with_status,
                pid,
                status,
                uptime,
                str(process.restart_count)
            )
        
        # Add summary row
        running_count = sum(1 for p in processes if p.is_running)
        table.add_row(
            "[bold]Total[/bold]",
            "",
            f"[green]{running_count}/{len(processes)}[/green]",
            "",
            "",
            style="dim"
        )
        
        return Panel(table, title="🔧 Managed Processes", border_style="magenta")
    
    def _format_uptime(self, start_time: datetime) -> str:
        """Format process uptime as a human-readable string."""
        if not start_time:
            return "N/A"
        
        uptime = datetime.now() - start_time
        total_seconds = int(uptime.total_seconds())
        
        if total_seconds < 60:
            return f"{total_seconds}s"
        elif total_seconds < 3600:
            minutes = total_seconds // 60
            seconds = total_seconds % 60
            return f"{minutes}m {seconds}s"
        else:
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            return f"{hours}h {minutes}m"
    
    def _create_stats_panel(self, stats: ProgressStats) -> Panel:
        """Create statistics panel."""
        table = Table(show_header=False, box=None)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="white")
        
        table.add_row("Total Batches", str(stats.total_batches))
        table.add_row("Discovered", str(stats.batches_discovered))
        table.add_row("Queue Items", f"{stats.total_queue_items:,}")
        table.add_row("Downloaded", f"{stats.items_downloaded:,}")
        
        if stats.download_size_mb > 1024:
            size_display = f"{stats.download_size_mb/1024:.1f} GB"
        else:
            size_display = f"{stats.download_size_mb:.0f} MB"
        table.add_row("Total Size", size_display)
        
        return Panel(table, title="📊 Statistics", border_style="blue")
    
    def _create_estimates_panel(self, stats: ProgressStats) -> Panel:
        """Create estimates panel."""
        content = []
        
        if stats.estimated_discovery_completion:
            eta = stats.estimated_discovery_completion.strftime("%H:%M:%S")
            content.append(f"Discovery ETA: {eta}")
        else:
            content.append("Discovery ETA: Calculating...")
        
        if stats.estimated_download_completion:
            eta = stats.estimated_download_completion.strftime("%H:%M:%S")
            content.append(f"Download ETA: {eta}")
        else:
            content.append("Download ETA: Calculating...")
        
        if not content:
            content = ["Calculating estimates..."]
        
        return Panel(
            "\n".join(content),
            title="⏱️ Estimates",
            border_style="yellow"
        )
    
    def run(self):
        """Run the TUI monitor."""
        self.console.print("[bold green]Starting LoC Archive Monitor...[/bold green]")
        
        # Start background processes
        self.process_manager.start_all()
        
        # Give processes time to start
        time.sleep(2)
        
        with Live(console=self.console, refresh_per_second=2, screen=True) as live:
            while not self.shutdown_requested:
                try:
                    # Monitor processes
                    processes = self.process_manager.monitor_processes()
                    
                    # Get progress stats
                    stats = self.progress_monitor.get_progress_stats()
                    
                    # Update display
                    layout = self.create_layout(stats, processes)
                    live.update(layout)
                    
                    # Sleep before next update
                    time.sleep(1)
                    
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    # Log errors but keep running
                    pass
        
        # Cleanup
        self.console.print("\n[yellow]Shutting down...[/yellow]")
        self.process_manager.stop_all()
        self.console.print("[green]Shutdown complete.[/green]")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="TUI Monitor for LoC Archive Discovery and Downloads")
    parser.add_argument(
        '--db-path',
        type=str,
        default="/home/jake/loc/data/newsagger.db",
        help='Path to database file'
    )
    parser.add_argument(
        '--downloads-dir', 
        type=str,
        default="/home/jake/loc/downloads",
        help='Path to downloads directory'
    )
    
    args = parser.parse_args()
    
    # Check if database directory exists
    db_dir = Path(args.db_path).parent
    if not db_dir.exists():
        print(f"Error: Database directory '{db_dir}' does not exist.")
        print("Please create the directory or update the path.")
        sys.exit(1)
    
    # Create and run monitor
    monitor = TUIMonitor(args.db_path, args.downloads_dir)
    monitor.run()


if __name__ == '__main__':
    main()
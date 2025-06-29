#!/usr/bin/env python3
"""
TUI Demo - Shows the TUI with mock data without starting real processes.
"""

import time
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from tui_monitor import TUIMonitor, ProgressStats, ProcessStatus
from rich.console import Console
from rich.live import Live

def create_demo_data():
    """Create demo progress data."""
    stats = ProgressStats()
    
    # Simulate some realistic progress
    stats.total_batches = 25
    stats.batches_discovered = 8
    stats.current_batch = "vi_alpina_ver01"
    stats.current_batch_progress = 65.0
    stats.discovery_rate_per_hour = 1250.0
    
    stats.total_queue_items = 15420
    stats.items_downloaded = 8934
    stats.download_rate_per_hour = 847.0
    stats.download_size_mb = 31500.0  # 31.5 GB
    
    # Simulate rate limiting occasionally
    import random
    if random.random() > 0.8:  # 20% chance
        stats.is_rate_limited = True
        stats.rate_limit_reason = "CAPTCHA Cooldown"
        stats.cooldown_remaining_minutes = random.uniform(5, 45)
    
    # Estimates
    stats.estimated_discovery_completion = datetime.now() + timedelta(hours=12, minutes=30)
    stats.estimated_download_completion = datetime.now() + timedelta(hours=8, minutes=15)
    
    return stats

def create_demo_processes():
    """Create demo process status."""
    processes = [
        ProcessStatus(
            name="Batch Discovery",
            command=["python", "main.py", "discover-via-batches"],
            is_running=True,
            status_text="Running",
            last_update=datetime.now(),
            restart_count=1
        ),
        ProcessStatus(
            name="Downloads", 
            command=["python", "main.py", "process-downloads"],
            is_running=True,
            status_text="Running",
            last_update=datetime.now(),
            restart_count=0
        )
    ]
    return processes

def run_tui_demo():
    """Run TUI demo with simulated data."""
    console = Console()
    
    # Create TUI monitor (but don't start real processes)
    monitor = TUIMonitor("/home/jake/loc/data/newsagger.db", "/home/jake/loc/downloads")
    
    console.print("[bold green]🚀 Starting TUI Demo - Press Ctrl+C to exit[/bold green]")
    console.print("[yellow]This demo shows the TUI with simulated data (no real processes)[/yellow]")
    console.print()
    
    # Set up signal handling for graceful exit
    import signal
    shutdown_requested = False
    
    def signal_handler(signum, frame):
        nonlocal shutdown_requested
        shutdown_requested = True
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        with Live(console=console, refresh_per_second=2, screen=True) as live:
            iteration = 0
            while not shutdown_requested:
                # Create dynamic demo data
                stats = create_demo_data()
                processes = create_demo_processes()
                
                # Add some animation
                stats.current_batch_progress = (stats.current_batch_progress + iteration * 0.5) % 100
                stats.batches_discovered = min(25, 8 + iteration // 20)
                stats.items_downloaded = min(stats.total_queue_items, 8934 + iteration * 3)
                
                # Update download size
                stats.download_size_mb = 31500 + iteration * 15
                
                # Create and update layout
                layout = monitor.create_layout(stats, processes)
                live.update(layout)
                
                time.sleep(0.5)
                iteration += 1
                
    except KeyboardInterrupt:
        pass  # Already handled by signal handler
    except Exception as e:
        console.print(f"\n[red]❌ Error: {e}[/red]")
        import traceback
        traceback.print_exc()
    finally:
        console.print("\n[green]✅ Demo completed![/green]")

if __name__ == '__main__':
    run_tui_demo()
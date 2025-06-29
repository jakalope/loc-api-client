#!/usr/bin/env python3
"""
Simple TUI test to debug display issues.
"""

import time
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich.table import Table

def test_simple_display():
    """Test simple Rich display."""
    console = Console()
    
    print("🧪 Testing simple Rich display...")
    
    # Test 1: Simple panel
    console.print(Panel("Hello from Rich!", title="Test Panel"))
    
    # Test 2: Simple table
    table = Table(title="Test Table")
    table.add_column("Column 1")
    table.add_column("Column 2")
    table.add_row("Row 1", "Data 1")
    table.add_row("Row 2", "Data 2")
    console.print(table)
    
    # Test 3: Live display for 5 seconds
    print("\n🔄 Testing Live display (5 seconds)...")
    
    with Live(console=console, refresh_per_second=2) as live:
        for i in range(10):
            layout = Layout()
            layout.split_column(
                Layout(Panel(f"Live Update {i+1}/10", title="Dynamic Content"), size=3),
                Layout(Panel("Static content area", title="Main Area"))
            )
            live.update(layout)
            time.sleep(0.5)
    
    print("✅ Simple TUI test completed")

def test_tui_monitor_components():
    """Test TUI monitor specific components."""
    console = Console()
    
    print("\n🧪 Testing TUI Monitor components...")
    
    # Test imports
    try:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent))
        
        from tui_monitor import TUIMonitor, ProgressStats, ProcessStatus
        print("✅ TUI Monitor imports successful")
        
        # Test component creation
        stats = ProgressStats()
        stats.total_batches = 25
        stats.batches_discovered = 5
        stats.current_batch = "test_batch"
        
        processes = [
            ProcessStatus("Discovery", ["test"], is_running=True, status_text="Running"),
            ProcessStatus("Downloads", ["test"], is_running=False, status_text="Stopped")
        ]
        
        # Test monitor creation
        monitor = TUIMonitor("/home/jake/loc/data/newsagger.db", "/home/jake/loc/downloads")
        print("✅ TUI Monitor created successfully")
        
        # Test layout creation
        layout = monitor.create_layout(stats, processes)
        print("✅ Layout created successfully")
        
        # Display the layout briefly
        console.print(Panel("Testing TUI Monitor Layout...", title="TUI Test"))
        with Live(layout, console=console, refresh_per_second=1) as live:
            time.sleep(3)
        
        print("✅ TUI Monitor components working")
        
    except Exception as e:
        print(f"❌ TUI Monitor error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    test_simple_display()
    test_tui_monitor_components()
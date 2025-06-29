#!/usr/bin/env python3
"""
Test the real TUI monitor with actual data for a short period.
"""

import signal
import time
from tui_monitor import TUIMonitor

def test_real_tui():
    """Test the real TUI monitor briefly."""
    print("🚀 Starting TUI Monitor test (will run for 10 seconds)...")
    
    # Create monitor with real paths
    monitor = TUIMonitor(
        db_path="/home/jake/loc/data/newsagger.db",
        downloads_dir="/home/jake/loc/downloads"
    )
    
    # Set up a timeout to stop after 10 seconds
    def timeout_handler(signum, frame):
        monitor.shutdown_requested = True
        print("\n⏰ Test timeout reached, shutting down...")
    
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(10)  # 10 second timeout
    
    try:
        # This will start the processes and show the TUI
        monitor.run()
    except KeyboardInterrupt:
        print("\n⌨️ Manual interrupt")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        signal.alarm(0)  # Cancel the alarm
        print("✅ TUI Monitor test completed")

if __name__ == '__main__':
    test_real_tui()
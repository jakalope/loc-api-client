#!/usr/bin/env python3
"""
Demo script to show batch discovery progress monitoring.

This creates a mock batch discovery session and updates it to demonstrate
the monitoring capabilities.
"""

import sys
import time
import sqlite3
import random
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from newsagger.storage import NewsStorage


def simulate_batch_discovery(db_path: str):
    """Simulate a batch discovery process for monitoring demo."""
    storage = NewsStorage(db_path)
    
    # Create a demo session
    session_name = f"demo_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Create batch discovery session
    storage.create_batch_discovery_session(
        session_name=session_name,
        total_batches=10,
        auto_enqueue=True
    )
    
    print(f"Created batch discovery session: {session_name}")
    print("Run the monitor in another terminal to watch progress:")
    print(f"  python monitor_batch_discovery.py --db-path {db_path}")
    print()
    
    # Simulate processing batches
    total_pages = 0
    total_enqueued = 0
    
    for batch_idx in range(10):
        batch_name = f"demo_batch_{batch_idx+1:03d}"
        issues_count = random.randint(20, 50)
        
        # Update batch info
        storage.update_batch_discovery_session(
            session_name=session_name,
            current_batch_index=batch_idx,
            current_batch_name=batch_name,
            total_issues_in_batch=issues_count,
            status='active'
        )
        
        print(f"Processing batch {batch_idx+1}/10: {batch_name} ({issues_count} issues)")
        
        # Simulate processing issues
        for issue_idx in range(issues_count):
            # Random pages per issue
            pages_count = random.randint(2, 8)
            total_pages += pages_count
            
            # Simulate enqueuing (80% chance)
            if random.random() < 0.8:
                total_enqueued += pages_count
            
            # Update progress
            storage.update_batch_discovery_session(
                session_name=session_name,
                current_issue_index=issue_idx + 1,
                pages_discovered_delta=pages_count,
                pages_enqueued_delta=pages_count if random.random() < 0.8 else 0
            )
            
            # Simulate processing time
            time.sleep(0.1)
            
            # Simulate CAPTCHA on random occasions (5% chance)
            if random.random() < 0.05:
                print(f"  CAPTCHA detected at issue {issue_idx+1}!")
                storage.update_batch_discovery_session(
                    session_name=session_name,
                    status='captcha_blocked'
                )
                
                # Simulate cooling off period
                print("  Cooling off for 5 seconds...")
                time.sleep(5)
                
                # Resume
                storage.update_batch_discovery_session(
                    session_name=session_name,
                    status='active'
                )
                print("  Resumed after cooling off")
    
    # Mark as completed
    storage.update_batch_discovery_session(
        session_name=session_name,
        status='completed',
        current_batch_index=10
    )
    
    print(f"\nCompleted! Discovered {total_pages} pages, enqueued {total_enqueued}")


def main():
    """Run the demo."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Demo batch discovery monitoring")
    parser.add_argument(
        '--db-path',
        type=str,
        default='demo_monitor.db',
        help='Path to database file (default: demo_monitor.db)'
    )
    
    args = parser.parse_args()
    
    print("Starting batch discovery simulation...")
    print("This will create a mock batch discovery session to demonstrate monitoring.")
    print()
    
    simulate_batch_discovery(args.db_path)


if __name__ == '__main__':
    main()
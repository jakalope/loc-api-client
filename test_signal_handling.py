#!/usr/bin/env python3
"""
Test signal handling in continuous downloads.
"""

import sys
import time
import signal
import subprocess
from pathlib import Path

def test_signal_handling():
    """Test that process-downloads responds to Ctrl+C quickly."""
    print("🧪 Testing signal handling in process-downloads...")
    
    # Activate virtual environment
    venv_python = Path("venv/bin/python")
    if not venv_python.exists():
        venv_python = sys.executable
    
    cmd = [
        str(venv_python), "main.py", "process-downloads", 
        "--continuous", "--max-idle-minutes", "1", "--dry-run"
    ]
    
    print(f"Starting: {' '.join(cmd)}")
    
    # Start the process
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # Wait a moment for it to start
    time.sleep(3)
    
    # Send SIGINT (Ctrl+C)
    print("Sending SIGINT (Ctrl+C)...")
    start_time = time.time()
    process.send_signal(signal.SIGINT)
    
    # Wait for process to exit
    try:
        stdout, stderr = process.communicate(timeout=10)
        end_time = time.time()
        response_time = end_time - start_time
        
        print(f"✅ Process responded to SIGINT in {response_time:.2f} seconds")
        
        if "shutdown signal received" in stderr.lower() or "shutdown signal received" in stdout.lower():
            print("✅ Graceful shutdown message detected")
        
        if response_time < 5:
            print("✅ Fast response time (< 5 seconds)")
        else:
            print("⚠️ Slow response time (>= 5 seconds)")
        
        print(f"Exit code: {process.returncode}")
        if stderr:
            print("STDERR:")
            print(stderr[-500:])  # Last 500 chars
        
    except subprocess.TimeoutExpired:
        print("❌ Process did not respond to SIGINT within 10 seconds")
        process.kill()
        process.communicate()

if __name__ == '__main__':
    test_signal_handling()
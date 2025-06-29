#!/usr/bin/env python3
"""
Test what operations are still blocking signals in process-downloads.
"""

import sys
import time
import signal
import subprocess
import threading
from pathlib import Path

def test_real_blocking():
    """Test what's actually blocking in real continuous downloads."""
    print("🧪 Testing real blocking operations in process-downloads...")
    
    # Start continuous downloads with real queue
    venv_python = Path("venv/bin/python")
    if not venv_python.exists():
        venv_python = sys.executable
    
    cmd = [
        str(venv_python), "main.py", "process-downloads", 
        "--continuous", "--max-idle-minutes", "5", "--max-items", "5"
    ]
    
    print(f"Starting: {' '.join(cmd)}")
    print("Will test Ctrl+C response time after 5 seconds...")
    
    # Start the process
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    def monitor_output():
        """Monitor process output in background."""
        try:
            for line in iter(process.stderr.readline, ''):
                if line.strip():
                    print(f"STDERR: {line.strip()}")
                if process.poll() is not None:
                    break
        except:
            pass
    
    # Start output monitoring
    monitor_thread = threading.Thread(target=monitor_output, daemon=True)
    monitor_thread.start()
    
    # Wait for process to start and potentially begin downloads
    time.sleep(5)
    
    # Test SIGINT response
    print("\\n📡 Sending SIGINT (Ctrl+C)...")
    start_time = time.time()
    process.send_signal(signal.SIGINT)
    
    # Wait for process to exit
    try:
        stdout, remaining_stderr = process.communicate(timeout=15)
        end_time = time.time()
        response_time = end_time - start_time
        
        print(f"\\n✅ Process responded to SIGINT in {response_time:.2f} seconds")
        
        if response_time < 2:
            print("✅ Fast response (< 2 seconds)")
        elif response_time < 5:
            print("⚠️ Moderate response (2-5 seconds)")
        else:
            print("❌ Slow response (> 5 seconds)")
        
        print(f"Exit code: {process.returncode}")
        
        # Show any final output
        if remaining_stderr:
            print("\\nFinal STDERR:")
            print(remaining_stderr[-300:])
        
    except subprocess.TimeoutExpired:
        print("\\n❌ Process did not respond to SIGINT within 15 seconds")
        print("This suggests there are still blocking operations")
        
        # Try SIGQUIT as backup
        print("Trying SIGQUIT (Ctrl+\\\\)...")
        process.send_signal(signal.SIGQUIT)
        try:
            process.communicate(timeout=3)
            print("✅ SIGQUIT worked")
        except subprocess.TimeoutExpired:
            print("❌ Even SIGQUIT didn't work, force killing...")
            process.kill()
            process.communicate()

if __name__ == '__main__':
    test_real_blocking()
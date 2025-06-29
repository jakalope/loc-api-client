#!/usr/bin/env python3
"""
Test SIGQUIT (Ctrl+\) force quit behavior.
"""

import sys
import time
import signal
import subprocess
from pathlib import Path

def test_force_quit():
    """Test that Ctrl+\\ (SIGQUIT) immediately stops the process."""
    print("🧪 Testing SIGQUIT (Ctrl+\\\\) force quit behavior...")
    
    venv_python = Path("venv/bin/python")
    if not venv_python.exists():
        venv_python = sys.executable
    
    cmd = [
        str(venv_python), "main.py", "process-downloads", 
        "--continuous", "--max-idle-minutes", "10", "--max-items", "2"
    ]
    
    print(f"Starting: {' '.join(cmd)}")
    print("Will test SIGQUIT after 3 seconds...")
    
    # Start the process
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # Wait for it to start
    time.sleep(3)
    
    # Test SIGQUIT response
    print("\\n⚡ Sending SIGQUIT (Ctrl+\\\\) for immediate termination...")
    start_time = time.time()
    process.send_signal(signal.SIGQUIT)
    
    # Wait for process to exit
    try:
        stdout, stderr = process.communicate(timeout=5)
        end_time = time.time()
        response_time = end_time - start_time
        
        print(f"\\n✅ Process responded to SIGQUIT in {response_time:.2f} seconds")
        
        if response_time < 1:
            print("✅ Immediate response (< 1 second)")
        elif response_time < 3:
            print("⚠️ Fast response (1-3 seconds)")
        else:
            print("❌ Slow response (> 3 seconds)")
        
        print(f"Exit code: {process.returncode}")
        
        # Check for force quit message
        if "force quit" in stderr.lower():
            print("✅ Force quit message detected")
        else:
            print("⚠️ No force quit message found")
        
        # Show final output
        if stderr:
            print("\\nFinal STDERR:")
            print(stderr[-400:])
        
    except subprocess.TimeoutExpired:
        print("\\n❌ Process did not respond to SIGQUIT within 5 seconds")
        process.kill()
        process.communicate()

def test_graceful_vs_force():
    """Test the difference between SIGINT and SIGQUIT."""
    print("\\n🧪 Testing graceful (SIGINT) vs force (SIGQUIT) behavior...")
    
    venv_python = Path("venv/bin/python")
    if not venv_python.exists():
        venv_python = sys.executable
    
    for signal_type, signal_num, description in [
        ("SIGINT (Ctrl+C)", signal.SIGINT, "graceful shutdown"),
        ("SIGQUIT (Ctrl+\\\\)", signal.SIGQUIT, "force quit")
    ]:
        print(f"\\n--- Testing {signal_type} ({description}) ---")
        
        cmd = [
            str(venv_python), "main.py", "process-downloads", 
            "--continuous", "--max-idle-minutes", "10", "--dry-run"
        ]
        
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        time.sleep(2)  # Let it start
        
        start_time = time.time()
        process.send_signal(signal_num)
        
        try:
            stdout, stderr = process.communicate(timeout=8)
            response_time = time.time() - start_time
            
            print(f"  Response time: {response_time:.2f} seconds")
            print(f"  Exit code: {process.returncode}")
            
            # Look for specific messages
            if "shutdown signal received" in stderr:
                print("  ✅ Graceful shutdown detected")
            elif "force quit" in stderr.lower():
                print("  ⚡ Force quit detected")
            else:
                print("  ? No specific shutdown message")
                
        except subprocess.TimeoutExpired:
            print(f"  ❌ No response within 8 seconds")
            process.kill()
            process.communicate()

if __name__ == '__main__':
    test_force_quit()
    test_graceful_vs_force()
"""
Durability Benchmark.
Tests data durability by killing the server randomly and checking for data loss.
"""

import subprocess
import threading
import time
import sys
import os
import shutil
import signal
import random

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client import KVClient


class DurabilityServerManager:
    """Manage server for durability testing."""
    
    def __init__(self, port: int = 6410, data_dir: str = "bench_data_durability"):
        self.port = port
        self.data_dir = data_dir
        self.process = None
        self._lock = threading.Lock()
    
    def start(self, clean: bool = False):
        """Start server."""
        with self._lock:
            if clean and os.path.exists(self.data_dir):
                shutil.rmtree(self.data_dir)
            os.makedirs(self.data_dir, exist_ok=True)
            
            self.process = subprocess.Popen(
                [sys.executable, "server.py", "--port", str(self.port), "--data-dir", self.data_dir],
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            time.sleep(1)
    
    def stop(self):
        """Graceful stop."""
        with self._lock:
            if self.process:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                self.process = None
    
    def kill_hard(self):
        """SIGKILL the server (-9)."""
        with self._lock:
            if self.process:
                if sys.platform == 'win32':
                    subprocess.run(['taskkill', '/F', '/PID', str(self.process.pid)], 
                                 capture_output=True)
                else:
                    os.kill(self.process.pid, signal.SIGKILL)
                self.process = None
    
    def is_running(self):
        """Check if server is running."""
        with self._lock:
            return self.process is not None and self.process.poll() is None
    
    def cleanup(self):
        """Clean data."""
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)


def run_durability_benchmark():
    """
    Run durability benchmark.
    
    - Thread 1: Continuous writes with acknowledgment tracking
    - Thread 2: Random kills of server
    - Check which acknowledged keys are lost after restart
    """
    port = 6410
    data_dir = "bench_data_durability"
    
    print("=" * 70)
    print("DURABILITY BENCHMARK")
    print("=" * 70)
    
    # Shared state
    acknowledged_keys = {}  # key -> value for acknowledged writes
    lock = threading.Lock()
    stop_flag = threading.Event()
    kill_count = 0
    write_count = 0
    error_count = 0
    
    manager = DurabilityServerManager(port=port, data_dir=data_dir)
    manager.start(clean=True)
    
    def writer_thread():
        """Continuously write data and track acknowledgments."""
        nonlocal write_count, error_count
        counter = 0
        
        while not stop_flag.is_set():
            try:
                client = KVClient("127.0.0.1", port, timeout=2)
                
                for _ in range(20):
                    if stop_flag.is_set():
                        break
                    
                    key = f"durability_key_{counter}"
                    value = f"durability_value_{counter}"
                    
                    try:
                        if client.set(key, value):
                            with lock:
                                acknowledged_keys[key] = value
                                write_count += 1
                        counter += 1
                    except Exception:
                        error_count += 1
                    
                    time.sleep(0.005)
                
                client.close()
            except Exception:
                time.sleep(0.1)
    
    def killer_thread():
        """Randomly kill the server."""
        nonlocal kill_count
        
        while not stop_flag.is_set():
            # Random wait between kills
            time.sleep(random.uniform(0.5, 2.0))
            
            if stop_flag.is_set():
                break
            
            print(f"  [KILL] Killing server (kill #{kill_count + 1})...")
            manager.kill_hard()
            kill_count += 1
            
            time.sleep(0.5)
            
            if not stop_flag.is_set():
                print(f"  [START] Restarting server...")
                manager.start(clean=False)
    
    # Start threads
    print("\nStarting durability test...")
    print(f"  - Writer thread: continuous writes with ack tracking")
    print(f"  - Killer thread: random SIGKILL every 0.5-2s")
    print(f"  - Test duration: 10 seconds")
    print("-" * 70)
    
    writer = threading.Thread(target=writer_thread)
    killer = threading.Thread(target=killer_thread)
    
    writer.start()
    killer.start()
    
    # Run for 10 seconds
    test_duration = 10
    start_time = time.time()
    while time.time() - start_time < test_duration:
        time.sleep(0.5)
        print(f"  Progress: {time.time() - start_time:.1f}s | Writes: {write_count} | Kills: {kill_count}")
    
    # Stop threads
    stop_flag.set()
    writer.join(timeout=5)
    killer.join(timeout=5)
    
    print("-" * 70)
    print("\nStopping and verifying...")
    
    # Ensure server is running for verification
    try:
        manager.stop()
    except:
        pass
    time.sleep(0.5)
    manager.start(clean=False)
    time.sleep(1)
    
    # Verify all acknowledged writes
    print("\nVerifying acknowledged writes...")
    
    client = KVClient("127.0.0.1", port, timeout=30)
    
    lost_keys = []
    corrupted_keys = []
    
    total_keys = len(acknowledged_keys)
    verified = 0
    
    for key, expected_value in acknowledged_keys.items():
        actual_value = client.get(key)
        
        if actual_value is None:
            lost_keys.append(key)
        elif actual_value != expected_value:
            corrupted_keys.append((key, expected_value, actual_value))
        
        verified += 1
        if verified % 1000 == 0:
            print(f"  Verified {verified}/{total_keys} keys...")
    
    client.close()
    manager.stop()
    manager.cleanup()
    
    # Report results
    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"\nTest Statistics:")
    print(f"  Total acknowledged writes: {total_keys:,}")
    print(f"  Total kills performed: {kill_count}")
    print(f"  Write errors (connection): {error_count}")
    
    print(f"\nDurability Results:")
    print(f"  Lost keys:      {len(lost_keys):,}")
    print(f"  Corrupted keys: {len(corrupted_keys):,}")
    
    if total_keys > 0:
        loss_rate = len(lost_keys) / total_keys * 100
        corruption_rate = len(corrupted_keys) / total_keys * 100
        durability = 100 - loss_rate - corruption_rate
        
        print(f"\n  Loss rate:       {loss_rate:.4f}%")
        print(f"  Corruption rate: {corruption_rate:.4f}%")
        print(f"  DURABILITY:      {durability:.4f}%")
        
        if durability == 100.0:
            print("\n  ✓ 100% DURABILITY ACHIEVED!")
        else:
            print(f"\n  ✗ Durability below target (lost {len(lost_keys)} keys)")
            if lost_keys:
                print(f"\n  First 5 lost keys: {lost_keys[:5]}")
    
    print("\n" + "=" * 70)
    
    return {
        'total_acknowledged': total_keys,
        'kills': kill_count,
        'lost': len(lost_keys),
        'corrupted': len(corrupted_keys),
        'durability_pct': 100 - (len(lost_keys) / total_keys * 100) if total_keys > 0 else 100
    }


if __name__ == "__main__":
    run_durability_benchmark()

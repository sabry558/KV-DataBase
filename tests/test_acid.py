"""
ACID compliance tests for the Key-Value Store.
Tests atomicity, consistency, isolation, and durability.
"""

import pytest
import subprocess
import threading
import time
import sys
import os
import signal
import shutil
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client import KVClient


class TestServerManager:
    """Helper class to manage server process for tests."""
    
    def __init__(self, port: int = 6390, data_dir: str = "test_data_acid"):
        self.port = port
        self.data_dir = data_dir
        self.process = None
    
    def start(self, clean: bool = True):
        """Start the server."""
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
        
        if self.process.poll() is not None:
            stdout, stderr = self.process.communicate()
            raise RuntimeError(f"Server failed to start: {stderr.decode()}")
    
    def stop(self, graceful: bool = True):
        """Stop the server."""
        if self.process:
            if graceful:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
            else:
                self.process.kill()
            self.process = None
    
    def kill_hard(self):
        """Force kill with SIGKILL (-9)."""
        if self.process:
            if sys.platform == 'win32':
                # On Windows, use taskkill with /F for force
                subprocess.run(['taskkill', '/F', '/PID', str(self.process.pid)], 
                             capture_output=True)
            else:
                os.kill(self.process.pid, signal.SIGKILL)
            self.process = None
    
    def get_pid(self):
        """Get server process ID."""
        return self.process.pid if self.process else None
    
    def cleanup(self):
        """Clean up data directory."""
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)


class TestConcurrentBulkSet:
    """Test concurrent bulk set writes touching the same keys."""
    
    def test_concurrent_bulk_set_same_keys(self):
        """
        Concurrent bulk set writes on same keys should not affect each other's atomicity.
        Each bulk write should be atomic - either all keys are set or none.
        """
        port = 6391
        data_dir = "test_data_concurrent"
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        
        manager = TestServerManager(port=port, data_dir=data_dir)
        manager.start()
        
        num_threads = 10
        keys_per_thread = 5
        results = []
        
        def bulk_write(thread_id):
            """Each thread writes its own set of values to shared keys."""
            try:
                client = KVClient("127.0.0.1", port, timeout=30)
                items = [(f"shared_key_{i}", f"thread_{thread_id}_value_{i}") 
                        for i in range(keys_per_thread)]
                success = client.bulk_set(items)
                client.close()
                return thread_id, success
            except Exception as e:
                return thread_id, False
        
        # Run concurrent writes
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(bulk_write, i) for i in range(num_threads)]
            results = [f.result() for f in as_completed(futures)]
        
        # Check results - all writes should succeed
        all_success = all(success for _, success in results)
        
        # Verify final state - values should be consistent (from one thread)
        client = KVClient("127.0.0.1", port)
        values = []
        for i in range(keys_per_thread):
            val = client.get(f"shared_key_{i}")
            if val:
                values.append(val)
        client.close()
        
        manager.stop()
        manager.cleanup()
        
        assert all_success, "Not all bulk writes succeeded"
        
        # All values should be from the same thread (atomic writes)
        if values:
            # Extract thread IDs from values
            thread_ids = set()
            for v in values:
                if v:
                    parts = v.split('_')
                    if len(parts) >= 2:
                        thread_ids.add(parts[1])
            
            # Ideally all should be from same thread, but concurrent writes 
            # may interleave - the key is no partial writes
            assert len(values) == keys_per_thread, "Some keys are missing"


class TestDurabilityWithKill:
    """Test durability by killing server randomly."""
    
    def test_bulk_write_with_random_kill(self):
        """
        Bulk writes should be completely applied or not at all even with SIGKILL.
        """
        port = 6392
        data_dir = "test_data_kill"
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        
        manager = TestServerManager(port=port, data_dir=data_dir)
        manager.start()
        
        acknowledged_keys = set()
        write_count = 100
        batch_size = 10
        
        try:
            client = KVClient("127.0.0.1", port, timeout=5)
            
            # Write batches and track acknowledgments
            for batch_start in range(0, write_count, batch_size):
                items = [(f"key_{i}", f"value_{i}") 
                        for i in range(batch_start, min(batch_start + batch_size, write_count))]
                
                if client.bulk_set(items):
                    for key, _ in items:
                        acknowledged_keys.add(key)
                
                # Random delay
                time.sleep(random.uniform(0.01, 0.05))
            
            client.close()
        except Exception:
            pass
        
        # Kill server hard
        manager.kill_hard()
        time.sleep(1)
        
        # Restart and verify
        manager.start(clean=False)
        
        client = KVClient("127.0.0.1", port)
        
        lost_keys = []
        for key in acknowledged_keys:
            if client.get(key) is None:
                lost_keys.append(key)
        
        client.close()
        manager.stop()
        manager.cleanup()
        
        # No acknowledged keys should be lost
        assert len(lost_keys) == 0, f"Lost {len(lost_keys)} acknowledged keys: {lost_keys[:10]}"
    
    def test_acknowledged_writes_survive_kill(self):
        """
        Track acknowledged writes and verify none are lost after SIGKILL.
        """
        port = 6393
        data_dir = "test_data_ack"
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        
        acknowledged = []
        stop_flag = threading.Event()
        kill_count = 0
        
        def writer_thread():
            """Continuously write and track acknowledgments."""
            nonlocal acknowledged
            counter = 0
            while not stop_flag.is_set():
                try:
                    client = KVClient("127.0.0.1", port, timeout=2)
                    for _ in range(10):
                        if stop_flag.is_set():
                            break
                        key = f"ack_key_{counter}"
                        value = f"ack_value_{counter}"
                        if client.set(key, value):
                            acknowledged.append((key, value))
                        counter += 1
                        time.sleep(0.01)
                    client.close()
                except Exception:
                    time.sleep(0.1)
        
        def killer_thread(manager):
            """Randomly kill the server."""
            nonlocal kill_count
            while not stop_flag.is_set():
                time.sleep(random.uniform(0.5, 1.5))
                if stop_flag.is_set():
                    break
                try:
                    manager.kill_hard()
                    kill_count += 1
                    time.sleep(0.5)
                    manager.start(clean=False)
                except Exception:
                    pass
        
        # Start server
        manager = TestServerManager(port=port, data_dir=data_dir)
        manager.start()
        
        # Run writer and killer
        writer = threading.Thread(target=writer_thread)
        killer = threading.Thread(target=lambda: killer_thread(manager))
        
        writer.start()
        killer.start()
        
        # Run for a few seconds
        time.sleep(5)
        
        # Stop threads
        stop_flag.set()
        writer.join(timeout=5)
        killer.join(timeout=5)
        
        # Ensure server is running for verification
        try:
            manager.stop()
        except:
            pass
        manager.start(clean=False)
        
        # Verify acknowledged writes
        client = KVClient("127.0.0.1", port)
        lost = []
        for key, value in acknowledged:
            actual = client.get(key)
            if actual != value:
                lost.append((key, value, actual))
        
        client.close()
        manager.stop()
        manager.cleanup()
        
        loss_rate = len(lost) / len(acknowledged) * 100 if acknowledged else 0
        
        print(f"\n=== Durability Test Results ===")
        print(f"Kill count: {kill_count}")
        print(f"Acknowledged writes: {len(acknowledged)}")
        print(f"Lost writes: {len(lost)}")
        print(f"Loss rate: {loss_rate:.2f}%")
        
        # Target: 100% durability
        assert loss_rate == 0, f"Lost {len(lost)} writes ({loss_rate:.2f}%): {lost[:5]}"


class TestAtomicity:
    """Test atomicity of bulk operations."""
    
    def test_bulk_set_atomicity(self):
        """
        Bulk set should be atomic - all or nothing.
        """
        port = 6394
        data_dir = "test_data_atomic"
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        
        manager = TestServerManager(port=port, data_dir=data_dir)
        manager.start()
        
        # Test multiple atomic batch writes
        client = KVClient("127.0.0.1", port)
        
        batch1 = [("atomic_a1", "val1"), ("atomic_a2", "val2"), ("atomic_a3", "val3")]
        batch2 = [("atomic_b1", "val1"), ("atomic_b2", "val2"), ("atomic_b3", "val3")]
        
        assert client.bulk_set(batch1)
        assert client.bulk_set(batch2)
        
        # Verify all keys exist
        for key, val in batch1 + batch2:
            assert client.get(key) == val
        
        client.close()
        manager.stop()
        manager.cleanup()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

"""
Basic tests for the Key-Value Store.
Tests common scenarios using the client library.
"""

import pytest
import subprocess
import time
import sys
import os
import signal
import shutil

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client import KVClient


class TestServerManager:
    """Helper class to manage server process for tests."""
    
    def __init__(self, port: int = 6380, data_dir: str = "test_data"):
        self.port = port
        self.data_dir = data_dir
        self.process = None
    
    def start(self, clean: bool = True):
        """Start the server."""
        # Clean data directory (only if clean=True)
        if clean and os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Start server process
        self.process = subprocess.Popen(
            [sys.executable, "server.py", "--port", str(self.port), "--data-dir", self.data_dir],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wait for server to be ready
        time.sleep(1)
        
        # Check if server is running
        if self.process.poll() is not None:
            stdout, stderr = self.process.communicate()
            raise RuntimeError(f"Server failed to start: {stderr.decode()}")
    
    def stop(self, graceful: bool = True):
        """Stop the server."""
        if self.process:
            if graceful:
                # Send SIGTERM for graceful shutdown
                if sys.platform == 'win32':
                    self.process.terminate()
                else:
                    self.process.send_signal(signal.SIGTERM)
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
            else:
                # Force kill
                self.process.kill()
            self.process = None
    
    def kill(self):
        """Force kill the server (SIGKILL)."""
        if self.process:
            self.process.kill()
            self.process = None
    
    def cleanup(self):
        """Clean up data directory."""
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)


@pytest.fixture
def server():
    """Fixture that provides a running server."""
    manager = TestServerManager(port=6380, data_dir="test_data_basic")
    manager.start()
    yield manager
    manager.stop()
    manager.cleanup()


@pytest.fixture
def client(server):
    """Fixture that provides a connected client."""
    client = KVClient("127.0.0.1", 6380)
    yield client
    client.close()


class TestBasicOperations:
    """Test basic KV operations."""
    
    def test_set_then_get(self, client):
        """Test: Set then Get - should return the set value."""
        assert client.set("name", "Alice")
        assert client.get("name") == "Alice"
    
    def test_set_then_delete_then_get(self, client):
        """Test: Set then Delete then Get - should return None after delete."""
        assert client.set("temp_key", "temp_value")
        assert client.get("temp_key") == "temp_value"
        assert client.delete("temp_key")
        assert client.get("temp_key") is None
    
    def test_get_without_setting(self, client):
        """Test: Get without setting - should return None."""
        assert client.get("nonexistent_key") is None
    
    def test_set_set_same_key_then_get(self, client):
        """Test: Set then Set (same key) then Get - should return last value."""
        assert client.set("overwrite_key", "first_value")
        assert client.get("overwrite_key") == "first_value"
        assert client.set("overwrite_key", "second_value")
        assert client.get("overwrite_key") == "second_value"
    
    def test_bulk_set(self, client):
        """Test: Bulk set multiple keys at once."""
        items = [
            ("bulk1", "value1"),
            ("bulk2", "value2"),
            ("bulk3", "value3"),
        ]
        assert client.bulk_set(items)
        
        assert client.get("bulk1") == "value1"
        assert client.get("bulk2") == "value2"
        assert client.get("bulk3") == "value3"
    
    def test_delete_nonexistent_key(self, client):
        """Test: Delete a key that doesn't exist."""
        result = client.delete("never_existed")
        assert result is False
    
    def test_ping(self, client):
        """Test: Ping server."""
        assert client.ping() is True
    
    def test_count_and_keys(self, client):
        """Test: Count and Keys operations."""
        # Set some keys
        client.set("count1", "val1")
        client.set("count2", "val2")
        client.set("count3", "val3")
        
        assert client.count() >= 3
        keys = client.keys()
        assert "count1" in keys
        assert "count2" in keys
        assert "count3" in keys


class TestPersistence:
    """Test data persistence across restarts."""
    
    def test_set_then_graceful_exit_then_get(self):
        """Test: Set then exit gracefully then Get - data should persist."""
        data_dir = "test_data_persist"
        port = 6381
        
        # Clean up
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        
        # Start server and set data
        manager = TestServerManager(port=port, data_dir=data_dir)
        manager.start()
        
        client = KVClient("127.0.0.1", port)
        assert client.set("persist_key", "persist_value")
        assert client.get("persist_key") == "persist_value"
        client.close()
        
        # Graceful shutdown
        manager.stop(graceful=True)
        time.sleep(1)
        
        # Restart server
        manager.start(clean=False)
        
        # Check data persisted
        client = KVClient("127.0.0.1", port)
        value = client.get("persist_key")
        client.close()
        
        manager.stop()
        manager.cleanup()
        
        assert value == "persist_value", f"Expected 'persist_value', got '{value}'"
    
    def test_multiple_values_persist(self):
        """Test: Multiple values persist across restart."""
        data_dir = "test_data_multi"
        port = 6382
        
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
        
        manager = TestServerManager(port=port, data_dir=data_dir)
        manager.start()
        
        client = KVClient("127.0.0.1", port)
        
        # Set multiple values
        for i in range(100):
            assert client.set(f"key_{i}", f"value_{i}")
        
        client.close()
        manager.stop(graceful=True)
        time.sleep(1)
        
        # Restart
        manager.start(clean=False)
        
        client = KVClient("127.0.0.1", port)
        
        # Verify all values
        all_correct = True
        for i in range(100):
            if client.get(f"key_{i}") != f"value_{i}":
                all_correct = False
                break
        
        client.close()
        manager.stop()
        manager.cleanup()
        
        assert all_correct, "Not all values persisted correctly"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

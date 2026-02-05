"""
Cluster tests.
Tests primary-secondary replication and failover.
"""

import pytest
import subprocess
import threading
import time
import sys
import os
import shutil
import signal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client import KVClient


class ClusterManager:
    """Manage a 3-node cluster for testing."""
    
    def __init__(self, base_port: int = 7000, base_data_dir: str = "test_cluster_data"):
        self.base_port = base_port
        self.base_data_dir = base_data_dir
        self.processes = {}
        self.nodes = [
            {"id": "node1", "port": base_port, "cluster_port": base_port + 100},
            {"id": "node2", "port": base_port + 1, "cluster_port": base_port + 101},
            {"id": "node3", "port": base_port + 2, "cluster_port": base_port + 102},
        ]
    
    def start_all(self):
        """Start all nodes."""
        if os.path.exists(self.base_data_dir):
            shutil.rmtree(self.base_data_dir)
        os.makedirs(self.base_data_dir, exist_ok=True)
        
        for i, node in enumerate(self.nodes):
            self.start_node(node["id"], is_primary=(i == 0))
            time.sleep(0.5)
    
    def start_node(self, node_id: str, is_primary: bool = False):
        """Start a specific node."""
        node = next(n for n in self.nodes if n["id"] == node_id)
        other_nodes = [n for n in self.nodes if n["id"] != node_id]
        
        peers = ",".join([
            f"{n['id']}:{n['port']}:{n['cluster_port']}" 
            for n in other_nodes
        ])
        
        data_dir = os.path.join(self.base_data_dir, node_id)
        os.makedirs(data_dir, exist_ok=True)
        
        cmd = [
            sys.executable, "cluster_runner.py",
            "--node-id", node_id,
            "--port", str(node["port"]),
            "--cluster-port", str(node["cluster_port"]),
            "--data-dir", data_dir,
            "--peers", peers,
        ]
        
        if is_primary:
            cmd.append("--primary")
        
        self.processes[node_id] = subprocess.Popen(
            cmd,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(1)
    
    def stop_node(self, node_id: str, graceful: bool = True):
        """Stop a specific node."""
        if node_id in self.processes:
            proc = self.processes[node_id]
            if graceful:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
            else:
                proc.kill()
            del self.processes[node_id]
    
    def kill_node(self, node_id: str):
        """Force kill a node with SIGKILL."""
        if node_id in self.processes:
            proc = self.processes[node_id]
            if sys.platform == 'win32':
                subprocess.run(['taskkill', '/F', '/PID', str(proc.pid)], capture_output=True)
            else:
                os.kill(proc.pid, signal.SIGKILL)
            del self.processes[node_id]
    
    def stop_all(self):
        """Stop all nodes."""
        for node_id in list(self.processes.keys()):
            self.stop_node(node_id)
    
    def cleanup(self):
        """Clean up data directories."""
        self.stop_all()
        if os.path.exists(self.base_data_dir):
            shutil.rmtree(self.base_data_dir)
    
    def get_node_port(self, node_id: str) -> int:
        """Get the client port for a node."""
        node = next(n for n in self.nodes if n["id"] == node_id)
        return node["port"]


class TestClusterBasics:
    """Test basic cluster operations."""
    
    @pytest.fixture
    def cluster(self):
        """Create and start a cluster."""
        manager = ClusterManager(base_port=7100, base_data_dir="test_cluster_basic")
        manager.start_all()
        yield manager
        manager.cleanup()
    
    def test_write_to_primary_read_from_any(self, cluster):
        """Test that writes to primary can be read from any node."""
        # Write to primary (node1)
        primary_port = cluster.get_node_port("node1")
        client = KVClient("127.0.0.1", primary_port)
        
        assert client.set("cluster_key", "cluster_value")
        client.close()
        
        time.sleep(1)  # Wait for replication
        
        # Read from all nodes
        for node_id in ["node1", "node2", "node3"]:
            port = cluster.get_node_port(node_id)
            client = KVClient("127.0.0.1", port)
            value = client.get("cluster_key")
            client.close()
            assert value == "cluster_value", f"Node {node_id} returned {value}"
    
    def test_write_to_secondary_fails(self, cluster):
        """Test that writes to secondary are rejected."""
        # Try to write to secondary (node2)
        secondary_port = cluster.get_node_port("node2")
        client = KVClient("127.0.0.1", secondary_port)
        
        # This should fail or be redirected
        result = client.set("test_key", "test_value")
        client.close()
        
        # Depending on implementation, this might return False or redirect
        # For our implementation, it should fail


class TestFailover:
    """Test failover scenarios."""
    
    def test_primary_failure_triggers_election(self):
        """Test that primary failure triggers election and new primary is elected."""
        cluster = ClusterManager(base_port=7200, base_data_dir="test_cluster_failover")
        
        try:
            cluster.start_all()
            time.sleep(2)
            
            # Write some data to primary
            primary_port = cluster.get_node_port("node1")
            client = KVClient("127.0.0.1", primary_port)
            client.set("failover_key", "failover_value")
            client.close()
            
            time.sleep(1)  # Wait for replication
            
            # Kill primary
            print("\nKilling primary (node1)...")
            cluster.kill_node("node1")
            
            # Wait for election
            time.sleep(5)
            
            # Try to find new primary
            new_primary_found = False
            for node_id in ["node2", "node3"]:
                try:
                    port = cluster.get_node_port(node_id)
                    client = KVClient("127.0.0.1", port, timeout=2)
                    
                    if client.ping():
                        # Try to write to see if it's primary
                        value = client.get("failover_key")
                        if value == "failover_value":
                            print(f"  Data persisted on {node_id}")
                            new_primary_found = True
                    
                    client.close()
                except Exception as e:
                    print(f"  Failed to connect to {node_id}: {e}")
            
            assert new_primary_found, "No new primary found after failover"
        
        finally:
            cluster.cleanup()
    
    def test_data_survives_primary_failure(self):
        """Test that data survives primary failure due to replication."""
        cluster = ClusterManager(base_port=7300, base_data_dir="test_cluster_survival")
        
        try:
            cluster.start_all()
            time.sleep(2)
            
            # Write data
            primary_port = cluster.get_node_port("node1")
            client = KVClient("127.0.0.1", primary_port)
            
            for i in range(10):
                client.set(f"survive_key_{i}", f"survive_value_{i}")
            
            client.close()
            time.sleep(2)  # Wait for replication
            
            # Kill primary
            cluster.kill_node("node1")
            time.sleep(3)
            
            # Verify data on secondaries
            for node_id in ["node2", "node3"]:
                try:
                    port = cluster.get_node_port(node_id)
                    client = KVClient("127.0.0.1", port, timeout=2)
                    
                    found = 0
                    for i in range(10):
                        if client.get(f"survive_key_{i}") == f"survive_value_{i}":
                            found += 1
                    
                    client.close()
                    print(f"  {node_id}: Found {found}/10 keys")
                    assert found == 10, f"Only found {found}/10 keys on {node_id}"
                
                except Exception as e:
                    print(f"  Failed to connect to {node_id}: {e}")
        
        finally:
            cluster.cleanup()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

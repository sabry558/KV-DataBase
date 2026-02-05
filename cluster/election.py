"""
Leader Election Manager.
Implements Bully algorithm for cluster leader election.
"""

import socket
import json
import threading
import time
from typing import List, Tuple, Optional, Dict, Any


class ElectionManager:
    """
    Manages leader election using the Bully algorithm.
    
    The node with the highest ID wins elections.
    Elections are triggered when:
    - A node starts and doesn't see a primary
    - A node detects primary failure (no heartbeat)
    """
    
    def __init__(
        self,
        node_id: str,
        host: str,
        cluster_port: int,
        peers: List[Tuple[str, str, int, int]]  # (id, host, port, cluster_port)
    ):
        self.node_id = node_id
        self.host = host
        self.cluster_port = cluster_port
        self.peers = peers
        
        self._lock = threading.Lock()
        self._election_in_progress = False
        self._current_leader: Optional[str] = None
        self._election_timeout = 3.0
    
    def get_higher_peers(self) -> List[Tuple[str, str, int, int]]:
        """Get peers with higher node IDs (for Bully algorithm)."""
        return [p for p in self.peers if p[0] > self.node_id]
    
    def get_lower_peers(self) -> List[Tuple[str, str, int, int]]:
        """Get peers with lower node IDs."""
        return [p for p in self.peers if p[0] < self.node_id]
    
    def start_election(self) -> Optional[str]:
        """
        Start leader election.
        
        Returns:
            The ID of the new leader, or None if election failed
        """
        with self._lock:
            if self._election_in_progress:
                return self._current_leader
            self._election_in_progress = True
        
        try:
            print(f"[{self.node_id}] Starting Bully election...")
            
            # Send ELECTION message to all higher-ID nodes
            higher_peers = self.get_higher_peers()
            
            if not higher_peers:
                # We have the highest ID, we win
                print(f"[{self.node_id}] No higher peers, becoming leader")
                self._announce_victory()
                return self.node_id
            
            # Check if any higher node responds
            any_response = False
            for peer_id, peer_host, _, peer_cluster_port in higher_peers:
                response = self._send_election_message(peer_host, peer_cluster_port)
                if response and response.get('ok'):
                    any_response = True
                    print(f"[{self.node_id}] Higher peer {peer_id} responded, waiting for coordinator")
                    break
            
            if not any_response:
                # No higher node responded, we win
                print(f"[{self.node_id}] No higher peers responded, becoming leader")
                self._announce_victory()
                return self.node_id
            
            # Wait for coordinator message
            time.sleep(self._election_timeout)
            
            return self._current_leader
        
        finally:
            with self._lock:
                self._election_in_progress = False
    
    def _send_election_message(self, host: str, port: int) -> Optional[Dict[str, Any]]:
        """Send election message to a peer."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((host, port))
            
            message = json.dumps({
                'cmd': 'ELECTION',
                'node_id': self.node_id
            }).encode('utf-8') + b'\n'
            
            sock.sendall(message)
            
            response = b""
            while b'\n' not in response:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response += chunk
            
            sock.close()
            
            if response:
                return json.loads(response.strip().decode('utf-8'))
        except Exception:
            pass
        
        return None
    
    def _announce_victory(self):
        """Announce that we are the new leader."""
        with self._lock:
            self._current_leader = self.node_id
        
        # Send COORDINATOR message to all peers
        for peer_id, peer_host, _, peer_cluster_port in self.peers:
            try:
                self._send_coordinator_message(peer_host, peer_cluster_port)
            except Exception as e:
                print(f"[{self.node_id}] Failed to announce to {peer_id}: {e}")
    
    def _send_coordinator_message(self, host: str, port: int):
        """Send coordinator message to a peer."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((host, port))
            
            message = json.dumps({
                'cmd': 'COORDINATOR',
                'node_id': self.node_id
            }).encode('utf-8') + b'\n'
            
            sock.sendall(message)
            sock.close()
        except Exception:
            pass
    
    def handle_election_message(self, sender_id: str) -> Dict[str, Any]:
        """Handle incoming ELECTION message."""
        print(f"[{self.node_id}] Received ELECTION from {sender_id}")
        
        # If we have higher ID, start our own election
        if self.node_id > sender_id:
            threading.Thread(target=self.start_election, daemon=True).start()
        
        return {'ok': True, 'node_id': self.node_id}
    
    def handle_coordinator_message(self, leader_id: str):
        """Handle incoming COORDINATOR message."""
        with self._lock:
            self._current_leader = leader_id
            print(f"[{self.node_id}] New leader: {leader_id}")
    
    def get_current_leader(self) -> Optional[str]:
        """Get the current leader ID."""
        with self._lock:
            return self._current_leader
    
    def set_current_leader(self, leader_id: str):
        """Set the current leader."""
        with self._lock:
            self._current_leader = leader_id


class QuorumChecker:
    """
    Checks if quorum is maintained to prevent split-brain.
    """
    
    def __init__(self, total_nodes: int):
        self.total_nodes = total_nodes
        self.quorum_size = total_nodes // 2 + 1
    
    def has_quorum(self, reachable_nodes: int) -> bool:
        """
        Check if we have quorum.
        
        Args:
            reachable_nodes: Number of reachable nodes (including self)
            
        Returns:
            True if quorum is maintained
        """
        return reachable_nodes >= self.quorum_size
    
    def check_cluster_health(self, peers: List[Tuple[str, str, int, int]]) -> Tuple[bool, int]:
        """
        Check cluster health by contacting peers.
        
        Returns:
            Tuple of (has_quorum, reachable_count)
        """
        reachable = 1  # Count self
        
        for peer_id, peer_host, _, peer_cluster_port in peers:
            if self._ping_peer(peer_host, peer_cluster_port):
                reachable += 1
        
        return self.has_quorum(reachable), reachable
    
    def _ping_peer(self, host: str, port: int) -> bool:
        """Ping a peer to check if it's alive."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect((host, port))
            
            message = json.dumps({'cmd': 'PING'}).encode('utf-8') + b'\n'
            sock.sendall(message)
            
            response = sock.recv(4096)
            sock.close()
            
            return b'ok' in response
        except Exception:
            return False

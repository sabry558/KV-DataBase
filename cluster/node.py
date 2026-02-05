"""
Cluster Node Implementation.
Handles primary/secondary roles and replication.
"""

import socket
import json
import threading
import time
import os
import sys
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage import KVStore


class NodeRole(Enum):
    PRIMARY = "primary"
    SECONDARY = "secondary"
    CANDIDATE = "candidate"


@dataclass
class NodeInfo:
    """Information about a cluster node."""
    node_id: str
    host: str
    port: int
    role: NodeRole
    last_heartbeat: float = 0.0


class ClusterNode:
    """
    Cluster node that can be primary or secondary.
    
    - Primary: Accepts read/write requests
    - Secondary: Replicates from primary, can be promoted
    """
    
    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 6379,
        cluster_port: int = 7379,
        data_dir: str = "data",
        peers: List[Tuple[str, str, int, int]] = None  # (id, host, port, cluster_port)
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.cluster_port = cluster_port
        self.data_dir = data_dir
        self.peers = peers or []
        
        self._role = NodeRole.SECONDARY
        self._store: Optional[KVStore] = None
        self._running = False
        self._lock = threading.RLock()
        
        # Cluster state
        self._current_primary: Optional[str] = None
        self._term = 0
        self._last_heartbeat = 0.0
        self._heartbeat_timeout = 3.0
        self._election_timeout = 5.0
        
        # Sockets
        self._client_socket: Optional[socket.socket] = None
        self._cluster_socket: Optional[socket.socket] = None
        
        # Threads
        self._client_thread: Optional[threading.Thread] = None
        self._cluster_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._election_thread: Optional[threading.Thread] = None
        
        # Replication state
        self._replication_log: List[Dict[str, Any]] = []
        self._commit_index = 0
    
    @property
    def role(self) -> NodeRole:
        with self._lock:
            return self._role
    
    @property
    def is_primary(self) -> bool:
        with self._lock:
            return self._role == NodeRole.PRIMARY
    
    def start(self):
        """Start the node."""
        print(f"[{self.node_id}] Starting node on {self.host}:{self.port} (cluster: {self.cluster_port})")
        
        # Initialize storage
        node_data_dir = os.path.join(self.data_dir, self.node_id)
        self._store = KVStore(node_data_dir)
        
        self._running = True
        
        # Start client server
        self._client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._client_socket.bind((self.host, self.port))
        self._client_socket.listen(128)
        self._client_socket.settimeout(1.0)
        
        self._client_thread = threading.Thread(target=self._client_loop, daemon=True)
        self._client_thread.start()
        
        # Start cluster server
        self._cluster_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._cluster_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._cluster_socket.bind((self.host, self.cluster_port))
        self._cluster_socket.listen(128)
        self._cluster_socket.settimeout(1.0)
        
        self._cluster_thread = threading.Thread(target=self._cluster_loop, daemon=True)
        self._cluster_thread.start()
        
        # Start heartbeat/election threads
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()
        
        self._election_thread = threading.Thread(target=self._election_loop, daemon=True)
        self._election_thread.start()
        
        print(f"[{self.node_id}] Node started as {self._role.value}")
    
    def stop(self):
        """Stop the node."""
        print(f"[{self.node_id}] Stopping node...")
        self._running = False
        
        if self._store:
            self._store.close()
        
        if self._client_socket:
            try:
                self._client_socket.close()
            except:
                pass
        
        if self._cluster_socket:
            try:
                self._cluster_socket.close()
            except:
                pass
        
        print(f"[{self.node_id}] Node stopped")
    
    def promote_to_primary(self):
        """Promote this node to primary."""
        with self._lock:
            self._role = NodeRole.PRIMARY
            self._current_primary = self.node_id
            self._term += 1
            print(f"[{self.node_id}] Promoted to PRIMARY (term {self._term})")
    
    def demote_to_secondary(self, primary_id: str):
        """Demote this node to secondary."""
        with self._lock:
            self._role = NodeRole.SECONDARY
            self._current_primary = primary_id
            print(f"[{self.node_id}] Demoted to SECONDARY (primary: {primary_id})")
    
    def _client_loop(self):
        """Handle client connections."""
        while self._running:
            try:
                client_socket, address = self._client_socket.accept()
                handler = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,),
                    daemon=True
                )
                handler.start()
            except socket.timeout:
                continue
            except OSError:
                break
    
    def _handle_client(self, client_socket: socket.socket):
        """Handle a single client connection."""
        client_socket.settimeout(None)
        buffer = b""
        
        try:
            while self._running:
                data = client_socket.recv(65536)
                if not data:
                    break
                
                buffer += data
                
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    if line:
                        response = self._process_client_command(line)
                        client_socket.sendall(response + b'\n')
        except Exception as e:
            pass
        finally:
            client_socket.close()
    
    def _process_client_command(self, data: bytes) -> bytes:
        """Process a client command."""
        try:
            request = json.loads(data.decode('utf-8'))
            command = request.get('cmd', '').upper()
            
            # Check if we're primary for write operations
            if command in ('SET', 'DELETE', 'BULK_SET') and not self.is_primary:
                return json.dumps({
                    'ok': False,
                    'error': 'Not primary',
                    'primary': self._current_primary
                }).encode('utf-8')
            
            # Handle commands
            if command == 'SET':
                return self._handle_set(request)
            elif command == 'GET':
                return self._handle_get(request)
            elif command == 'DELETE':
                return self._handle_delete(request)
            elif command == 'BULK_SET':
                return self._handle_bulk_set(request)
            elif command == 'PING':
                return json.dumps({'ok': True, 'pong': True, 'role': self._role.value}).encode('utf-8')
            elif command == 'INFO':
                return self._handle_info()
            else:
                return json.dumps({'ok': False, 'error': f'Unknown command: {command}'}).encode('utf-8')
        
        except Exception as e:
            return json.dumps({'ok': False, 'error': str(e)}).encode('utf-8')
    
    def _handle_set(self, request: Dict[str, Any]) -> bytes:
        """Handle SET command with replication."""
        key = str(request.get('key'))
        value = str(request.get('value'))
        debug = request.get('debug', False)
        
        # Write locally
        seq = self._store.set(key, value, debug)
        
        # Replicate to secondaries
        self._replicate_to_secondaries({'cmd': 'REPLICATE_SET', 'key': key, 'value': value, 'seq': seq})
        
        return json.dumps({'ok': True, 'seq': seq}).encode('utf-8')
    
    def _handle_get(self, request: Dict[str, Any]) -> bytes:
        """Handle GET command."""
        key = str(request.get('key'))
        value = self._store.get(key)
        
        if value is None:
            return json.dumps({'ok': False, 'error': 'Key not found'}).encode('utf-8')
        
        return json.dumps({'ok': True, 'value': value}).encode('utf-8')
    
    def _handle_delete(self, request: Dict[str, Any]) -> bytes:
        """Handle DELETE command with replication."""
        key = str(request.get('key'))
        debug = request.get('debug', False)
        
        deleted = self._store.delete(key, debug)
        
        # Replicate to secondaries
        self._replicate_to_secondaries({'cmd': 'REPLICATE_DELETE', 'key': key})
        
        return json.dumps({'ok': True, 'deleted': deleted}).encode('utf-8')
    
    def _handle_bulk_set(self, request: Dict[str, Any]) -> bytes:
        """Handle BULK_SET command with replication."""
        items = request.get('items', [])
        debug = request.get('debug', False)
        
        pairs = [(str(item['key']), str(item['value'])) for item in items]
        seq = self._store.bulk_set(pairs, debug)
        
        # Replicate
        self._replicate_to_secondaries({'cmd': 'REPLICATE_BULK_SET', 'items': items, 'seq': seq})
        
        return json.dumps({'ok': True, 'seq': seq, 'count': len(pairs)}).encode('utf-8')
    
    def _handle_info(self) -> bytes:
        """Handle INFO command."""
        return json.dumps({
            'ok': True,
            'node_id': self.node_id,
            'role': self._role.value,
            'term': self._term,
            'primary': self._current_primary,
            'host': self.host,
            'port': self.port,
            'cluster_port': self.cluster_port
        }).encode('utf-8')
    
    def _replicate_to_secondaries(self, message: Dict[str, Any]):
        """Replicate a command to all secondary nodes."""
        for peer_id, peer_host, _, peer_cluster_port in self.peers:
            try:
                self._send_cluster_message(peer_host, peer_cluster_port, message)
            except Exception as e:
                print(f"[{self.node_id}] Failed to replicate to {peer_id}: {e}")
    
    def _send_cluster_message(self, host: str, port: int, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Send a message to a cluster peer."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((host, port))
            
            data = json.dumps(message).encode('utf-8') + b'\n'
            sock.sendall(data)
            
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
    
    def _cluster_loop(self):
        """Handle cluster communication."""
        while self._running:
            try:
                client_socket, address = self._cluster_socket.accept()
                handler = threading.Thread(
                    target=self._handle_cluster_peer,
                    args=(client_socket,),
                    daemon=True
                )
                handler.start()
            except socket.timeout:
                continue
            except OSError:
                break
    
    def _handle_cluster_peer(self, client_socket: socket.socket):
        """Handle cluster peer connection."""
        client_socket.settimeout(5.0)
        buffer = b""
        
        try:
            while self._running:
                data = client_socket.recv(65536)
                if not data:
                    break
                
                buffer += data
                
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    if line:
                        response = self._process_cluster_command(line)
                        client_socket.sendall(response + b'\n')
                        break  # One message per connection
        except Exception:
            pass
        finally:
            client_socket.close()
    
    def _process_cluster_command(self, data: bytes) -> bytes:
        """Process a cluster command."""
        try:
            request = json.loads(data.decode('utf-8'))
            command = request.get('cmd', '').upper()
            
            if command == 'HEARTBEAT':
                return self._handle_heartbeat(request)
            elif command == 'REQUEST_VOTE':
                return self._handle_vote_request(request)
            elif command == 'REPLICATE_SET':
                key = request.get('key')
                value = request.get('value')
                self._store.set(key, value)
                return json.dumps({'ok': True}).encode('utf-8')
            elif command == 'REPLICATE_DELETE':
                key = request.get('key')
                self._store.delete(key)
                return json.dumps({'ok': True}).encode('utf-8')
            elif command == 'REPLICATE_BULK_SET':
                items = request.get('items', [])
                pairs = [(str(item['key']), str(item['value'])) for item in items]
                self._store.bulk_set(pairs)
                return json.dumps({'ok': True}).encode('utf-8')
            else:
                return json.dumps({'ok': False, 'error': f'Unknown cluster command: {command}'}).encode('utf-8')
        
        except Exception as e:
            return json.dumps({'ok': False, 'error': str(e)}).encode('utf-8')
    
    def _handle_heartbeat(self, request: Dict[str, Any]) -> bytes:
        """Handle heartbeat from primary."""
        sender_id = request.get('node_id')
        sender_term = request.get('term', 0)
        
        with self._lock:
            if sender_term >= self._term:
                self._term = sender_term
                self._last_heartbeat = time.time()
                self._current_primary = sender_id
                
                if self._role != NodeRole.SECONDARY:
                    self._role = NodeRole.SECONDARY
                    print(f"[{self.node_id}] Became SECONDARY (heartbeat from {sender_id})")
        
        return json.dumps({
            'ok': True,
            'node_id': self.node_id,
            'term': self._term
        }).encode('utf-8')
    
    def _handle_vote_request(self, request: Dict[str, Any]) -> bytes:
        """Handle vote request from candidate."""
        candidate_id = request.get('node_id')
        candidate_term = request.get('term', 0)
        
        with self._lock:
            vote_granted = False
            
            if candidate_term > self._term:
                self._term = candidate_term
                vote_granted = True
                print(f"[{self.node_id}] Voted for {candidate_id} (term {candidate_term})")
        
        return json.dumps({
            'ok': True,
            'vote_granted': vote_granted,
            'term': self._term
        }).encode('utf-8')
    
    def _heartbeat_loop(self):
        """Send heartbeats if primary."""
        while self._running:
            time.sleep(1.0)
            
            if self.is_primary:
                for peer_id, peer_host, _, peer_cluster_port in self.peers:
                    try:
                        self._send_cluster_message(peer_host, peer_cluster_port, {
                            'cmd': 'HEARTBEAT',
                            'node_id': self.node_id,
                            'term': self._term
                        })
                    except Exception:
                        pass
    
    def _election_loop(self):
        """Monitor for primary failure and start election."""
        while self._running:
            time.sleep(0.5)
            
            with self._lock:
                if self._role == NodeRole.SECONDARY:
                    time_since_heartbeat = time.time() - self._last_heartbeat
                    
                    if self._last_heartbeat > 0 and time_since_heartbeat > self._election_timeout:
                        print(f"[{self.node_id}] Primary timeout, starting election...")
                        self._start_election()
    
    def _start_election(self):
        """Start leader election."""
        with self._lock:
            self._role = NodeRole.CANDIDATE
            self._term += 1
            votes = 1  # Vote for self
            
            print(f"[{self.node_id}] Starting election for term {self._term}")
        
        # Request votes from peers
        for peer_id, peer_host, _, peer_cluster_port in self.peers:
            try:
                response = self._send_cluster_message(peer_host, peer_cluster_port, {
                    'cmd': 'REQUEST_VOTE',
                    'node_id': self.node_id,
                    'term': self._term
                })
                
                if response and response.get('vote_granted'):
                    votes += 1
            except Exception:
                pass
        
        # Check if won election (majority)
        quorum = (len(self.peers) + 1) // 2 + 1
        
        with self._lock:
            if votes >= quorum:
                self._role = NodeRole.PRIMARY
                self._current_primary = self.node_id
                print(f"[{self.node_id}] Won election with {votes} votes, becoming PRIMARY")
            else:
                self._role = NodeRole.SECONDARY
                print(f"[{self.node_id}] Lost election with {votes} votes")

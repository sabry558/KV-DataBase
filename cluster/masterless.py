"""
Master-less Replication.
Implements conflict resolution with vector clocks and gossip protocol.
"""

import socket
import json
import threading
import time
import random
import os
import sys
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass, field
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage import KVStore


@dataclass
class VectorClock:
    """Vector clock for conflict detection."""
    clocks: Dict[str, int] = field(default_factory=dict)
    
    def increment(self, node_id: str):
        """Increment clock for a node."""
        self.clocks[node_id] = self.clocks.get(node_id, 0) + 1
    
    def merge(self, other: 'VectorClock'):
        """Merge another vector clock into this one."""
        for node_id, value in other.clocks.items():
            self.clocks[node_id] = max(self.clocks.get(node_id, 0), value)
    
    def happens_before(self, other: 'VectorClock') -> bool:
        """Check if this clock happens-before another."""
        if not self.clocks:
            return bool(other.clocks)
        
        at_least_one_less = False
        for node_id in set(self.clocks.keys()) | set(other.clocks.keys()):
            self_val = self.clocks.get(node_id, 0)
            other_val = other.clocks.get(node_id, 0)
            
            if self_val > other_val:
                return False
            if self_val < other_val:
                at_least_one_less = True
        
        return at_least_one_less
    
    def concurrent(self, other: 'VectorClock') -> bool:
        """Check if two clocks are concurrent (conflict)."""
        return not self.happens_before(other) and not other.happens_before(self)
    
    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary."""
        return self.clocks.copy()
    
    @classmethod
    def from_dict(cls, d: Dict[str, int]) -> 'VectorClock':
        """Create from dictionary."""
        return cls(clocks=d.copy())


@dataclass
class VersionedValue:
    """Value with version information for conflict resolution."""
    value: str
    clock: VectorClock
    timestamp: float
    node_id: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'value': self.value,
            'clock': self.clock.to_dict(),
            'timestamp': self.timestamp,
            'node_id': self.node_id
        }
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'VersionedValue':
        return cls(
            value=d['value'],
            clock=VectorClock.from_dict(d['clock']),
            timestamp=d['timestamp'],
            node_id=d['node_id']
        )


class MasterlessNode:
    """
    Master-less replication node.
    
    Features:
    - All nodes can accept writes
    - Gossip protocol for replication
    - Vector clocks for conflict detection
    - Last-write-wins conflict resolution
    """
    
    def __init__(
        self,
        node_id: str,
        host: str = "127.0.0.1",
        port: int = 6379,
        gossip_port: int = 8379,
        data_dir: str = "data",
        peers: List[Tuple[str, str, int, int]] = None  # (id, host, port, gossip_port)
    ):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.gossip_port = gossip_port
        self.data_dir = data_dir
        self.peers = peers or []
        
        self._running = False
        self._lock = threading.RLock()
        
        # Storage with versioned values
        self._data: Dict[str, VersionedValue] = {}
        self._vector_clock = VectorClock()
        
        # Gossip state
        self._gossip_interval = 1.0
        self._pending_updates: List[Tuple[str, VersionedValue]] = []
        
        # Sockets
        self._client_socket: Optional[socket.socket] = None
        self._gossip_socket: Optional[socket.socket] = None
    
    def start(self):
        """Start the node."""
        print(f"[{self.node_id}] Starting master-less node on {self.host}:{self.port}")
        
        os.makedirs(os.path.join(self.data_dir, self.node_id), exist_ok=True)
        self._running = True
        
        # Client server
        self._client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._client_socket.bind((self.host, self.port))
        self._client_socket.listen(128)
        self._client_socket.settimeout(1.0)
        
        # Gossip server
        self._gossip_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._gossip_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._gossip_socket.bind((self.host, self.gossip_port))
        self._gossip_socket.listen(128)
        self._gossip_socket.settimeout(1.0)
        
        # Start threads
        threading.Thread(target=self._client_loop, daemon=True).start()
        threading.Thread(target=self._gossip_server_loop, daemon=True).start()
        threading.Thread(target=self._gossip_client_loop, daemon=True).start()
        
        print(f"[{self.node_id}] Node started")
    
    def stop(self):
        """Stop the node."""
        self._running = False
        
        if self._client_socket:
            try:
                self._client_socket.close()
            except:
                pass
        
        if self._gossip_socket:
            try:
                self._gossip_socket.close()
            except:
                pass
    
    def set(self, key: str, value: str) -> VersionedValue:
        """Set a key-value pair."""
        with self._lock:
            self._vector_clock.increment(self.node_id)
            
            versioned = VersionedValue(
                value=value,
                clock=VectorClock(self._vector_clock.clocks.copy()),
                timestamp=time.time(),
                node_id=self.node_id
            )
            
            self._data[key] = versioned
            self._pending_updates.append((key, versioned))
            
            return versioned
    
    def get(self, key: str) -> Optional[str]:
        """Get a value by key."""
        with self._lock:
            versioned = self._data.get(key)
            return versioned.value if versioned else None
    
    def delete(self, key: str) -> bool:
        """Delete a key (tombstone)."""
        with self._lock:
            if key in self._data:
                self._vector_clock.increment(self.node_id)
                
                # Use tombstone value
                versioned = VersionedValue(
                    value="__TOMBSTONE__",
                    clock=VectorClock(self._vector_clock.clocks.copy()),
                    timestamp=time.time(),
                    node_id=self.node_id
                )
                
                self._data[key] = versioned
                self._pending_updates.append((key, versioned))
                return True
            return False
    
    def _merge_value(self, key: str, incoming: VersionedValue):
        """Merge incoming value using vector clocks."""
        with self._lock:
            existing = self._data.get(key)
            
            if existing is None:
                # No existing value, accept incoming
                self._data[key] = incoming
                self._vector_clock.merge(incoming.clock)
                return
            
            if existing.clock.happens_before(incoming.clock):
                # Incoming is newer, accept it
                self._data[key] = incoming
                self._vector_clock.merge(incoming.clock)
            elif incoming.clock.happens_before(existing.clock):
                # Existing is newer, keep it
                pass
            else:
                # Concurrent writes - use last-write-wins (by timestamp)
                if incoming.timestamp > existing.timestamp:
                    self._data[key] = incoming
                    self._vector_clock.merge(incoming.clock)
    
    def _client_loop(self):
        """Handle client connections."""
        while self._running:
            try:
                client_socket, _ = self._client_socket.accept()
                threading.Thread(
                    target=self._handle_client,
                    args=(client_socket,),
                    daemon=True
                ).start()
            except socket.timeout:
                continue
            except OSError:
                break
    
    def _handle_client(self, client_socket: socket.socket):
        """Handle client connection."""
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
                        response = self._process_command(line)
                        client_socket.sendall(response + b'\n')
        except:
            pass
        finally:
            client_socket.close()
    
    def _process_command(self, data: bytes) -> bytes:
        """Process client command."""
        try:
            request = json.loads(data.decode('utf-8'))
            cmd = request.get('cmd', '').upper()
            
            if cmd == 'SET':
                key = str(request['key'])
                value = str(request['value'])
                versioned = self.set(key, value)
                return json.dumps({'ok': True, 'version': versioned.to_dict()}).encode('utf-8')
            
            elif cmd == 'GET':
                key = str(request['key'])
                value = self.get(key)
                if value and value != "__TOMBSTONE__":
                    return json.dumps({'ok': True, 'value': value}).encode('utf-8')
                return json.dumps({'ok': False, 'error': 'Key not found'}).encode('utf-8')
            
            elif cmd == 'DELETE':
                key = str(request['key'])
                deleted = self.delete(key)
                return json.dumps({'ok': True, 'deleted': deleted}).encode('utf-8')
            
            elif cmd == 'PING':
                return json.dumps({'ok': True, 'pong': True}).encode('utf-8')
            
            else:
                return json.dumps({'ok': False, 'error': f'Unknown command: {cmd}'}).encode('utf-8')
        
        except Exception as e:
            return json.dumps({'ok': False, 'error': str(e)}).encode('utf-8')
    
    def _gossip_server_loop(self):
        """Handle incoming gossip connections."""
        while self._running:
            try:
                client_socket, _ = self._gossip_socket.accept()
                threading.Thread(
                    target=self._handle_gossip,
                    args=(client_socket,),
                    daemon=True
                ).start()
            except socket.timeout:
                continue
            except OSError:
                break
    
    def _handle_gossip(self, client_socket: socket.socket):
        """Handle incoming gossip."""
        client_socket.settimeout(5.0)
        
        try:
            data = b""
            while b'\n' not in data:
                chunk = client_socket.recv(65536)
                if not chunk:
                    break
                data += chunk
            
            if data:
                message = json.loads(data.strip().decode('utf-8'))
                
                if message.get('cmd') == 'GOSSIP':
                    updates = message.get('updates', [])
                    for update in updates:
                        key = update['key']
                        versioned = VersionedValue.from_dict(update['versioned'])
                        self._merge_value(key, versioned)
                
                client_socket.sendall(json.dumps({'ok': True}).encode('utf-8') + b'\n')
        except:
            pass
        finally:
            client_socket.close()
    
    def _gossip_client_loop(self):
        """Periodically gossip updates to peers."""
        while self._running:
            time.sleep(self._gossip_interval)
            
            # Get pending updates
            with self._lock:
                updates = self._pending_updates.copy()
                self._pending_updates.clear()
            
            if not updates:
                continue
            
            # Gossip to random subset of peers
            peers_to_gossip = random.sample(
                self.peers,
                min(len(self.peers), 2)
            ) if self.peers else []
            
            for peer_id, peer_host, _, peer_gossip_port in peers_to_gossip:
                self._send_gossip(
                    peer_host,
                    peer_gossip_port,
                    [{'key': k, 'versioned': v.to_dict()} for k, v in updates]
                )
    
    def _send_gossip(self, host: str, port: int, updates: List[Dict[str, Any]]):
        """Send gossip to a peer."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect((host, port))
            
            message = json.dumps({
                'cmd': 'GOSSIP',
                'node_id': self.node_id,
                'updates': updates
            }).encode('utf-8') + b'\n'
            
            sock.sendall(message)
            sock.recv(4096)  # Wait for ack
            sock.close()
        except:
            pass

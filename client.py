"""
Client library for the Key-Value Store.
Provides a simple interface for interacting with the KV server.
"""

import socket
import json
from typing import Optional, List, Tuple


class KVClient:
    """
    Client for connecting to the KV Store server.
    
    Example:
        client = KVClient('127.0.0.1', 6379)
        client.set('name', 'Alice')
        print(client.get('name'))  # 'Alice'
        client.close()
    """
    
    def __init__(self, host: str = "127.0.0.1", port: int = 6379, timeout: float = 30.0):
        """
        Initialize the client.
        
        Args:
            host: Server hostname
            port: Server port
            timeout: Socket timeout in seconds
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self._socket: Optional[socket.socket] = None
        self._buffer = b""
        self._connect()
    
    def _connect(self):
        """Establish connection to the server."""
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(self.timeout)
        self._socket.connect((self.host, self.port))
    
    def _send_command(self, command: dict) -> dict:
        """
        Send a command to the server and wait for response.
        
        Args:
            command: Command dictionary
            
        Returns:
            Response dictionary
        """
        if not self._socket:
            self._connect()
        
        # Send command
        data = json.dumps(command).encode('utf-8') + b'\n'
        self._socket.sendall(data)
        
        # Read response (newline-delimited)
        while b'\n' not in self._buffer:
            chunk = self._socket.recv(65536)
            if not chunk:
                raise ConnectionError("Connection closed by server")
            self._buffer += chunk
        
        line, self._buffer = self._buffer.split(b'\n', 1)
        return json.loads(line.decode('utf-8'))
    
    def set(self, key: str, value: str, debug: bool = False) -> bool:
        """
        Set a key-value pair.
        
        Args:
            key: The key to set
            value: The value to store
            debug: Enable debug mode (random write failures for non-WAL)
            
        Returns:
            True if successful
        """
        response = self._send_command({
            'cmd': 'SET',
            'key': key,
            'value': value,
            'debug': debug
        })
        return response.get('ok', False)
    
    def get(self, key: str) -> Optional[str]:
        """
        Get a value by key.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value if found, None otherwise
        """
        response = self._send_command({
            'cmd': 'GET',
            'key': key
        })
        if response.get('ok'):
            return response.get('value')
        return None
    
    def delete(self, key: str) -> bool:
        """
        Delete a key.
        
        Args:
            key: The key to delete
            
        Returns:
            True if key was deleted
        """
        response = self._send_command({
            'cmd': 'DELETE',
            'key': key
        })
        return response.get('ok', False) and response.get('deleted', False)
    
    def bulk_set(self, items: List[Tuple[str, str]], debug: bool = False) -> bool:
        """
        Set multiple key-value pairs atomically.
        
        Args:
            items: List of (key, value) tuples
            debug: Enable debug mode
            
        Returns:
            True if successful
        """
        response = self._send_command({
            'cmd': 'BULK_SET',
            'items': [{'key': k, 'value': v} for k, v in items],
            'debug': debug
        })
        return response.get('ok', False)
    
    def keys(self) -> List[str]:
        """
        Get all keys.
        
        Returns:
            List of all keys
        """
        response = self._send_command({'cmd': 'KEYS'})
        if response.get('ok'):
            return response.get('keys', [])
        return []
    
    def count(self) -> int:
        """
        Get the number of keys.
        
        Returns:
            Number of keys in the store
        """
        response = self._send_command({'cmd': 'COUNT'})
        if response.get('ok'):
            return response.get('count', 0)
        return 0
    
    def ping(self) -> bool:
        """
        Check if server is responsive.
        
        Returns:
            True if server responded
        """
        try:
            response = self._send_command({'cmd': 'PING'})
            return response.get('ok', False) and response.get('pong', False)
        except:
            return False
    
    def close(self):
        """Close the connection."""
        if self._socket:
            try:
                self._socket.close()
            except:
                pass
            self._socket = None
    
    def reconnect(self):
        """Reconnect to the server."""
        self.close()
        self._buffer = b""
        self._connect()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False

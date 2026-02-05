"""
TCP Server for the Key-Value Store.
Handles client connections and processes commands.
"""

import socket
import json
import threading
import signal
import sys
import os
from typing import Optional, Dict, Any, Callable
from storage import KVStore


class KVServer:
    """
    Multi-threaded TCP server for the key-value store.
    Uses JSON-based protocol for communication.
    """
    
    def __init__(self, host: str = "127.0.0.1", port: int = 6379, data_dir: str = "data"):
        self.host = host
        self.port = port
        self.data_dir = data_dir
        self._store: Optional[KVStore] = None
        self._socket: Optional[socket.socket] = None
        self._running = False
        self._clients: list = []
        self._lock = threading.Lock()
        
        # Command handlers
        self._handlers: Dict[str, Callable] = {
            'SET': self._handle_set,
            'GET': self._handle_get,
            'DELETE': self._handle_delete,
            'BULK_SET': self._handle_bulk_set,
            'KEYS': self._handle_keys,
            'COUNT': self._handle_count,
            'PING': self._handle_ping,
        }
    
    def start(self):
        """Start the server."""
        # Initialize storage
        self._store = KVStore(self.data_dir)
        
        # Create server socket
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind((self.host, self.port))
        self._socket.listen(128)
        self._socket.settimeout(1.0)  # Allow periodic checks for shutdown
        
        self._running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        print(f"KV Server started on {self.host}:{self.port}")
        
        # Accept connections
        while self._running:
            try:
                client_socket, address = self._socket.accept()
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address),
                    daemon=True
                )
                with self._lock:
                    self._clients.append(client_socket)
                client_thread.start()
            except socket.timeout:
                continue
            except OSError:
                break
        
        self._cleanup()
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        print("\nShutting down gracefully...")
        self._running = False
    
    def _cleanup(self):
        """Clean up resources."""
        # Close all client connections
        with self._lock:
            for client in self._clients:
                try:
                    client.close()
                except:
                    pass
            self._clients.clear()
        
        # Close server socket
        if self._socket:
            try:
                self._socket.close()
            except:
                pass
        
        # Close storage gracefully
        if self._store:
            self._store.close()
        
        print("Server stopped.")
    
    def stop(self):
        """Stop the server."""
        self._running = False
    
    def _handle_client(self, client_socket: socket.socket, address):
        """Handle a client connection."""
        client_socket.settimeout(None)  # No timeout for client operations
        buffer = b""
        
        try:
            while self._running:
                # Read data
                try:
                    data = client_socket.recv(65536)
                    if not data:
                        break
                    
                    buffer += data
                    
                    # Process complete messages (newline-delimited JSON)
                    while b'\n' in buffer:
                        line, buffer = buffer.split(b'\n', 1)
                        if line:
                            response = self._process_command(line)
                            client_socket.sendall(response + b'\n')
                
                except socket.timeout:
                    continue
                except ConnectionResetError:
                    break
        except Exception as e:
            print(f"Error handling client {address}: {e}")
        finally:
            with self._lock:
                if client_socket in self._clients:
                    self._clients.remove(client_socket)
            client_socket.close()
    
    def _process_command(self, data: bytes) -> bytes:
        """Process a command and return response."""
        try:
            request = json.loads(data.decode('utf-8'))
            command = request.get('cmd', '').upper()
            
            handler = self._handlers.get(command)
            if handler:
                result = handler(request)
                return json.dumps(result).encode('utf-8')
            else:
                return json.dumps({
                    'ok': False,
                    'error': f'Unknown command: {command}'
                }).encode('utf-8')
        
        except json.JSONDecodeError:
            return json.dumps({
                'ok': False,
                'error': 'Invalid JSON'
            }).encode('utf-8')
        except Exception as e:
            return json.dumps({
                'ok': False,
                'error': str(e)
            }).encode('utf-8')
    
    def _handle_set(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle SET command."""
        key = request.get('key')
        value = request.get('value')
        debug = request.get('debug', False)
        
        if key is None or value is None:
            return {'ok': False, 'error': 'Missing key or value'}
        
        seq = self._store.set(str(key), str(value), debug)
        return {'ok': True, 'seq': seq}
    
    def _handle_get(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle GET command."""
        key = request.get('key')
        
        if key is None:
            return {'ok': False, 'error': 'Missing key'}
        
        value = self._store.get(str(key))
        if value is None:
            return {'ok': False, 'error': 'Key not found'}
        
        return {'ok': True, 'value': value}
    
    def _handle_delete(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle DELETE command."""
        key = request.get('key')
        debug = request.get('debug', False)
        
        if key is None:
            return {'ok': False, 'error': 'Missing key'}
        
        deleted = self._store.delete(str(key), debug)
        return {'ok': True, 'deleted': deleted}
    
    def _handle_bulk_set(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle BULK_SET command."""
        items = request.get('items', [])
        debug = request.get('debug', False)
        
        if not items:
            return {'ok': False, 'error': 'No items provided'}
        
        # Convert to list of tuples
        pairs = [(str(item['key']), str(item['value'])) for item in items]
        seq = self._store.bulk_set(pairs, debug)
        return {'ok': True, 'seq': seq, 'count': len(pairs)}
    
    def _handle_keys(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle KEYS command."""
        keys = self._store.keys()
        return {'ok': True, 'keys': keys}
    
    def _handle_count(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle COUNT command."""
        count = self._store.count()
        return {'ok': True, 'count': count}
    
    def _handle_ping(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle PING command."""
        return {'ok': True, 'pong': True}


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='KV Store Server')
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind to')
    parser.add_argument('--port', type=int, default=6379, help='Port to bind to')
    parser.add_argument('--data-dir', default='data', help='Data directory')
    
    args = parser.parse_args()
    
    server = KVServer(host=args.host, port=args.port, data_dir=args.data_dir)
    server.start()


if __name__ == '__main__':
    main()

"""
Core Key-Value Store Engine.
Supports Set, Get, Delete, and BulkSet operations with persistence.
"""

import os
import json
import random
import threading
from typing import Optional, List, Tuple, Dict, Any
from wal import WriteAheadLog, Operation


class KVStore:
    """
    In-memory key-value store with Write-Ahead Log for durability.
    All writes are persisted to WAL before acknowledgment.
    """
    
    def __init__(self, data_dir: str = "data", checkpoint_interval: int = 1000):
        self.data_dir = data_dir
        self._data: Dict[str, str] = {}
        self._lock = threading.RLock()
        self._wal = WriteAheadLog(data_dir)
        self._checkpoint_interval = checkpoint_interval
        self._ops_since_checkpoint = 0
        self._data_file = os.path.join(data_dir, "data.json")
        
        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        # Recover state
        self._recover()
    
    def _recover(self):
        """Recover state from checkpoint and WAL."""
        # First, load checkpoint if exists
        checkpoint_state = self._wal.load_checkpoint()
        if checkpoint_state:
            self._data = checkpoint_state.copy()
        
        # Then replay WAL entries
        entries = self._wal.recover()
        for entry in entries:
            if entry.operation == Operation.SET:
                self._data[entry.key] = entry.value
            elif entry.operation == Operation.DELETE:
                self._data.pop(entry.key, None)
    
    def _maybe_checkpoint(self):
        """Create checkpoint if enough operations have occurred."""
        self._ops_since_checkpoint += 1
        if self._ops_since_checkpoint >= self._checkpoint_interval:
            self.checkpoint()
    
    def _save(self, debug: bool = False):
        """
        Save data to disk (non-WAL backup).
        
        Args:
            debug: If True, randomly skip writes to simulate FS sync issues
        """
        if debug:
            if random.random() < 0.01:  # 1% chance to skip
                return
        
        temp_file = self._data_file + ".tmp"
        with open(temp_file, "w") as f:
            json.dump(self._data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, self._data_file)
    
    def set(self, key: str, value: str, debug: bool = False) -> int:
        """
        Set a key-value pair.
        
        Args:
            key: The key to set
            value: The value to store
            debug: If True, enable debug mode (random write failures for non-WAL)
            
        Returns:
            Sequence number for the operation
        """
        with self._lock:
            # Write to WAL first (synchronous, always happens)
            seq = self._wal.append(Operation.SET, key, value)
            
            # Update in-memory state
            self._data[key] = value
            
            # Save to disk (may randomly fail in debug mode)
            self._save(debug)
            
            # Maybe checkpoint
            self._maybe_checkpoint()
            
            return seq
    
    def get(self, key: str) -> Optional[str]:
        """
        Get a value by key.
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value if found, None otherwise
        """
        with self._lock:
            return self._data.get(key)
    
    def delete(self, key: str, debug: bool = False) -> bool:
        """
        Delete a key.
        
        Args:
            key: The key to delete
            debug: If True, enable debug mode
            
        Returns:
            True if key existed and was deleted, False otherwise
        """
        with self._lock:
            if key not in self._data:
                return False
            
            # Write to WAL first
            self._wal.append(Operation.DELETE, key)
            
            # Update in-memory state
            del self._data[key]
            
            # Save to disk
            self._save(debug)
            
            # Maybe checkpoint
            self._maybe_checkpoint()
            
            return True
    
    def bulk_set(self, items: List[Tuple[str, str]], debug: bool = False) -> int:
        """
        Set multiple key-value pairs atomically.
        
        Args:
            items: List of (key, value) tuples
            debug: If True, enable debug mode
            
        Returns:
            Final sequence number
        """
        if not items:
            return self._wal.get_sequence()
        
        with self._lock:
            # Write all to WAL atomically
            seq = self._wal.append_bulk(items)
            
            # Update in-memory state
            for key, value in items:
                self._data[key] = value
            
            # Save to disk
            self._save(debug)
            
            # Maybe checkpoint
            self._maybe_checkpoint()
            
            return seq
    
    def checkpoint(self):
        """Create a checkpoint of current state."""
        with self._lock:
            self._wal.checkpoint(self._data.copy())
            self._ops_since_checkpoint = 0
    
    def keys(self) -> List[str]:
        """Get all keys."""
        with self._lock:
            return list(self._data.keys())
    
    def count(self) -> int:
        """Get number of keys."""
        with self._lock:
            return len(self._data)
    
    def clear(self):
        """Clear all data (for testing)."""
        with self._lock:
            self._data.clear()
            # Re-initialize WAL
            self._wal.close()
            # Remove all files
            for f in os.listdir(self.data_dir):
                os.remove(os.path.join(self.data_dir, f))
            self._wal = WriteAheadLog(self.data_dir)
    
    def close(self):
        """Close the store gracefully."""
        with self._lock:
            self.checkpoint()
            self._wal.close()
    
    def get_sequence(self) -> int:
        """Get current WAL sequence number."""
        return self._wal.get_sequence()

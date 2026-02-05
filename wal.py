"""
Write-Ahead Log (WAL) for durability.
Ensures all writes are persisted before acknowledgment.
"""

import os
import json
import struct
import threading
import time
from typing import Optional, List, Tuple, Dict, Any
from dataclasses import dataclass
from enum import Enum


class Operation(Enum):
    SET = 1
    DELETE = 2
    BULK_SET = 3
    CHECKPOINT = 4


@dataclass
class WALEntry:
    """Represents a single WAL entry."""
    sequence: int
    operation: Operation
    key: str
    value: Optional[str]
    timestamp: float
    
    def to_bytes(self) -> bytes:
        """Serialize entry to bytes."""
        data = {
            'seq': self.sequence,
            'op': self.operation.value,
            'key': self.key,
            'value': self.value,
            'ts': self.timestamp
        }
        json_bytes = json.dumps(data).encode('utf-8')
        # Format: length (4 bytes) + data + checksum (4 bytes)
        length = len(json_bytes)
        checksum = sum(json_bytes) & 0xFFFFFFFF
        return struct.pack('>I', length) + json_bytes + struct.pack('>I', checksum)
    
    @classmethod
    def from_bytes(cls, data: bytes) -> Tuple['WALEntry', int]:
        """Deserialize entry from bytes. Returns entry and bytes consumed."""
        if len(data) < 8:
            raise ValueError("Insufficient data")
        
        length = struct.unpack('>I', data[:4])[0]
        if len(data) < 8 + length:
            raise ValueError("Insufficient data for entry")
        
        json_bytes = data[4:4+length]
        stored_checksum = struct.unpack('>I', data[4+length:8+length])[0]
        
        # Verify checksum
        computed_checksum = sum(json_bytes) & 0xFFFFFFFF
        if stored_checksum != computed_checksum:
            raise ValueError("Checksum mismatch - corrupted entry")
        
        entry_data = json.loads(json_bytes.decode('utf-8'))
        entry = cls(
            sequence=entry_data['seq'],
            operation=Operation(entry_data['op']),
            key=entry_data['key'],
            value=entry_data['value'],
            timestamp=entry_data['ts']
        )
        return entry, 8 + length


class WriteAheadLog:
    """
    Write-Ahead Log for durability.
    All operations are written to WAL and fsynced before acknowledgment.
    """
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.wal_file = os.path.join(data_dir, "wal.log")
        self.checkpoint_file = os.path.join(data_dir, "checkpoint.json")
        self._lock = threading.Lock()
        self._sequence = 0
        self._file_handle: Optional[Any] = None
        
        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        # Load last sequence number
        self._load_sequence()
    
    def _load_sequence(self):
        """Load the last sequence number from checkpoint or WAL."""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    self._sequence = data.get('sequence', 0)
            except (json.JSONDecodeError, IOError):
                self._sequence = 0
        
        # Also check WAL for any entries after checkpoint
        entries = self.recover()
        if entries:
            self._sequence = max(e.sequence for e in entries)
    
    def _get_file_handle(self):
        """Get or create file handle for WAL."""
        if self._file_handle is None:
            self._file_handle = open(self.wal_file, 'ab')
        return self._file_handle
    
    def append(self, operation: Operation, key: str, value: Optional[str] = None) -> int:
        """
        Append an entry to the WAL and fsync.
        Returns the sequence number.
        """
        with self._lock:
            self._sequence += 1
            entry = WALEntry(
                sequence=self._sequence,
                operation=operation,
                key=key,
                value=value,
                timestamp=time.time()
            )
            
            fh = self._get_file_handle()
            fh.write(entry.to_bytes())
            fh.flush()
            os.fsync(fh.fileno())  # Ensure durability
            
            return self._sequence
    
    def append_bulk(self, items: List[Tuple[str, str]]) -> int:
        """
        Append bulk set entries to WAL atomically.
        Returns the final sequence number.
        """
        with self._lock:
            fh = self._get_file_handle()
            
            for key, value in items:
                self._sequence += 1
                entry = WALEntry(
                    sequence=self._sequence,
                    operation=Operation.SET,
                    key=key,
                    value=value,
                    timestamp=time.time()
                )
                fh.write(entry.to_bytes())
            
            # Single fsync for all entries (atomic bulk)
            fh.flush()
            os.fsync(fh.fileno())
            
            return self._sequence
    
    def recover(self) -> List[WALEntry]:
        """
        Read all entries from WAL for recovery.
        Returns list of valid entries.
        """
        entries = []
        
        if not os.path.exists(self.wal_file):
            return entries
        
        try:
            with open(self.wal_file, 'rb') as f:
                data = f.read()
            
            offset = 0
            while offset < len(data):
                try:
                    entry, consumed = WALEntry.from_bytes(data[offset:])
                    entries.append(entry)
                    offset += consumed
                except ValueError:
                    # Corrupted entry, stop recovery here
                    break
        except IOError:
            pass
        
        return entries
    
    def checkpoint(self, state: Dict[str, str]):
        """
        Create a checkpoint with current state.
        This allows WAL truncation.
        """
        with self._lock:
            # Write checkpoint atomically
            temp_file = self.checkpoint_file + ".tmp"
            with open(temp_file, 'w') as f:
                json.dump({
                    'sequence': self._sequence,
                    'state': state,
                    'timestamp': time.time()
                }, f)
                f.flush()
                os.fsync(f.fileno())
            
            # Atomic rename
            os.replace(temp_file, self.checkpoint_file)
            
            # Close current WAL handle
            if self._file_handle:
                self._file_handle.close()
                self._file_handle = None
            
            # Truncate WAL
            with open(self.wal_file, 'wb') as f:
                f.flush()
                os.fsync(f.fileno())
    
    def load_checkpoint(self) -> Optional[Dict[str, str]]:
        """Load state from checkpoint file."""
        if not os.path.exists(self.checkpoint_file):
            return None
        
        try:
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)
                return data.get('state', {})
        except (json.JSONDecodeError, IOError):
            return None
    
    def close(self):
        """Close the WAL file handle."""
        with self._lock:
            if self._file_handle:
                self._file_handle.close()
                self._file_handle = None
    
    def get_sequence(self) -> int:
        """Get current sequence number."""
        return self._sequence

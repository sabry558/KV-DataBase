"""
Inverted Index for Full-Text Search.
Enables searching values by words/terms.
"""

import re
import os
import json
import threading
from typing import Dict, List, Set, Optional
from collections import defaultdict


class InvertedIndex:
    """
    Inverted index for full-text search on values.
    
    Features:
    - Tokenization and normalization
    - Posting lists for fast lookup
    - Phrase and multi-word search
    """
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.index_file = os.path.join(data_dir, "inverted_index.json")
        self._lock = threading.RLock()
        
        # term -> list of (key, position)
        self._index: Dict[str, List[tuple]] = defaultdict(list)
        
        # key -> list of tokens (for deletion)
        self._key_tokens: Dict[str, List[str]] = {}
        
        # Load existing index
        os.makedirs(data_dir, exist_ok=True)
        self._load()
    
    def _tokenize(self, text: str) -> List[str]:
        """Tokenize and normalize text."""
        # Lowercase and split on non-alphanumeric
        text = text.lower()
        tokens = re.findall(r'\b[a-z0-9]+\b', text)
        return tokens
    
    def _normalize(self, token: str) -> str:
        """Normalize a single token."""
        return token.lower().strip()
    
    def index_value(self, key: str, value: str):
        """
        Index a value for a key.
        
        Args:
            key: The key in the KV store
            value: The value to index
        """
        with self._lock:
            # Remove old tokens for this key if exists
            if key in self._key_tokens:
                self._remove_key(key)
            
            # Tokenize value
            tokens = self._tokenize(value)
            self._key_tokens[key] = tokens
            
            # Add to inverted index with positions
            for position, token in enumerate(tokens):
                self._index[token].append((key, position))
    
    def remove_key(self, key: str):
        """Remove a key from the index."""
        with self._lock:
            self._remove_key(key)
    
    def _remove_key(self, key: str):
        """Internal remove (without lock)."""
        if key not in self._key_tokens:
            return
        
        tokens = self._key_tokens[key]
        for token in set(tokens):
            if token in self._index:
                self._index[token] = [
                    (k, p) for k, p in self._index[token] if k != key
                ]
                if not self._index[token]:
                    del self._index[token]
        
        del self._key_tokens[key]
    
    def search(self, query: str) -> List[str]:
        """
        Search for keys matching the query.
        
        Args:
            query: Search query (can be multiple words)
            
        Returns:
            List of matching keys
        """
        with self._lock:
            query_tokens = self._tokenize(query)
            
            if not query_tokens:
                return []
            
            # Get keys that contain all tokens
            result_keys: Optional[Set[str]] = None
            
            for token in query_tokens:
                if token not in self._index:
                    return []
                
                keys_with_token = {k for k, _ in self._index[token]}
                
                if result_keys is None:
                    result_keys = keys_with_token
                else:
                    result_keys &= keys_with_token
            
            return list(result_keys or [])
    
    def search_prefix(self, prefix: str) -> List[str]:
        """
        Search for keys containing words with given prefix.
        
        Args:
            prefix: Word prefix to search
            
        Returns:
            List of matching keys
        """
        with self._lock:
            prefix = prefix.lower()
            result_keys: Set[str] = set()
            
            for token in self._index:
                if token.startswith(prefix):
                    for key, _ in self._index[token]:
                        result_keys.add(key)
            
            return list(result_keys)
    
    def search_phrase(self, phrase: str) -> List[str]:
        """
        Search for keys containing exact phrase.
        
        Args:
            phrase: Exact phrase to search
            
        Returns:
            List of matching keys
        """
        with self._lock:
            tokens = self._tokenize(phrase)
            
            if not tokens:
                return []
            
            if len(tokens) == 1:
                return self.search(tokens[0])
            
            # First, find keys that have first token
            first_token = tokens[0]
            if first_token not in self._index:
                return []
            
            candidates = {}  # key -> positions of first token
            for key, pos in self._index[first_token]:
                if key not in candidates:
                    candidates[key] = []
                candidates[key].append(pos)
            
            # Check for consecutive tokens
            result_keys = []
            
            for key, first_positions in candidates.items():
                key_tokens = self._key_tokens.get(key, [])
                
                for first_pos in first_positions:
                    # Check if all tokens appear consecutively
                    match = True
                    for i, token in enumerate(tokens):
                        expected_pos = first_pos + i
                        if expected_pos >= len(key_tokens) or key_tokens[expected_pos] != token:
                            match = False
                            break
                    
                    if match:
                        result_keys.append(key)
                        break
            
            return result_keys
    
    def get_term_frequency(self, term: str) -> int:
        """Get number of documents containing term."""
        with self._lock:
            term = term.lower()
            if term in self._index:
                return len(set(k for k, _ in self._index[term]))
            return 0
    
    def get_all_terms(self) -> List[str]:
        """Get all indexed terms."""
        with self._lock:
            return list(self._index.keys())
    
    def save(self):
        """Save index to disk."""
        with self._lock:
            data = {
                'index': {k: v for k, v in self._index.items()},
                'key_tokens': self._key_tokens
            }
            
            temp_file = self.index_file + ".tmp"
            with open(temp_file, 'w') as f:
                json.dump(data, f)
            os.replace(temp_file, self.index_file)
    
    def _load(self):
        """Load index from disk."""
        if not os.path.exists(self.index_file):
            return
        
        try:
            with open(self.index_file, 'r') as f:
                data = json.load(f)
            
            self._index = defaultdict(list, {
                k: [tuple(item) for item in v] 
                for k, v in data.get('index', {}).items()
            })
            self._key_tokens = data.get('key_tokens', {})
        except (json.JSONDecodeError, IOError):
            pass
    
    def clear(self):
        """Clear the index."""
        with self._lock:
            self._index.clear()
            self._key_tokens.clear()
            if os.path.exists(self.index_file):
                os.remove(self.index_file)


class IndexedKVStore:
    """
    KV Store with integrated inverted index.
    Automatically indexes values on set.
    """
    
    def __init__(self, store, data_dir: str = "data"):
        self._store = store
        self._index = InvertedIndex(data_dir)
    
    def set(self, key: str, value: str, **kwargs) -> int:
        """Set value and index it."""
        seq = self._store.set(key, value, **kwargs)
        self._index.index_value(key, value)
        return seq
    
    def get(self, key: str) -> Optional[str]:
        """Get value by key."""
        return self._store.get(key)
    
    def delete(self, key: str, **kwargs) -> bool:
        """Delete key and remove from index."""
        result = self._store.delete(key, **kwargs)
        if result:
            self._index.remove_key(key)
        return result
    
    def search(self, query: str) -> List[str]:
        """Search for keys by value content."""
        return self._index.search(query)
    
    def search_phrase(self, phrase: str) -> List[str]:
        """Search for keys containing exact phrase."""
        return self._index.search_phrase(phrase)
    
    def search_prefix(self, prefix: str) -> List[str]:
        """Search for keys with word prefix."""
        return self._index.search_prefix(prefix)
    
    def save_index(self):
        """Save the index to disk."""
        self._index.save()

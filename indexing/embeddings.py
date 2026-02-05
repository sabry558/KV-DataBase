"""
Word Embedding Index.
Enables semantic similarity search on values using word embeddings.
"""

import os
import json
import math
import threading
from typing import Dict, List, Optional, Tuple
from collections import defaultdict


class SimpleEmbedding:
    """
    Simple word embedding using character n-gram hashing.
    This is a lightweight local solution that doesn't require external APIs.
    For production, you might want to use sentence-transformers or OpenAI embeddings.
    """
    
    def __init__(self, dimension: int = 128):
        self.dimension = dimension
    
    def embed_text(self, text: str) -> List[float]:
        """
        Create embedding for text using character n-gram hashing.
        
        This is a simple approach that captures:
        - Character-level patterns (good for typos)
        - Word-level patterns
        """
        text = text.lower()
        
        # Initialize embedding vector
        embedding = [0.0] * self.dimension
        
        # Character 3-grams
        for i in range(len(text) - 2):
            ngram = text[i:i+3]
            h = hash(ngram) % self.dimension
            embedding[h] += 1.0
        
        # Word-level
        words = text.split()
        for word in words:
            h = hash(word) % self.dimension
            embedding[h] += 2.0  # Weight words more
        
        # Normalize
        magnitude = math.sqrt(sum(x*x for x in embedding))
        if magnitude > 0:
            embedding = [x / magnitude for x in embedding]
        
        return embedding
    
    @staticmethod
    def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        if len(vec1) != len(vec2):
            return 0.0
        
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        mag1 = math.sqrt(sum(a * a for a in vec1))
        mag2 = math.sqrt(sum(b * b for b in vec2))
        
        if mag1 == 0 or mag2 == 0:
            return 0.0
        
        return dot_product / (mag1 * mag2)


class EmbeddingIndex:
    """
    Vector index for semantic similarity search.
    
    Features:
    - Store embeddings for values
    - K-nearest neighbor search
    - Persisted to disk
    """
    
    def __init__(self, data_dir: str = "data", dimension: int = 128):
        self.data_dir = data_dir
        self.dimension = dimension
        self.index_file = os.path.join(data_dir, "embeddings.json")
        self._lock = threading.RLock()
        
        self._embedder = SimpleEmbedding(dimension)
        self._embeddings: Dict[str, List[float]] = {}
        self._values: Dict[str, str] = {}  # key -> original value
        
        os.makedirs(data_dir, exist_ok=True)
        self._load()
    
    def index_value(self, key: str, value: str):
        """
        Create and store embedding for a value.
        
        Args:
            key: Key in the KV store
            value: Value to embed and index
        """
        with self._lock:
            embedding = self._embedder.embed_text(value)
            self._embeddings[key] = embedding
            self._values[key] = value
    
    def remove_key(self, key: str):
        """Remove a key from the index."""
        with self._lock:
            self._embeddings.pop(key, None)
            self._values.pop(key, None)
    
    def search_similar(self, query: str, k: int = 10, threshold: float = 0.0) -> List[Tuple[str, float]]:
        """
        Find keys with values most similar to query.
        
        Args:
            query: Search query text
            k: Maximum number of results
            threshold: Minimum similarity score (0-1)
            
        Returns:
            List of (key, similarity_score) tuples, sorted by similarity
        """
        with self._lock:
            query_embedding = self._embedder.embed_text(query)
            
            results = []
            for key, embedding in self._embeddings.items():
                similarity = SimpleEmbedding.cosine_similarity(query_embedding, embedding)
                if similarity >= threshold:
                    results.append((key, similarity))
            
            # Sort by similarity (descending)
            results.sort(key=lambda x: -x[1])
            
            return results[:k]
    
    def find_similar_to_key(self, key: str, k: int = 10) -> List[Tuple[str, float]]:
        """
        Find keys with values similar to the value of given key.
        
        Args:
            key: Key to find similar values for
            k: Maximum number of results
            
        Returns:
            List of (key, similarity_score) tuples
        """
        with self._lock:
            if key not in self._embeddings:
                return []
            
            query_embedding = self._embeddings[key]
            
            results = []
            for other_key, embedding in self._embeddings.items():
                if other_key == key:
                    continue
                
                similarity = SimpleEmbedding.cosine_similarity(query_embedding, embedding)
                results.append((other_key, similarity))
            
            results.sort(key=lambda x: -x[1])
            
            return results[:k]
    
    def get_embedding(self, key: str) -> Optional[List[float]]:
        """Get the embedding for a key."""
        with self._lock:
            return self._embeddings.get(key)
    
    def save(self):
        """Save embeddings to disk."""
        with self._lock:
            data = {
                'dimension': self.dimension,
                'embeddings': self._embeddings,
                'values': self._values
            }
            
            temp_file = self.index_file + ".tmp"
            with open(temp_file, 'w') as f:
                json.dump(data, f)
            os.replace(temp_file, self.index_file)
    
    def _load(self):
        """Load embeddings from disk."""
        if not os.path.exists(self.index_file):
            return
        
        try:
            with open(self.index_file, 'r') as f:
                data = json.load(f)
            
            self.dimension = data.get('dimension', self.dimension)
            self._embeddings = data.get('embeddings', {})
            self._values = data.get('values', {})
        except (json.JSONDecodeError, IOError):
            pass
    
    def clear(self):
        """Clear the index."""
        with self._lock:
            self._embeddings.clear()
            self._values.clear()
            if os.path.exists(self.index_file):
                os.remove(self.index_file)


class SemanticKVStore:
    """
    KV Store with semantic search capabilities.
    Automatically creates embeddings for values.
    """
    
    def __init__(self, store, data_dir: str = "data"):
        self._store = store
        self._embedding_index = EmbeddingIndex(data_dir)
    
    def set(self, key: str, value: str, **kwargs):
        """Set value and create embedding."""
        result = self._store.set(key, value, **kwargs)
        self._embedding_index.index_value(key, value)
        return result
    
    def get(self, key: str) -> Optional[str]:
        """Get value by key."""
        return self._store.get(key)
    
    def delete(self, key: str, **kwargs) -> bool:
        """Delete key and remove embedding."""
        result = self._store.delete(key, **kwargs)
        if result:
            self._embedding_index.remove_key(key)
        return result
    
    def search_semantic(self, query: str, k: int = 10) -> List[Tuple[str, str, float]]:
        """
        Semantic search for values similar to query.
        
        Returns:
            List of (key, value, similarity) tuples
        """
        similar_keys = self._embedding_index.search_similar(query, k)
        
        results = []
        for key, similarity in similar_keys:
            value = self._store.get(key)
            if value:
                results.append((key, value, similarity))
        
        return results
    
    def find_similar(self, key: str, k: int = 10) -> List[Tuple[str, str, float]]:
        """
        Find values similar to the value of given key.
        
        Returns:
            List of (key, value, similarity) tuples
        """
        similar_keys = self._embedding_index.find_similar_to_key(key, k)
        
        results = []
        for other_key, similarity in similar_keys:
            value = self._store.get(other_key)
            if value:
                results.append((other_key, value, similarity))
        
        return results
    
    def save_index(self):
        """Save embedding index to disk."""
        self._embedding_index.save()

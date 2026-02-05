# KV Database

A high-performance, persistent key-value store with clustering and full-text search.

## Features

- **Core Operations**: Set, Get, Delete, BulkSet
- **Persistence**: Write-Ahead Log (WAL) for 100% durability
- **TCP Server**: JSON-based protocol over TCP
- **Clustering**: Primary-Secondary replication with automatic failover
- **Master-less Mode**: Vector clocks with gossip protocol
- **Indexing**: Full-text search with inverted index
- **Semantic Search**: Word embedding-based similarity search

## Quick Start

### Start Server

```bash
python main.py --port 6379 --data-dir data
```

### Use Client

```python
from client import KVClient

# Connect
client = KVClient("127.0.0.1", 6379)

# Basic operations
client.set("name", "Alice")
print(client.get("name"))  # "Alice"

# Bulk operations
client.bulk_set([("k1", "v1"), ("k2", "v2"), ("k3", "v3")])

# Delete
client.delete("name")

# Close
client.close()
```

### Run Tests

```bash
# Install pytest
pip install pytest

# Run all tests
python -m pytest tests/ -v

# Run specific tests
python -m pytest tests/test_basic.py -v
python -m pytest tests/test_acid.py -v -s
```

### Run Benchmarks

```bash
# Throughput benchmark
python benchmarks/throughput.py

# Durability benchmark
python benchmarks/durability.py
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       Client Layer                           │
│  ┌─────────────────────────────────────────────────────┐    │
│  │   KVClient (TCP)                                     │    │
│  │   - set(key, value)                                  │    │
│  │   - get(key)                                         │    │
│  │   - delete(key)                                      │    │
│  │   - bulk_set(items)                                  │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       Server Layer                           │
│  ┌─────────────────────────────────────────────────────┐    │
│  │   KVServer (Multi-threaded TCP)                      │    │
│  │   - JSON protocol                                    │    │
│  │   - Graceful shutdown                                │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       Storage Layer                          │
│  ┌──────────────────┐    ┌──────────────────┐              │
│  │     KVStore       │    │  Write-Ahead Log │              │
│  │   (In-Memory)     │◄──►│     (WAL)        │              │
│  └──────────────────┘    └──────────────────┘              │
│                                                              │
│  ┌──────────────────┐    ┌──────────────────┐              │
│  │  Inverted Index   │    │  Embedding Index │              │
│  │  (Full-Text)      │    │  (Semantic)      │              │
│  └──────────────────┘    └──────────────────┘              │
└─────────────────────────────────────────────────────────────┘
```

## Cluster Mode

### Start 3-Node Cluster

```bash
# Node 1 (Primary)
python cluster_runner.py --node-id node1 --port 6379 --cluster-port 7379 \
  --peers node2:6380:7380,node3:6381:7381 --primary

# Node 2 (Secondary)
python cluster_runner.py --node-id node2 --port 6380 --cluster-port 7380 \
  --peers node1:6379:7379,node3:6381:7381

# Node 3 (Secondary)
python cluster_runner.py --node-id node3 --port 6381 --cluster-port 7381 \
  --peers node1:6379:7379,node2:6380:7380
```

## File Structure

```
KV Database/
├── main.py              # Entry point
├── server.py            # TCP server
├── client.py            # Client library
├── storage.py           # KV store engine
├── wal.py               # Write-ahead log
├── cluster_runner.py    # Cluster node runner
├── cluster/
│   ├── node.py          # Cluster node
│   ├── election.py      # Leader election
│   └── masterless.py    # Master-less replication
├── indexing/
│   ├── inverted_index.py  # Full-text search
│   └── embeddings.py      # Word embeddings
├── tests/
│   ├── test_basic.py    # Basic tests
│   ├── test_acid.py     # ACID tests
│   └── test_cluster.py  # Cluster tests
└── benchmarks/
    ├── throughput.py    # Write throughput
    └── durability.py    # Durability test
```

## Debug Mode

The debug parameter simulates filesystem sync issues (1% chance to skip non-WAL writes):

```python
client.set("key", "value", debug=True)
client.bulk_set(items, debug=True)
```

## License

MIT

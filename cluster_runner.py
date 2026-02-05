"""
Cluster Runner.
Entry point for running a cluster node.
"""

import argparse
import signal
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cluster.node import ClusterNode


def main():
    parser = argparse.ArgumentParser(description='Run a cluster node')
    parser.add_argument('--node-id', required=True, help='Unique node ID')
    parser.add_argument('--host', default='127.0.0.1', help='Host to bind to')
    parser.add_argument('--port', type=int, required=True, help='Client port')
    parser.add_argument('--cluster-port', type=int, required=True, help='Cluster port')
    parser.add_argument('--data-dir', default='data', help='Data directory')
    parser.add_argument('--peers', default='', help='Comma-separated peers (id:port:cluster_port)')
    parser.add_argument('--primary', action='store_true', help='Start as primary')
    
    args = parser.parse_args()
    
    # Parse peers
    peers = []
    if args.peers:
        for peer_str in args.peers.split(','):
            parts = peer_str.split(':')
            if len(parts) == 3:
                peers.append((parts[0], '127.0.0.1', int(parts[1]), int(parts[2])))
    
    # Create node
    node = ClusterNode(
        node_id=args.node_id,
        host=args.host,
        port=args.port,
        cluster_port=args.cluster_port,
        data_dir=args.data_dir,
        peers=peers
    )
    
    # Signal handler
    def signal_handler(signum, frame):
        print(f"\n[{args.node_id}] Shutting down...")
        node.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start node
    if args.primary:
        node.promote_to_primary()
    
    node.start()
    
    # Keep running
    try:
        while True:
            signal.pause() if hasattr(signal, 'pause') else __import__('time').sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()


if __name__ == '__main__':
    main()

"""
Main entry point for the KV Database.
Supports both standalone and cluster modes.
"""

import argparse
import sys
import os

from server import KVServer


def main():
    parser = argparse.ArgumentParser(
        description='KV Database - A persistent key-value store',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start standalone server
  python main.py --port 6379
  
  # Start with custom data directory
  python main.py --port 6379 --data-dir /path/to/data
  
  # Use the client
  python -c "from client import KVClient; c = KVClient(); c.set('key', 'value'); print(c.get('key'))"
  
  # Run tests
  python -m pytest tests/ -v
  
  # Run benchmarks
  python benchmarks/throughput.py
  python benchmarks/durability.py
"""
    )
    
    parser.add_argument(
        '--host',
        default='127.0.0.1',
        help='Host to bind to (default: 127.0.0.1)'
    )
    parser.add_argument(
        '--port',
        type=int,
        default=6379,
        help='Port to bind to (default: 6379)'
    )
    parser.add_argument(
        '--data-dir',
        default='data',
        help='Data directory for persistence (default: data)'
    )
    parser.add_argument(
        '--version',
        action='version',
        version='KV Database 1.0.0'
    )
    
    args = parser.parse_args()
    
    print("""
╔═══════════════════════════════════════════╗
║           KV Database Server              ║
║  Persistent Key-Value Store with TCP      ║
╚═══════════════════════════════════════════╝
""")
    
    print(f"Configuration:")
    print(f"  Host:     {args.host}")
    print(f"  Port:     {args.port}")
    print(f"  Data Dir: {os.path.abspath(args.data_dir)}")
    print()
    
    server = KVServer(
        host=args.host,
        port=args.port,
        data_dir=args.data_dir
    )
    
    try:
        server.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

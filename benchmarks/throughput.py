"""
Write Throughput Benchmark.
Measures writes per second with varying amounts of pre-populated data.
"""

import subprocess
import time
import sys
import os
import shutil
import statistics
from typing import List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client import KVClient


class BenchmarkServerManager:
    """Manage server for benchmarks."""
    
    def __init__(self, port: int = 6400, data_dir: str = "bench_data"):
        self.port = port
        self.data_dir = data_dir
        self.process = None
    
    def start(self, clean: bool = True):
        """Start server."""
        if clean and os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.process = subprocess.Popen(
            [sys.executable, "server.py", "--port", str(self.port), "--data-dir", self.data_dir],
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(1)
    
    def stop(self):
        """Stop server."""
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            self.process = None
    
    def cleanup(self):
        """Clean data."""
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)


def benchmark_write_throughput(client: KVClient, num_writes: int) -> Tuple[float, float]:
    """
    Benchmark write throughput.
    
    Returns:
        Tuple of (writes_per_second, total_time)
    """
    start_time = time.perf_counter()
    
    for i in range(num_writes):
        client.set(f"bench_key_{i}", f"bench_value_{i}" * 10)
    
    total_time = time.perf_counter() - start_time
    writes_per_second = num_writes / total_time if total_time > 0 else 0
    
    return writes_per_second, total_time


def benchmark_bulk_write_throughput(client: KVClient, num_items: int, batch_size: int = 100) -> Tuple[float, float]:
    """
    Benchmark bulk write throughput.
    
    Returns:
        Tuple of (writes_per_second, total_time)
    """
    start_time = time.perf_counter()
    
    for batch_start in range(0, num_items, batch_size):
        items = [(f"bulk_bench_{i}", f"bulk_value_{i}" * 10) 
                 for i in range(batch_start, min(batch_start + batch_size, num_items))]
        client.bulk_set(items)
    
    total_time = time.perf_counter() - start_time
    writes_per_second = num_items / total_time if total_time > 0 else 0
    
    return writes_per_second, total_time


def pre_populate(client: KVClient, count: int, batch_size: int = 1000):
    """Pre-populate database with data."""
    for batch_start in range(0, count, batch_size):
        items = [(f"prepop_{i}", f"prepop_value_{i}" * 10) 
                 for i in range(batch_start, min(batch_start + batch_size, count))]
        client.bulk_set(items)


def run_throughput_benchmark():
    """Run the full throughput benchmark suite."""
    port = 6400
    data_dir = "bench_data_throughput"
    
    manager = BenchmarkServerManager(port=port, data_dir=data_dir)
    
    # Test configurations
    prepopulate_sizes = [0, 1000, 10000, 100000]
    write_test_count = 10000
    iterations = 3
    
    print("=" * 70)
    print("WRITE THROUGHPUT BENCHMARK")
    print("=" * 70)
    print(f"\nTest: {write_test_count} writes per iteration, {iterations} iterations each")
    print("-" * 70)
    
    results = []
    
    for prepop_size in prepopulate_sizes:
        print(f"\n>>> Pre-populated data: {prepop_size:,} keys")
        
        # Start fresh
        manager.stop()
        manager.start(clean=True)
        
        client = KVClient("127.0.0.1", port)
        
        # Pre-populate
        if prepop_size > 0:
            print(f"    Pre-populating with {prepop_size:,} keys...")
            pre_populate(client, prepop_size)
        
        # Run benchmark iterations
        throughputs = []
        for i in range(iterations):
            wps, duration = benchmark_write_throughput(client, write_test_count)
            throughputs.append(wps)
            print(f"    Iteration {i+1}: {wps:,.0f} writes/sec ({duration:.2f}s)")
        
        avg_throughput = statistics.mean(throughputs)
        std_dev = statistics.stdev(throughputs) if len(throughputs) > 1 else 0
        
        results.append({
            'prepop_size': prepop_size,
            'avg_throughput': avg_throughput,
            'std_dev': std_dev,
            'throughputs': throughputs
        })
        
        print(f"    Average: {avg_throughput:,.0f} ± {std_dev:,.0f} writes/sec")
        
        client.close()
    
    # Bulk write benchmark
    print("\n" + "=" * 70)
    print("BULK WRITE THROUGHPUT BENCHMARK")
    print("=" * 70)
    
    bulk_results = []
    bulk_test_count = 100000
    batch_sizes = [10, 100, 1000]
    
    for batch_size in batch_sizes:
        print(f"\n>>> Batch size: {batch_size}")
        
        manager.stop()
        manager.start(clean=True)
        
        client = KVClient("127.0.0.1", port)
        
        throughputs = []
        for i in range(iterations):
            wps, duration = benchmark_bulk_write_throughput(client, bulk_test_count, batch_size)
            throughputs.append(wps)
            print(f"    Iteration {i+1}: {wps:,.0f} writes/sec ({duration:.2f}s)")
        
        avg_throughput = statistics.mean(throughputs)
        std_dev = statistics.stdev(throughputs) if len(throughputs) > 1 else 0
        
        bulk_results.append({
            'batch_size': batch_size,
            'avg_throughput': avg_throughput,
            'std_dev': std_dev
        })
        
        print(f"    Average: {avg_throughput:,.0f} ± {std_dev:,.0f} writes/sec")
        
        client.close()
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    print("\nSingle Write Throughput by Pre-populated Size:")
    print("-" * 50)
    print(f"{'Pre-pop Size':>15} | {'Throughput (w/s)':>20} | {'Std Dev':>10}")
    print("-" * 50)
    for r in results:
        print(f"{r['prepop_size']:>15,} | {r['avg_throughput']:>20,.0f} | {r['std_dev']:>10,.0f}")
    
    if results:
        baseline = results[0]['avg_throughput']
        print("\nThroughput Degradation:")
        for r in results:
            if baseline > 0:
                pct = (r['avg_throughput'] / baseline) * 100
                print(f"  {r['prepop_size']:,} keys: {pct:.1f}% of baseline")
    
    print("\nBulk Write Throughput by Batch Size:")
    print("-" * 50)
    for r in bulk_results:
        print(f"  Batch {r['batch_size']:>5}: {r['avg_throughput']:>15,.0f} ± {r['std_dev']:>8,.0f} writes/sec")
    
    manager.stop()
    manager.cleanup()
    
    return results, bulk_results


if __name__ == "__main__":
    run_throughput_benchmark()

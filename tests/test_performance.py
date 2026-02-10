"""
Performance Test Suite for py-lsm LSM-Tree Implementation

Benchmarks:
- MemTable: Write throughput, read latency, memory usage, sorting performance
- WAL: Append throughput, replay speed, fsync overhead
- SSTable: Write/read performance, bloom filter effectiveness, sparse index efficiency
- LSMEngine: Mixed workloads, write/read amplification, flush overhead

Run: python tests/test_performance.py
"""

import sys
import time
import shutil
import statistics
import tracemalloc
from pathlib import Path
from contextlib import contextmanager

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from wal import WAL
from memtable import MemTable
from lsm_engine import LSMEngine
from sstable.writer import SSTableWriter
from sstable.reader import SSTableReader
from bloom_filter import BloomFilter


# ============================================================================
# Performance Utilities
# ============================================================================

@contextmanager
def timer(name="Operation"):
    """Context manager for timing operations"""
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    print(f"  {name}: {elapsed:.4f}s")
    return elapsed


def format_throughput(count, elapsed):
    """Format throughput as ops/second"""
    if elapsed == 0:
        return "N/A"
    return f"{count / elapsed:,.0f} ops/sec"


def format_latency(elapsed_ms):
    """Format latency in appropriate units"""
    if elapsed_ms < 1:
        return f"{elapsed_ms * 1000:.2f} us"
    elif elapsed_ms < 1000:
        return f"{elapsed_ms:.2f} ms"
    else:
        return f"{elapsed_ms / 1000:.2f} s"


def measure_memory(func):
    """Measure memory usage of a function"""
    tracemalloc.start()
    func()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return current, peak


def print_stats(name, values):
    """Print statistical summary of measurements"""
    if not values:
        return
    
    print(f"\n  {name} Statistics:")
    print(f"    Mean:   {format_latency(statistics.mean(values))}")
    print(f"    Median: {format_latency(statistics.median(values))}")
    if len(values) > 1:
        print(f"    StdDev: {format_latency(statistics.stdev(values))}")
    if len(values) >= 20:
        sorted_vals = sorted(values)
        p95 = sorted_vals[int(len(sorted_vals) * 0.95)]
        p99 = sorted_vals[int(len(sorted_vals) * 0.99)]
        print(f"    P95:    {format_latency(p95)}")
        print(f"    P99:    {format_latency(p99)}")


# ============================================================================
# Test Directories and Results Storage
# ============================================================================

TEST_WAL_DIR = "perf_test_wal"
TEST_SST_DIR = "perf_test_sst"

# Global dictionary to store benchmark results
RESULTS = {}


def cleanup():
    """Remove test directories"""
    for d in [TEST_WAL_DIR, TEST_SST_DIR]:
        if Path(d).exists():
            shutil.rmtree(d)


def print_summary_table():
    """Print a formatted summary table of all benchmark results"""
    # Box drawing characters
    width = 76  # Total width including borders (80 - 4 spaces from right)
    inner_width = width - 4  # Account for borders
    
    # Top border
    print("\n    ┌" + "─" * inner_width + "┐")
    
    # Title
    title = "PERFORMANCE SUMMARY TABLE"
    padding = (inner_width - len(title)) // 2
    print("    │" + " " * padding + title + " " * (inner_width - padding - len(title)) + "│")
    
    # Separator after title
    print("    ├" + "─" * inner_width + "┤")
    
    # Header row
    header = "{:<38} {:>12} {:>18}".format("Benchmark", "Metric", "Value")
    print("    │ " + header + " │")
    
    # Separator after header
    print("    ├" + "─" * inner_width + "┤")
    
    # Helper function to print a row
    def print_row(benchmark, metric, value):
        row = "{:<38} {:>12} {:>18}".format(benchmark, metric, value)
        print("    │ " + row + " │")
    
    # MemTable Results
    if "memtable_write_100k" in RESULTS:
        print_row("MemTable Write (100K entries)", "Throughput", RESULTS["memtable_write_100k"])
    
    if "memtable_read_avg" in RESULTS:
        print_row("MemTable Read (avg latency)", "Latency", RESULTS["memtable_read_avg"])
    
    if "memtable_sort_50k" in RESULTS:
        print_row("MemTable Sort (50K entries)", "Time", RESULTS["memtable_sort_50k"])
    
    if "memtable_memory_100k" in RESULTS:
        print_row("MemTable Memory (100K entries)", "Memory", RESULTS["memtable_memory_100k"])
    
    # WAL Results
    if "wal_append_5k" in RESULTS:
        print_row("WAL Append (5K entries)", "Throughput", RESULTS["wal_append_5k"])
    
    if "wal_replay_50k" in RESULTS:
        print_row("WAL Replay (50K entries)", "Throughput", RESULTS["wal_replay_50k"])
    
    # SSTable Results
    if "sstable_write_50k" in RESULTS:
        print_row("SSTable Write (50K entries)", "Throughput", RESULTS["sstable_write_50k"])
    
    if "sstable_read_avg" in RESULTS:
        print_row("SSTable Read (avg latency)", "Latency", RESULTS["sstable_read_avg"])
    
    if "bloom_false_positive" in RESULTS:
        print_row("Bloom Filter False Positive", "Rate", RESULTS["bloom_false_positive"])
    
    if "sparse_index_best" in RESULTS:
        print_row("Sparse Index (sparsity=5)", "Avg Read", RESULTS["sparse_index_best"])
    
    # LSMEngine Results
    if "lsm_mixed_overall" in RESULTS:
        print_row("LSMEngine Mixed Workload", "Throughput", RESULTS["lsm_mixed_overall"])
    
    if "lsm_flush_spike" in RESULTS:
        print_row("LSMEngine Flush Latency Spike", "Multiplier", RESULTS["lsm_flush_spike"])
    
    if "lsm_multi_sstable_oldest" in RESULTS:
        print_row("Multi-SSTable Read (oldest)", "Latency", RESULTS["lsm_multi_sstable_oldest"])
    
    # Bottom border
    print("    └" + "─" * inner_width + "┘")


# ============================================================================
# MemTable Performance Tests
# ============================================================================

def bench_memtable_writes():
    """Benchmark MemTable write throughput (should be O(1) avg)"""
    print("\n" + "=" * 70)
    print("MemTable Write Throughput Benchmark")
    print("=" * 70)
    
    for size in [1_000, 10_000, 100_000]:
        mt = MemTable()
        
        start = time.perf_counter()
        for i in range(size):
            mt.set(f"key_{i:08d}", f"value_{i}")
        elapsed = time.perf_counter() - start
        
        throughput = format_throughput(size, elapsed)
        print(f"  {size:,} writes: {elapsed:.4f}s ({throughput})")
        
        if size == 100_000:
            RESULTS["memtable_write_100k"] = throughput
    
    print("  Expected: O(1) average case - consistent throughput")


def bench_memtable_reads():
    """Benchmark MemTable read latency (should be O(1))"""
    print("\n" + "=" * 70)
    print("MemTable Read Latency Benchmark")
    print("=" * 70)
    
    # Prepare dataset
    size = 100_000
    mt = MemTable()
    for i in range(size):
        mt.set(f"key_{i:08d}", f"value_{i}")
    
    # Measure individual read latencies
    latencies = []
    num_reads = 1_000
    
    for i in range(num_reads):
        key = f"key_{i * (size // num_reads):08d}"
        start = time.perf_counter()
        value = mt.get(key)
        elapsed = (time.perf_counter() - start) * 1000  # Convert to ms
        latencies.append(elapsed)
    
    print_stats("Read Latency", latencies)
    print("  Expected: O(1) - consistent low latency")
    
    RESULTS["memtable_read_avg"] = format_latency(statistics.mean(latencies))


def bench_memtable_sorting():
    """Benchmark MemTable sorting performance (should be O(n log n))"""
    print("\n" + "=" * 70)
    print("MemTable Sorting Performance (Flush Operation)")
    print("=" * 70)
    
    for size in [1_000, 10_000, 50_000]:
        mt = MemTable()
        for i in range(size):
            # Insert in random-ish order
            mt.set(f"key_{(i * 7919) % size:08d}", f"value_{i}")
        
        start = time.perf_counter()
        sorted_items = mt.get_sorted_items()
        elapsed = time.perf_counter() - start
        
        print(f"  {size:,} items: {elapsed:.4f}s")
        
        if size == 50_000:
            RESULTS["memtable_sort_50k"] = f"{elapsed:.4f}s"
    
    print("  Expected: O(n log n) - time increases super-linearly")


def bench_memtable_memory():
    """Benchmark MemTable memory usage"""
    print("\n" + "=" * 70)
    print("MemTable Memory Usage")
    print("=" * 70)
    
    for size in [1_000, 10_000, 100_000]:
        def create_memtable():
            mt = MemTable()
            for i in range(size):
                mt.set(f"key_{i:08d}", f"value_{i:08d}")
        
        current, peak = measure_memory(create_memtable)
        mem_mb = peak / 1024 / 1024
        print(f"  {size:,} entries: {mem_mb:.2f} MB (peak)")
        
        if size == 100_000:
            RESULTS["memtable_memory_100k"] = f"{mem_mb:.2f} MB"


# ============================================================================
# WAL Performance Tests
# ============================================================================

def bench_wal_append():
    """Benchmark WAL append throughput with fsync overhead"""
    print("\n" + "=" * 70)
    print("WAL Append Throughput (with fsync)")
    print("=" * 70)
    
    cleanup()
    
    for size in [100, 1_000, 5_000]:
        wal = WAL(TEST_WAL_DIR)
        
        start = time.perf_counter()
        for i in range(size):
            wal.append(f"key_{i:08d}", f"value_{i}")
        elapsed = time.perf_counter() - start
        
        wal.close()
        throughput = format_throughput(size, elapsed)
        print(f"  {size:,} appends: {elapsed:.4f}s ({throughput})")
        
        if size == 5_000:
            RESULTS["wal_append_5k"] = throughput
        
        cleanup()
    
    print("  Note: fsync() significantly impacts throughput")


def bench_wal_replay():
    """Benchmark WAL replay speed"""
    print("\n" + "=" * 70)
    print("WAL Replay Performance")
    print("=" * 70)
    
    cleanup()
    
    for size in [1_000, 10_000, 50_000]:
        # Write WAL
        wal = WAL(TEST_WAL_DIR)
        for i in range(size):
            wal.append(f"key_{i:08d}", f"value_{i}")
        wal.close()
        
        # Measure replay
        wal2 = WAL(TEST_WAL_DIR)
        start = time.perf_counter()
        entries = wal2.replay()
        elapsed = time.perf_counter() - start
        wal2.close()
        
        throughput = format_throughput(len(entries), elapsed)
        print(f"  {size:,} entries: {elapsed:.4f}s ({throughput})")
        
        if size == 50_000:
            RESULTS["wal_replay_50k"] = throughput
        
        cleanup()
    
    print("  Expected: O(n) - linear with WAL size")


# ============================================================================
# SSTable Performance Tests
# ============================================================================

def bench_sstable_write():
    """Benchmark SSTable write throughput"""
    print("\n" + "=" * 70)
    print("SSTable Write Throughput (Flush)")
    print("=" * 70)
    
    cleanup()
    
    for size in [1_000, 10_000, 50_000]:
        # Prepare sorted data
        data = [(f"key_{i:08d}", f"value_{i}") for i in range(size)]
        
        data_file = f"{TEST_SST_DIR}/test_{size}.sst"
        index_file = f"{TEST_SST_DIR}/test_{size}.index"
        Path(TEST_SST_DIR).mkdir(exist_ok=True)
        
        writer = SSTableWriter(data_file, index_file, sparsity_index=10)
        
        start = time.perf_counter()
        writer.write(data)
        elapsed = time.perf_counter() - start
        
        throughput = format_throughput(size, elapsed)
        print(f"  {size:,} entries: {elapsed:.4f}s ({throughput})")
        
        if size == 50_000:
            RESULTS["sstable_write_50k"] = throughput
    
    cleanup()


def bench_sstable_read():
    """Benchmark SSTable read latency (binary search + scan)"""
    print("\n" + "=" * 70)
    print("SSTable Read Latency")
    print("=" * 70)
    
    cleanup()
    
    size = 100_000
    sparsity = 10
    
    # Create SSTable
    data = [(f"key_{i:08d}", f"value_{i}") for i in range(size)]
    data_file = f"{TEST_SST_DIR}/test.sst"
    index_file = f"{TEST_SST_DIR}/test.index"
    Path(TEST_SST_DIR).mkdir(exist_ok=True)
    
    writer = SSTableWriter(data_file, index_file, sparsity_index=sparsity)
    writer.write(data)
    
    # Measure reads
    reader = SSTableReader(data_file, index_file)
    latencies = []
    num_reads = 1_000
    
    for i in range(num_reads):
        key = f"key_{i * (size // num_reads):08d}"
        start = time.perf_counter()
        value = reader.get(key)
        elapsed = (time.perf_counter() - start) * 1000  # ms
        latencies.append(elapsed)
    
    print_stats("Read Latency", latencies)
    print(f"  Expected: O(log n) binary search + O(sparsity) scan")
    print(f"  Sparsity index: {sparsity}")
    
    RESULTS["sstable_read_avg"] = format_latency(statistics.mean(latencies))
    
    cleanup()


def bench_bloom_filter():
    """Benchmark Bloom filter effectiveness"""
    print("\n" + "=" * 70)
    print("Bloom Filter Effectiveness")
    print("=" * 70)
    
    capacity = 10_000
    error_rate = 0.01
    bloom = BloomFilter(capacity=capacity, error_rate=error_rate)
    
    # Add keys
    for i in range(capacity):
        bloom.add(f"key_{i:08d}")
    
    # Test true negatives (keys not in filter)
    true_negatives = 0
    test_size = 10_000
    
    start = time.perf_counter()
    for i in range(capacity, capacity + test_size):
        if bloom.not_contains(f"key_{i:08d}"):
            true_negatives += 1
    elapsed = time.perf_counter() - start
    
    false_positive_rate = 1 - (true_negatives / test_size)
    
    print(f"  Capacity: {capacity:,}")
    print(f"  Expected error rate: {error_rate * 100:.1f}%")
    print(f"  Actual false positive rate: {false_positive_rate * 100:.2f}%")
    print(f"  True negatives: {true_negatives:,} / {test_size:,}")
    print(f"  Lookup time: {elapsed:.4f}s ({format_throughput(test_size, elapsed)})")
    print(f"  Expected: False positive rate should be ~{error_rate * 100:.1f}%")
    
    RESULTS["bloom_false_positive"] = f"{false_positive_rate * 100:.2f}%"


def bench_sparse_index_sparsity():
    """Compare different sparse index sparsity values"""
    print("\n" + "=" * 70)
    print("Sparse Index Sparsity Comparison")
    print("=" * 70)
    
    cleanup()
    
    size = 50_000
    data = [(f"key_{i:08d}", f"value_{i}") for i in range(size)]
    
    for sparsity in [5, 10, 20, 50]:
        data_file = f"{TEST_SST_DIR}/test_{sparsity}.sst"
        index_file = f"{TEST_SST_DIR}/test_{sparsity}.index"
        Path(TEST_SST_DIR).mkdir(exist_ok=True)
        
        # Write with different sparsity
        writer = SSTableWriter(data_file, index_file, sparsity_index=sparsity)
        writer.write(data)
        
        # Measure index file size
        index_size = Path(index_file).stat().st_size
        
        # Measure read performance
        reader = SSTableReader(data_file, index_file)
        latencies = []
        
        for i in range(0, size, size // 100):  # 100 reads
            key = f"key_{i:08d}"
            start = time.perf_counter()
            value = reader.get(key)
            elapsed = (time.perf_counter() - start) * 1000
            latencies.append(elapsed)
        
        avg_latency = statistics.mean(latencies)
        
        print(f"  Sparsity {sparsity:2d}: Index size: {index_size:,} bytes, "
              f"Avg read: {format_latency(avg_latency)}")
        
        if sparsity == 5:
            RESULTS["sparse_index_best"] = format_latency(avg_latency)
    
    cleanup()
    print("  Expected: Lower sparsity = larger index, faster reads")


# ============================================================================
# LSMEngine Integration Tests
# ============================================================================

def bench_lsm_mixed_workload():
    """Benchmark mixed read/write workload (70% reads, 30% writes)"""
    print("\n" + "=" * 70)
    print("LSMEngine Mixed Workload (70% reads, 30% writes)")
    print("=" * 70)
    
    cleanup()
    
    engine = LSMEngine(db_folder=TEST_SST_DIR, capacity=1000)
    
    # Pre-populate with data
    print("  Populating database...")
    for i in range(5000):
        engine.set(f"key_{i:08d}", f"value_{i}")
    
    # Mixed workload
    num_ops = 10_000
    read_count = 0
    write_count = 0
    
    print(f"  Running {num_ops:,} operations...")
    start = time.perf_counter()
    
    for i in range(num_ops):
        if i % 10 < 7:  # 70% reads
            key = f"key_{(i * 7919) % 5000:08d}"
            value = engine.get(key)
            read_count += 1
        else:  # 30% writes
            engine.set(f"key_{5000 + i:08d}", f"value_{i}")
            write_count += 1
    
    elapsed = time.perf_counter() - start
    
    overall_throughput = format_throughput(num_ops, elapsed)
    
    print(f"\n  Total time: {elapsed:.4f}s")
    print(f"  Reads:  {read_count:,} ({format_throughput(read_count, elapsed)})")
    print(f"  Writes: {write_count:,} ({format_throughput(write_count, elapsed)})")
    print(f"  Overall: {overall_throughput}")
    
    RESULTS["lsm_mixed_overall"] = overall_throughput
    
    engine.wal.close()
    cleanup()


def bench_lsm_flush_overhead():
    """Measure flush overhead and latency spike"""
    print("\n" + "=" * 70)
    print("LSMEngine Flush Overhead")
    print("=" * 70)
    
    cleanup()
    
    capacity = 1000
    engine = LSMEngine(db_folder=TEST_SST_DIR, capacity=capacity)
    
    write_latencies = []
    
    # Write until flush
    for i in range(capacity + 100):
        start = time.perf_counter()
        engine.set(f"key_{i:08d}", f"value_{i}")
        elapsed = (time.perf_counter() - start) * 1000  # ms
        write_latencies.append(elapsed)
    
    # Analyze latency spike at flush
    max_latency = max(write_latencies)
    avg_latency = statistics.mean(write_latencies)
    spike_multiplier = max_latency / avg_latency
    
    print(f"  Average write latency: {format_latency(avg_latency)}")
    print(f"  Maximum write latency: {format_latency(max_latency)} (during flush)")
    print(f"  Latency spike: {spike_multiplier:.1f}x")
    
    RESULTS["lsm_flush_spike"] = f"{spike_multiplier:.1f}x"
    
    # Find flush points (latency spikes)
    flush_threshold = avg_latency * 5
    flush_points = [i for i, lat in enumerate(write_latencies) if lat > flush_threshold]
    print(f"  Detected flushes at writes: {flush_points}")
    
    engine.wal.close()
    cleanup()


def bench_lsm_multi_sstable_reads():
    """Measure read performance degradation with multiple SSTables"""
    print("\n" + "=" * 70)
    print("Multi-SSTable Read Performance")
    print("=" * 70)
    
    cleanup()
    
    capacity = 500
    engine = LSMEngine(db_folder=TEST_SST_DIR, capacity=capacity)
    
    # Create multiple SSTables
    num_sstables = 10
    keys_per_sstable = capacity
    
    print(f"  Creating {num_sstables} SSTables...")
    for batch in range(num_sstables):
        for i in range(keys_per_sstable):
            key = f"batch_{batch:02d}_key_{i:08d}"
            engine.set(key, f"value_{i}")
    
    # Measure read latency for keys in different SSTables
    print(f"  Measuring read latency across {num_sstables} SSTables...")
    
    for sstable_idx in [0, num_sstables // 2, num_sstables - 1]:
        key = f"batch_{sstable_idx:02d}_key_{0:08d}"
        
        start = time.perf_counter()
        value = engine.get(key)
        elapsed = (time.perf_counter() - start) * 1000
        
        print(f"  SSTable {sstable_idx} (age: {num_sstables - sstable_idx}): "
              f"{format_latency(elapsed)}")
        
        if sstable_idx == 0:
            RESULTS["lsm_multi_sstable_oldest"] = format_latency(elapsed)
    
    print("  Note: Older SSTables require more lookups (read amplification)")
    
    engine.wal.close()
    cleanup()


# ============================================================================
# Main Test Runner
# ============================================================================

def main():
    """Run all performance benchmarks"""
    print("\n" + "=" * 70)
    print("py-lsm Performance Test Suite")
    print("=" * 70)
    print("\nBenchmarking all LSM-tree components:")
    print("  - MemTable (in-memory operations)")
    print("  - WAL (write-ahead log durability)")
    print("  - SSTable (disk-based sorted storage)")
    print("  - LSMEngine (end-to-end integration)")
    print("=" * 70)
    
    try:
        # MemTable benchmarks
        bench_memtable_writes()
        bench_memtable_reads()
        bench_memtable_sorting()
        bench_memtable_memory()
        
        # WAL benchmarks
        bench_wal_append()
        bench_wal_replay()
        
        # SSTable benchmarks
        bench_sstable_write()
        bench_sstable_read()
        bench_bloom_filter()
        bench_sparse_index_sparsity()
        
        # LSMEngine integration benchmarks
        bench_lsm_mixed_workload()
        bench_lsm_flush_overhead()
        bench_lsm_multi_sstable_reads()
        
        # Print summary table
        print_summary_table()
        
        print("\n" + "=" * 70)
        print("All Performance Benchmarks Completed Successfully")
        print("=" * 70)
        print("\nKey Takeaways:")
        print("  - MemTable: O(1) reads/writes, O(n log n) sorting")
        print("  - WAL: fsync overhead dominates, O(n) replay")
        print("  - SSTable: O(log n) reads with sparse index")
        print("  - Bloom filter: ~1% false positive rate")
        print("  - LSMEngine: Read amplification increases with SSTable count")
        print("\nTuning Recommendations:")
        print("  - Increase capacity to reduce flush frequency")
        print("  - Lower sparsity for faster reads (larger index)")
        print("  - Implement compaction to merge SSTables")
        print("=" * 70 + "\n")
        
    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        import traceback
        traceback.print_exc()
        cleanup()
        sys.exit(1)
    finally:
        cleanup()


if __name__ == "__main__":
    main()

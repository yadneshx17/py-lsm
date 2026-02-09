"""
Quick integration test for py-lsm
Tests: WAL, MemTable, SSTable, and LSMEngine
"""
import sys
import shutil
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from wal import WAL
from memtable import MemTable
from lsm_engine import LSMEngine

# Test directories (will be cleaned up)
TEST_WAL_DIR = "test_wal_dir"
TEST_SST_DIR = "test_sst_db"


def cleanup():
    """Remove test directories"""
    for d in [TEST_WAL_DIR, TEST_SST_DIR]:
        if Path(d).exists():
            shutil.rmtree(d)


def test_wal():
    """Test WAL append and replay"""
    print("\n=== Testing WAL ===")
    cleanup()
    
    # Write to WAL
    wal = WAL(TEST_WAL_DIR)
    wal.append("name", "yadnesh")
    wal.append("age", "25")
    wal.append("city", "mumbai")
    wal.close()
    
    # Replay WAL
    wal2 = WAL(TEST_WAL_DIR)
    entries = wal2.replay()
    wal2.close()
    
    print(f"  Entries recovered: {entries}")
    assert len(entries) == 3, f"Expected 3 entries, got {len(entries)}"
    assert entries[0] == ("name", "yadnesh"), f"Entry mismatch: {entries[0]}"
    assert entries[1] == ("age", "25"), f"Entry mismatch: {entries[1]}"
    assert entries[2] == ("city", "mumbai"), f"Entry mismatch: {entries[2]}"
    
    print("  ✅ WAL test passed!")
    cleanup()


def test_memtable():
    """Test MemTable operations"""
    print("\n=== Testing MemTable ===")
    
    mt = MemTable()
    mt.set("b", "banana")
    mt.set("a", "apple")
    mt.set("c", "cherry")
    
    # Test get
    assert mt.get("a") == "apple", "Get failed"
    assert mt.get("b") == "banana", "Get failed"
    assert mt.get("z") is None, "Non-existent key should return None"
    
    # Test sorted items
    items = mt.get_sorted_items()
    print(f"  Sorted items: {items}")
    assert items[0][0] == "a", "Should be sorted by key"
    
    print("  ✅ MemTable test passed!")


def test_lsm_engine():
    """Test full LSMEngine flow"""
    print("\n=== Testing LSMEngine ===")
    cleanup()
    
    # Create engine with small capacity for testing flush
    engine = LSMEngine(db_folder=TEST_SST_DIR, capacity=3)
    
    # Insert some data
    engine.set("user:1", "alice")
    engine.set("user:2", "bob")
    print("  Inserted 2 keys (should be in memtable)")
    
    # Get from memtable
    val = engine.get("user:1")
    print(f"  Get user:1 = {val}")
    assert val == "alice", f"Expected 'alice', got {val}"
    
    # Trigger flush by adding more keys
    engine.set("user:3", "charlie")  # This should trigger flush (capacity=3)
    print("  Inserted user:3 (should trigger flush)")
    
    # Add more after flush
    engine.set("user:4", "diana")
    
    # Get from SSTable
    val = engine.get("user:2")
    print(f"  Get user:2 (from SSTable) = {val}")
    
    # Get from memtable (new entry)
    val = engine.get("user:4")
    print(f"  Get user:4 (from memtable) = {val}")
    assert val == "diana", f"Expected 'diana', got {val}"
    
    print("  ✅ LSMEngine test passed!")
    cleanup()


def test_wal_recovery():
    """Test WAL recovery simulation"""
    print("\n=== Testing WAL Recovery ===")
    cleanup()
    
    # Simulate: Write data but don't flush (crash scenario)
    engine1 = LSMEngine(db_folder=TEST_SST_DIR, capacity=100)  # High capacity = no auto flush
    engine1.set("crash:1", "value1")
    engine1.set("crash:2", "value2")
    # DON'T call flush - simulating crash
    engine1.wal.close()
    
    print("  Simulated crash after 2 writes (no flush)")
    
    # New engine should recover from WAL
    engine2 = LSMEngine(db_folder=TEST_SST_DIR, capacity=100)
    
    val1 = engine2.get("crash:1")
    val2 = engine2.get("crash:2")
    
    print(f"  Recovered: crash:1 = {val1}, crash:2 = {val2}")
    assert val1 == "value1", f"WAL recovery failed for crash:1"
    assert val2 == "value2", f"WAL recovery failed for crash:2"
    
    print("  ✅ WAL Recovery test passed!")
    cleanup()


if __name__ == "__main__":
    print("=" * 50)
    print("py-lsm Quick Test Suite")
    print("=" * 50)
    
    try:
        test_wal()
        test_memtable()
        test_lsm_engine()
        test_wal_recovery()
        
        print("\n" + "=" * 50)
        print("🎉 All tests passed!")
        print("=" * 50)
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        cleanup()
        sys.exit(1)

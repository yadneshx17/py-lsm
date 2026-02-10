"""
Comprehensive tests for flush and crash recovery scenarios.

Tests:
1. Flush properly clears memtable and WAL
2. After flush, new data can be written to fresh memtable and WAL
3. WAL and memtable coordinate properly after crash recovery
4. Multiple crash/recovery cycles work correctly
"""
import sys
import shutil
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from wal import WAL
from memtable import MemTable
from lsm_engine import LSMEngine

# Test directories
TEST_DB_DIR = "test_flush_recovery_db"
TEST_WAL_DIR = "test_flush_recovery_wal"


def cleanup():
    """Remove test directories"""
    for d in [TEST_DB_DIR, TEST_WAL_DIR, "wal_dir"]:
        if Path(d).exists():
            shutil.rmtree(d)


def test_flush_clears_memtable_and_wal():
    """
    Test 1: Verify that flush properly clears both memtable and WAL
    """
    print("\n" + "=" * 60)
    print("TEST 1: Flush Clears Memtable and WAL")
    print("=" * 60)
    
    cleanup()
    
    # Create engine with capacity that won't auto-flush
    engine = LSMEngine(db_folder=TEST_DB_DIR, capacity=100)
    
    # Add some data
    print("\n📝 Adding data to memtable...")
    engine.set("key1", "value1")
    engine.set("key2", "value2")
    engine.set("key3", "value3")
    
    # Verify data is in memtable
    print(f"   Memtable size before flush: {len(engine.memtable)}")
    assert len(engine.memtable) == 3, "Memtable should have 3 entries"
    
    # Check WAL file size (should have data)
    wal_file = Path("wal_dir/wal.bin")
    wal_size_before = wal_file.stat().st_size if wal_file.exists() else 0
    print(f"   WAL file size before flush: {wal_size_before} bytes")
    assert wal_size_before > 0, "WAL should contain data"
    
    # Manually trigger flush
    print("\n🔄 Flushing memtable to SSTable...")
    engine.flush()
    
    # Verify memtable is cleared
    memtable_size_after = len(engine.memtable)
    print(f"   Memtable size after flush: {memtable_size_after}")
    assert memtable_size_after == 0, "Memtable should be empty after flush"
    
    # Verify WAL is cleared
    wal_size_after = wal_file.stat().st_size if wal_file.exists() else 0
    print(f"   WAL file size after flush: {wal_size_after} bytes")
    assert wal_size_after == 0, "WAL should be empty after flush"
    
    # Verify data is still accessible (from SSTable)
    print("\n🔍 Verifying data is accessible from SSTable...")
    val1 = engine.get("key1")
    val2 = engine.get("key2")
    val3 = engine.get("key3")
    
    assert val1 == "value1", f"Expected 'value1', got {val1}"
    assert val2 == "value2", f"Expected 'value2', got {val2}"
    assert val3 == "value3", f"Expected 'value3', got {val3}"
    
    print("\n✅ TEST 1 PASSED: Flush properly clears memtable and WAL")
    cleanup()


def test_fresh_data_after_flush():
    """
    Test 2: Verify that after flush, memtable and WAL can accept fresh data
    """
    print("\n" + "=" * 60)
    print("TEST 2: Fresh Data After Flush")
    print("=" * 60)
    
    cleanup()
    
    engine = LSMEngine(db_folder=TEST_DB_DIR, capacity=100)
    
    # First batch of data
    print("\n📝 Adding first batch of data...")
    engine.set("batch1_key1", "batch1_value1")
    engine.set("batch1_key2", "batch1_value2")
    
    print(f"   Memtable size: {len(engine.memtable)}")
    assert len(engine.memtable) == 2, "Should have 2 entries"
    
    # Flush
    print("\n🔄 Flushing first batch...")
    engine.flush()
    assert len(engine.memtable) == 0, "Memtable should be empty"
    
    # Add second batch of data (fresh data)
    print("\n📝 Adding second batch of fresh data...")
    engine.set("batch2_key1", "batch2_value1")
    engine.set("batch2_key2", "batch2_value2")
    engine.set("batch2_key3", "batch2_value3")
    
    memtable_size = len(engine.memtable)
    print(f"   Memtable size after fresh writes: {memtable_size}")
    assert memtable_size == 3, "Memtable should have 3 new entries"
    
    # Verify WAL has fresh data
    wal_file = Path("wal_dir/wal.bin")
    wal_size = wal_file.stat().st_size if wal_file.exists() else 0
    print(f"   WAL file size: {wal_size} bytes")
    assert wal_size > 0, "WAL should contain fresh data"
    
    # Verify all data is accessible
    print("\n🔍 Verifying all data is accessible...")
    
    # Old data from SSTable
    assert engine.get("batch1_key1") == "batch1_value1"
    assert engine.get("batch1_key2") == "batch1_value2"
    
    # New data from memtable
    assert engine.get("batch2_key1") == "batch2_value1"
    assert engine.get("batch2_key2") == "batch2_value2"
    assert engine.get("batch2_key3") == "batch2_value3"
    
    print("\n✅ TEST 2 PASSED: Fresh data works correctly after flush")
    cleanup()


def test_crash_recovery_coordination():
    """
    Test 3: Verify WAL and memtable coordinate properly after crash
    """
    print("\n" + "=" * 60)
    print("TEST 3: Crash Recovery Coordination")
    print("=" * 60)
    
    cleanup()
    
    # Phase 1: Write data and simulate crash (no flush)
    print("\n📝 Phase 1: Writing data before crash...")
    engine1 = LSMEngine(db_folder=TEST_DB_DIR, capacity=100)
    engine1.set("crash_key1", "crash_value1")
    engine1.set("crash_key2", "crash_value2")
    engine1.set("crash_key3", "crash_value3")
    
    print(f"   Memtable size: {len(engine1.memtable)}")
    print(f"   Data written to WAL")
    
    # Close WAL without flushing (simulating crash)
    engine1.wal.close()
    print("\n💥 Simulating crash (no flush)...")
    del engine1  # Simulate crash
    
    # Phase 2: Recovery - create new engine instance
    print("\n🔄 Phase 2: Recovery - creating new engine instance...")
    engine2 = LSMEngine(db_folder=TEST_DB_DIR, capacity=100)
    
    # Verify data was recovered from WAL to memtable
    memtable_size = len(engine2.memtable)
    print(f"   Memtable size after recovery: {memtable_size}")
    assert memtable_size == 3, f"Expected 3 entries in memtable, got {memtable_size}"
    
    # Verify all data is accessible
    print("\n🔍 Verifying recovered data...")
    val1 = engine2.get("crash_key1")
    val2 = engine2.get("crash_key2")
    val3 = engine2.get("crash_key3")
    
    print(f"   crash_key1 = {val1}")
    print(f"   crash_key2 = {val2}")
    print(f"   crash_key3 = {val3}")
    
    assert val1 == "crash_value1", f"Expected 'crash_value1', got {val1}"
    assert val2 == "crash_value2", f"Expected 'crash_value2', got {val2}"
    assert val3 == "crash_value3", f"Expected 'crash_value3', got {val3}"
    
    # Phase 3: Add new data after recovery
    print("\n📝 Phase 3: Adding new data after recovery...")
    engine2.set("post_recovery_key", "post_recovery_value")
    
    assert len(engine2.memtable) == 4, "Should have 4 entries (3 recovered + 1 new)"
    assert engine2.get("post_recovery_key") == "post_recovery_value"
    
    print("\n✅ TEST 3 PASSED: WAL and memtable coordinate properly after crash")
    cleanup()


def test_multiple_crash_recovery_cycles():
    """
    Test 4: Verify multiple crash/recovery cycles work correctly
    """
    print("\n" + "=" * 60)
    print("TEST 4: Multiple Crash/Recovery Cycles")
    print("=" * 60)
    
    cleanup()
    
    # Cycle 1: Write and crash
    print("\n🔄 Cycle 1: Write and crash...")
    engine1 = LSMEngine(db_folder=TEST_DB_DIR, capacity=100)
    engine1.set("cycle1_key", "cycle1_value")
    engine1.wal.close()
    del engine1
    
    # Cycle 2: Recover, write more, and crash
    print("\n🔄 Cycle 2: Recover, write more, and crash...")
    engine2 = LSMEngine(db_folder=TEST_DB_DIR, capacity=100)
    assert engine2.get("cycle1_key") == "cycle1_value", "Cycle 1 data should be recovered"
    engine2.set("cycle2_key", "cycle2_value")
    engine2.wal.close()
    del engine2
    
    # Cycle 3: Recover, write more, flush, write again, and crash
    print("\n🔄 Cycle 3: Recover, flush, write, and crash...")
    engine3 = LSMEngine(db_folder=TEST_DB_DIR, capacity=100)
    assert engine3.get("cycle1_key") == "cycle1_value", "Cycle 1 data should be recovered"
    assert engine3.get("cycle2_key") == "cycle2_value", "Cycle 2 data should be recovered"
    
    # Flush to SSTable
    engine3.flush()
    
    # Write new data after flush
    engine3.set("cycle3_key", "cycle3_value")
    engine3.wal.close()
    del engine3
    
    # Cycle 4: Final recovery and verification
    print("\n🔄 Cycle 4: Final recovery and verification...")
    engine4 = LSMEngine(db_folder=TEST_DB_DIR, capacity=100)
    
    # Verify all data
    print("\n🔍 Verifying all data from multiple cycles...")
    val1 = engine4.get("cycle1_key")
    val2 = engine4.get("cycle2_key")
    val3 = engine4.get("cycle3_key")
    
    print(f"   cycle1_key = {val1} (should be from SSTable)")
    print(f"   cycle2_key = {val2} (should be from SSTable)")
    print(f"   cycle3_key = {val3} (should be from memtable via WAL recovery)")
    
    assert val1 == "cycle1_value", f"Expected 'cycle1_value', got {val1}"
    assert val2 == "cycle2_value", f"Expected 'cycle2_value', got {val2}"
    assert val3 == "cycle3_value", f"Expected 'cycle3_value', got {val3}"
    
    print("\n✅ TEST 4 PASSED: Multiple crash/recovery cycles work correctly")
    cleanup()


def test_flush_after_recovery():
    """
    Test 5: Verify flush works correctly after crash recovery
    """
    print("\n" + "=" * 60)
    print("TEST 5: Flush After Recovery")
    print("=" * 60)
    
    cleanup()
    
    # Write data and crash
    print("\n📝 Writing data before crash...")
    engine1 = LSMEngine(db_folder=TEST_DB_DIR, capacity=100)
    engine1.set("key1", "value1")
    engine1.set("key2", "value2")
    engine1.wal.close()
    del engine1
    
    # Recover
    print("\n🔄 Recovering from crash...")
    engine2 = LSMEngine(db_folder=TEST_DB_DIR, capacity=100)
    assert len(engine2.memtable) == 2, "Should recover 2 entries"
    
    # Add more data
    print("\n📝 Adding more data after recovery...")
    engine2.set("key3", "value3")
    engine2.set("key4", "value4")
    assert len(engine2.memtable) == 4, "Should have 4 entries total"
    
    # Flush
    print("\n🔄 Flushing after recovery...")
    engine2.flush()
    
    # Verify flush worked
    assert len(engine2.memtable) == 0, "Memtable should be empty after flush"
    
    wal_file = Path("wal_dir/wal.bin")
    wal_size = wal_file.stat().st_size if wal_file.exists() else 0
    assert wal_size == 0, "WAL should be empty after flush"
    
    # Verify all data is accessible from SSTable
    print("\n🔍 Verifying all data is accessible from SSTable...")
    assert engine2.get("key1") == "value1"
    assert engine2.get("key2") == "value2"
    assert engine2.get("key3") == "value3"
    assert engine2.get("key4") == "value4"
    
    print("\n✅ TEST 5 PASSED: Flush works correctly after recovery")
    cleanup()


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("FLUSH AND CRASH RECOVERY TEST SUITE")
    print("=" * 60)
    
    try:
        test_flush_clears_memtable_and_wal()
        test_fresh_data_after_flush()
        test_crash_recovery_coordination()
        test_multiple_crash_recovery_cycles()
        test_flush_after_recovery()
        
        print("\n" + "=" * 60)
        print("🎉 ALL TESTS PASSED!")
        print("=" * 60)
        print("\nSummary:")
        print("  ✅ Flush properly clears memtable and WAL")
        print("  ✅ Fresh data works correctly after flush")
        print("  ✅ WAL and memtable coordinate properly after crash")
        print("  ✅ Multiple crash/recovery cycles work correctly")
        print("  ✅ Flush works correctly after recovery")
        print("=" * 60 + "\n")
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        import traceback
        traceback.print_exc()
        cleanup()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        cleanup()
        sys.exit(1)

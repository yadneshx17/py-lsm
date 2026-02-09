"""
SSTable Writer/Reader Test
Tests file creation and reading
"""
import os
import sys
import shutil
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sstable.writer import SSTableWriter
from sstable.reader import SSTableReader

TEST_DIR = "test_sstable_dir"


def cleanup():
    if Path(TEST_DIR).exists():
        shutil.rmtree(TEST_DIR)


def test_sstable():
    """Test SSTable write and read operations"""
    print("=" * 50)
    print("SSTable File Test")
    print("=" * 50)
    
    cleanup()
    Path(TEST_DIR).mkdir(parents=True, exist_ok=True)
    
    data_file = os.path.join(TEST_DIR, "test.sst")
    index_file = os.path.join(TEST_DIR, "test.index")
    
    # Create test data (must be sorted for SSTable)
    test_data = [
        ("apple", "red fruit"),
        ("banana", "yellow fruit"),
        ("cherry", "small red fruit"),
        ("date", "sweet brown fruit"),
        ("elderberry", "dark purple berry"),
        ("fig", "soft sweet fruit"),
        ("grape", "small round fruit"),
        ("honeydew", "green melon"),
    ]
    
    # Write SSTable
    print("\n📝 Writing SSTable...")
    writer = SSTableWriter(data_file, index_file, sparsity_index=3)
    writer.write(test_data)
    
    # Show created files
    print(f"\n📁 Files created:")
    for f in Path(TEST_DIR).iterdir():
        size = f.stat().st_size
        print(f"   {f.name}: {size} bytes")
    
    # Show data file contents
    print(f"\n📄 Data file ({data_file}) contents:")
    with open(data_file, "r") as f:
        for i, line in enumerate(f):
            print(f"   {i+1}: {line.rstrip()}")
    
    # Show index file contents (before bloom filter)
    print(f"\n📑 Index file ({index_file}) sparse index:")
    with open(index_file, "rb") as f:
        for line in f:
            if line == b"__BLOOM_START__\n":
                print("   --- Bloom Filter Data Starts ---")
                break
            key, offset = line.decode("utf-8").rstrip("\n").split("\t", 1)
            print(f"   Key: '{key}' @ offset {offset}")
    
    # Read from SSTable
    print("\n🔍 Testing SSTableReader...")
    reader = SSTableReader(data_file, index_file)
    
    # Test lookups
    test_keys = ["apple", "cherry", "grape", "notfound", "banana"]
    for key in test_keys:
        value = reader.get(key)
        status = "✅" if value else "❌"
        print(f"   {status} get('{key}') = {value}")
    
    print("\n" + "=" * 50)
    print("🎉 SSTable test complete!")
    print("=" * 50)
    
    # Optionally cleanup
    # cleanup()


if __name__ == "__main__":
    test_sstable()

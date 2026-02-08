# Orchestrates

import os
import time
from pathlib import Path

from sortedcontainers import SortedDict

from sstable.writer import SSTableWriter

from .wal import WAL
from .memtable import MemTable
from .sstable import SSTableReader


class LSMEngine:
    """A simple implementation of Log-Structured Merge-Tree (LSM-Tree)"""

    def __init__(self, db_folder="sst_files_db", capacity=50, sparsity_index=10):
        self.db_folder = Path(db_folder)  # convert to Path object
        self.capacity = capacity
        self.sparsity_index = sparsity_index
        self.memtable = MemTable()
        self.index_cache = SortedDict()

        self.wal = WAL()
        self._recover_from_wal()

        # Create directory if it doesn't exist
        self.db_folder.mkdir(parents=True, exist_ok=True)

        # checks for .sst files and the existence of .index simultaneously
        for sst_path in self.db_folder.glob("*.sst"):
            index_path = sst_path.with_suffix(".index")

            if index_path.exists():
                # Store as string if SSTableReader expects string, else keep as Path
                self.index_cache[str(sst_path)] = SSTableReader(
                    str(sst_path), str(index_path)
                )

    def set(self, key, value):
        """Sets a key-value pair in the memtable with WAL for durability or flushes if full."""
        # write to wal first
        self.wal.append(key, value)
        
        # then write to memtable
        self.memtable.set(key, value)

        # flush if capacity is reached
        if len(self.memtable) >= self.capacity:
            self.flush()

    def get(self, key):

        # 1. Check memtable ( fastest )
        value = self.memtable.get(key)
        if value is not None:
            print(f"Found in memtable: {key} -> {value}")
            return value

        # 2. Search SSTables from newest to oldest
        print(f"Searching into SSTables for key: {key}")
        for sst_file in reversed(self.index_cache.keys()):
            reader = self.index_cache.get(sst_file)
            if reader:              
                value = reader.get(key)
            if value is not None:
                print(f"Found in SSTable {sst_file}: {key} -> {value}")
                return value
        print(f"Key {key} not found in memtable or SSTables.")
        return None

    def flush(self):
        """Flushes the memtable to SSTable on disk"""

        if not self.memtable:
            return

        timestamp = int(time.time() * 1000000)  # seconds * 1 million = microseconds
        data_filename = os.path.join(self.db_folder, f"{timestamp}.sst")
        index_filename = os.path.join(self.db_folder, f"{timestamp}.index")

        writer = SSTableWriter(data_filename, index_filename, self.sparsity_index)
        writer.write(self.memtable.get_sorted_items())

        print(f"Flushed memtable to {data_filename} and {index_filename}")
        self.memtable.clear()
        self.wal.clear()
        print("WAL cleared after flush")
        self.index_cache[data_filename] = SSTableReader(data_filename, index_filename)

    def _recover_from_wal(self):
        """Replay WAL to recover/restore/rebuild/repopulate/recreate ( lol ) memtable after crash"""
        entries = self.wal.replay()

        for key, value in entries:
            if value is not None:
                self.memtable.set(key, value)
            #  else: handle deletes ( tombstones )
        
        if entries:
            print(f"Recovered {len(entries)} entries from WAL")
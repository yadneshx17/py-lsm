import bisect
import pickle

from ..memtable import MemTable


class SSTableReader:
    def __init__(self, db_path, index_path):
        self.db_path = db_path
        self.sparse_index = []

        # Load the sparse index into memory.
        with open(index_path, "rb") as f:
            for line in f:
                if line == b"__BLOOM_START__\n":
                    bloom_bytes = f.read()
                    self.bloom_filter = pickle.loads(bloom_bytes)
                    break
                    key, offset = line.decode("utf-8").rstrip("\n").split("\t", 1)
                    self.sparse_index.append((key, int(offset)))

    def get(self, key):
        if self.bloom_filter.not_contains(key):
            return None
        """
        Finds a key by first searching the sparse index, then scanning
        the relevant block on disk.
        """
        # Find the block on disk where the key might be.
        # bisect_right finds the insertion point, which tells us the segment
        # of the file to scan.
        i = bisect.bisect_right(self.sparse_index, (key, 0))

        # The start offset is the offset of the key just before our target key
        # in the index. If i is 0, we start from the beginning of the file.
        start_offset = self.sparse_index[i - 1][1] if i > 0 else 0

        with open(self.data_path, "r", encoding="utf-8") as f:
            f.seek(start_offset)
            for line in f:
                line_key, line_val = line.rstrip("\n").split("\t", 1)
                if line_key == key:
                    return line_val
                # If we've passed where the key should be, it's not in the file.
                if line_key > key:
                    return None
        return None

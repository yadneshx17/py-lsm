import pickle

from ..bloom_filter import BloomFilter


class SSTableWriter:
    BLOOM_MARKER = b"__BLOOM_START__\n"
    """
    Writes sorted key-value pairs to an SSTable data file and creates a
    corresponding sparse index file.
    """

    def __init__(self, data_path, index_path, sparsity_index=10):
        self.data_path = data_path
        self.index_path = index_path

        # the index will store every Nth key
        self.sparsity_index = sparsity_index
        self.bloon_error_rate = 0.01  # 1%

    def write(self, sorted_kv_pairs):
        kv_list = list(sorted_kv_pairs)
        capacity = len(kv_list)

        # Create Bloom filter with capacity and error rate
        bloom = BloomFilter(capacity=capacity, error_rate=self.bloon_error_rate)

        """
        Writes the key-value pairs to the data file and generates the
        sparse index.
        """
        
        with (
            open(self.data_path, "w", encoding="utf-8") as data_f,
            open(self.index_path, "wb") as index_f,
        ):
            offset = 0
            for i, (key, value) in enumerate(sorted_kv_pairs):
                bloom.add(key)
                # Every Nth item, write the key and current offset to the index.
                if i % self.sparsity_index == 0:
                    index_f.write(f"{key}\t{offset}\n".encode("utf-8"))

                line = f"{key}\t{value}\n"
                data_f.write(line)
                offset += len(line.encode("utf-8"))

            # Write Bloom filter marker
            index_f.write(self.BLOOM_MARKER)

            # Serialize bloom filter
            bloom_bytes = pickle.dumps(bloom)  # converts object into bytes
            index_f.write(bloom_bytes)

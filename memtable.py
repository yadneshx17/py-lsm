class MemTable:
    """
    An in-memory data structure using a dict for O(1) writes and reads.
    Sorting happens at flush time O(n log n).

    Another Approach:
        bisect + list for O(n) writes and O(log n) for reads,
        O(1) for flush.
    """

    def __init__(self):
        self._data = {}
        # self._data = []

    def set(self, key, value):
        """Adds a key-value pair. O(1) avg."""
        self._data[key] = value

    def get(self, key):
        """Returns a value for a given key. O(1)"""
        return self._data.get(key)

    def clear(self):
        """Clears the Memtable"""
        self.data = {}

    def get_sorted_items(self):
        """Returns all the k-v pairs sorted by key. O(n log n)."""
        return sorted(self._data.items())

    def __len__(self):
        """Returns the number of items in the memtable."""
        return len(self._data)

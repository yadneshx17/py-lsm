import os
import struct
import zlib

from pathlib import Path

class WAL:
    """
    Write-Ahead Log for durability.
    
    Record Format (binary):
    ┌──────────────┬──────────────┬──────────────────┬──────────┬───────────┬───────────┐
    │ key_len (4B) │ val_len (4B) │ checksum_len(4B) │  key     │  value    │ checksum  │
    └──────────────┴──────────────┴──────────────────┴──────────┴───────────┴───────────┘
    
    Operations:
    - SET: value_len > 0
    - DELETE: value_len = 0 (tombstone)
    """

    HEADER_FORMAT = ">III" 
    # 8 bytes
    # Returns the size in bytes of the struct defined by the format string "HEADER_FORMAT"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT) 

    def __init__(self, wal_path="wal_dir"):
        self.wal_path = Path(wal_path)

        self.wal_path.mkdir(parents=True, exist_ok=True)

        self.wal_file = self.wal_path / "wal.bin"

        # no python level buffering, write() directly goes into OS
        self._file = open(self.wal_file, 'ab+', buffering=0) # Disables buffering ( only in binary mode )

    def append(self, key: str, value:str) -> None:
        """Appends a write operation to the WAL"""
        key_bytes = key.encode("utf-8")
        value_bytes = value.encode("utf-8")

        # calculate checksum
        checksum = self._calculate_checksum(key_bytes, value_bytes) # int
        checksum_bytes = str(checksum).encode("utf-8") # str -> bytes

        # pack header: key_len, value_len
        header = struct.pack(self.HEADER_FORMAT, len(key_bytes), len(value_bytes), len(checksum_bytes))
    
        # write header + key + value + checksum
        record = header + key_bytes + value_bytes + checksum_bytes
        self._file.write(record)

        # flush to ensure data is written to disk
        self._file.flush()      # python -> os
        os.fsync(self._file.fileno()) #  force OS -> disk hardware

    def replay(self):
        """Replays the WAL to apply write operations to the Memtable"""

        entries = []
        
        with open(self.wal_file, "rb") as f:
            while True:
                # Read Header
                header_data = f.read(self.HEADER_SIZE)
                
                if not header_data or len(header_data) < self.HEADER_SIZE:
                    break

                # Unpack Header
                key_len, val_len, checksum_len = struct.unpack(self.HEADER_FORMAT, header_data)

                # Read key
                key_bytes = f.read(key_len)
                if len(key_bytes) < key_len:
                    break

                # Read value
                value_bytes = f.read(val_len) if val_len > 0 else b""
                if len(value_bytes) < val_len:
                    break
                
                # Read checksum
                checksum_bytes = f.read(checksum_len)
                checksum_data = int(checksum_bytes.decode("utf-8"))

                if checksum_data != self._calculate_checksum(key_bytes, value_bytes):
                    break

                key = key_bytes.decode("utf-8")
                value = value_bytes.decode("utf-8") if val_len > 0 else None

                # Add to entries
                entries.append((key, value))

        return entries


    def clear(self):
        """Clears the WAL file"""
        self._file.close()
        self._file = open(self.wal_file, "wb", buffering=0)

    def close(self):
        """Closes the WAL file"""
        if self._file:
            self._file.close()

    def _calculate_checksum(self, key: bytes, value: bytes) -> int:
        """Calculates CRC32 checksum of the record"""
        return zlib.crc32(key + value)
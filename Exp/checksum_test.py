import zlib

def _calculate_checksum(key: bytes, value: bytes) -> int:
    """Calculates CRC32 checksum of the record"""
    return zlib.crc32(key + value)

key = "hello".encode("utf-8")
value = "world".encode("utf-8")
checksum = _calculate_checksum(key, value)
print(f"Checksum: {checksum}")
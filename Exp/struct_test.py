import struct

# This is just me playing with new modules i got to know about

# Format string: 'i' for int (4 bytes), 'f' for float (4 bytes), 's' for bytes of a given length
# '<' at the start specifies little-endian byte order (platform-independent standard size)
format_string = "<if10s"
print()
values = (23, 42.0, b"hello")
print(f"Original Values:  {values}")

# Packing the data
packed_data = struct.pack(format_string, *values)
print(f"Packed data: {packed_data}")
print(f"Size in bytes: {struct.calcsize(format_string)}")

# Unpacking the data
unpacked_data = struct.unpack(format_string, packed_data)
print(f"Unpacked data: {unpacked_data}")

**Record Format: WAL**
`key_len (4B) │ val_len (4B) │ checksum_len(4B) │  key     │  value    │ checksum  │`

**Hexdump for two key-value pairs**
key: dev, value: ops
key: key, value: br 

```
00000000  00 00 00 03 00 00 00 03  00 00 00 0a 64 65 76 6f  |............devo|
00000010  70 73 32 35 36 30 31 37  39 39 36 37 00 00 00 03  |ps2560179967....|
00000020  00 00 00 02 00 00 00 0a  6b 65 79 62 72 32 35 33  |........keybr253|
00000030  30 38 30 32 38 34 32                              |0802842|
00000037
```

**BreakDown**

```
0000 0003 → key_len = 3
0000 0003 → val_len = 3
0000 000a → checksum_len = 10
```
So
```
key = 3 bytes
value = 3 bytes
checksum = 10 bytes
```

Next 3 bytes are key
```
64 65 76 → dev
```

Next 3 bytes are value
```
6f 70 73 → ops
```

Next 10 bytes are checksum
```
32 35 36 30 31 37 39 39 36 37 → 2560179967
```

**First Record: `key: dev, value: ops`**
```
key_len = 3
val_len = 3
checksum_len = 10

key = "dev"
value = "ops"
checksum = "2560179967"
```
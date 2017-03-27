Store format spec
=====================

Update Entry
--------------

- **checksum** - long - checksum for update entry. 
    - It uses proprietary [64bit XXHash](https://github.com/jpountz/lz4-java/tree/master/src/java/net/jpountz/xxhash)
- **update size** - int - number of bytes consumed by this entry, includes long
- **key count** - int - number of keys in this Update
- **key size** - int - number of bytes in each key in this Update
- **versionID size** - int - number of bytes used in versionID
- **prevVersionID size** - int - number of bytes used by previous versionID
- **is merged** - boolean - true if this is merged update

- section with keys, its size is **number of keys** * **key size**

- section with value size & offset pair, 
    - section size is **key count** * (4+4)
    - value size is int, -1 size is tombstone (deleted key)
    - value offset is int, 
        - single update can not be larger than 2GB
        - value offset is counted from start of this update, not from start of the file


- **versionID** - byte[] - version of current update

- **prevVersionID** - byte[] - version of previous update

- section with values    


Shard Spec 
---------------
this is stored in Shard Layout Log

- **checksum** - long - checksum for update entry.  
    - It uses proprietary [64bit XXHash](https://github.com/jpountz/lz4-java/tree/master/src/java/net/jpountz/xxhash)
    - Hash is `xxhash - 1000`, to make it different from Update Entry hash
- **update size** - int - number of bytes consumed by this entry, includes long
- **key count** - int - number of keys in this Update
- **key size** - int - number of bytes in each key in this Update
- **versionID size** - int - number of bytes used in versionID
- **versionID data** - byte[] - data from version ID

- followed by table with size 'key count' of 
    - **start key** - byte[]
    - **file number** - long
    - **versionID size** - int - number of bytes used in versionID
    - **versionID** - byte[] - versionID at which this shard was created

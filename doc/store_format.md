Store format spec
=====================

Update Entry
--------------

- **checksum** - long - checksum for update entry
- **update size** - int - number of bytes consumed by this entry, includes long
- **key count** - int - number of keys in this Update
- **key size** - int - number of bytes in each key in this Update
- **versionID size** - int - number of bytes used in versionID
- **prevVersionID size** - int - number of bytes used by previous versionID
- **is merged** - boolean - true if this is merged update

- section with keys, its size is **number of keys** * **key size**

- section with value sizes and offsets, its size is **key count** * (4+4)
    - value offsets are within this Update, not from start of the file
    - size is int, -1 size is tombstone
    - offset is int, single update can not be larger than 2GB

- **versionID** - byte[] - version of current update

- **prevVersionID** - byte[] - version of previous update

- section with values    


Shard Spec 
---------------
this is stored in Shard Layout Log

- **checksum** - long - checksum for update entry
- **update size** - int - number of bytes consumed by this entry, includes long
- **key count** - int - number of keys in this Update
- **key size** - int - number of bytes in each key in this Update
- **versionID size** - int - number of bytes used in versionID
- **prevVersionID size** - int - number of bytes used by previous versionID

- followed by table with size 'key count' of 
    - **start key** - byte[]
    - **file number** - long
    - **versionID size** - int - number of bytes used in versionID
    - **versionID** - byte[] - versionID at which this shard was created

Store format spec
=====================


LSM Store has two types of log files:

* Journal file stores recent modifications
* Shard files store older data 

They both share the same format and are composed of log entries 

## Types of Log Entries


### Header
Each Log Entry has following header:

* **checksum** - long - checksum for update entry 
    - it uses proprietary [64bit XXHash](https://github.com/jpountz/lz4-java/tree/master/src/java/net/jpountz/xxhash)
    - is calculated from entire entry including checksum (filled with zeroes)
* **entry size** - int - number of bytes consumed by this entry, includes checksum
* **Update Type** - byte - identifies type of Log Entry that follows
    - 1 - Update Entry
    - 2 - Distribute Entry
    - 3 - Offset Alias
    - 4 - Merge Entry
    
### Update Entry
Is written to Journal or Shard on each Update

* **prev update file offset** - long - offset of previous Update in file
* **prev update file number** - long - file number of file where previous Update is
* **key count** - int - number of keys in this Update
* **key size** - int - number of bytes in each key in this Update
* **versionID size** - int - number of bytes used in versionID
* **is merged** - boolean - true if this is merged update

* section with keys, its size is **number of keys** * **key size**

* section with value size & offset pair, 
    * section size is **key count** * (4+4)
    * value size is int, -1 size is tombstone (deleted key)
    * value offset is int, 
        * single update can not be larger than 2GB
        * value offset is counted from start of this update, not from start of the file


- **versionID** - byte[] - version of current update

- section with values    

### Distribute Entry

Distribute Entry is written to Journal after Distribute Task finishes.
Distribute Entry finishes log traversal (while finding keys) similar way Merge Entry does.
When Distribute Entry is found in Journal, its traversal should finish and traversal continues in Shards

* **prev update file offset** - long - offset of previous Update in file
* **prev update file number** - long - file number of file where previous Update is
* **shard count** - int - number of shards in this Distribute Entry

* section with Shard Prefix and file offsets
    **shard prefix** - int - first 4 bytes of shard key
    **shard pos file offset** - long - position in Shard where traversal continues for given version
    **shard pos file number** - long - filenumber in Shard where traversal continues for given version 


### Offset Alias 

Stored in Journal.  Is used to reconstruct Offset Alias table when Journal is reopened.

TODO format

### Merge Entry

TODO is there difference between Update Entry and this?
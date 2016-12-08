Store format spec
=====================

Log and merge file
----------------

Log File and Merge file share the same format, but have different file extension: `.log` and `.merge`.

Log File contains updates from single update (`Store.update()`). 
Merge File is created by background compaction and contains union of changes from older log files. 

Main Log and Shard Index both use log files. All code is in `LogStore` class. 

Log File (and Merge File, format is shared) contains following meta-information:

- File Header - 8 bytes long
- Checksum (used to detect data corruption, not for cryptography) - 8 bytes long
    - TODO this is not used yet
- File size (used to detect data corruption) - 8 bytes long
- Number of keys in file - 4 bytes int
- Key Size - 4 bytes int
- Version, an `long` version for which file was created. This number is used for file names. - 8 bytes long
- VersionID size - 4 bytes int
- VersionID, an `byte[]` versionID for which file was created - byte[] with size from previous field

Meta-information section is than followed by sorted table of keys. All keys have equal size, 
so fast binary search is used to find key within file. Each key contains extra information 
necessary to find values. Structure of each key entry is following:

- byte[] with key data, length is Key Size
- 4 bytes Value Size
    - `-1` Value Size represents tombstone, this key was deleted
- 8 bytes offset within file, where value is located. 

Keys are followed by values.

Shard Info file
-------------

Shard Info file with exception `.shardinfo` holds information necessary to identify what data are contained in shard.
From those files IODB reconstructs Shard Layout (`LSMStore.shards` field)

Shard Info file is parsed into `LSMStore.ShardInfo` class and it contains:

- Start VersionID, version under which shard was first time created.
- Start Version, long number used in file names. VersionID to Version translation is held in `LSMStore.versionsLookup`
- Start Key, first key in Shard Interval (inclusive). 
- Last Key, last key in Shard Interval (exclusive). Is null if shard has no upper bound

File is created by `LSMStore.ShardInfo.save()` method. 

Format:

TODO add file header and checksums

- Key Size - 4 byte int
- Start Version - 8 byte long
- Size of VersionID - 4 byte int
- VersionID data - `byte[]`, size from previous field
- isLastShard, if true shard has no upper bound - 1 byte boolean
- First Key, lower bound (inclusive) of shard interval - `byte[]`, size from Key Size
- Last Key, upper bound (exclusive) of shard interval - `byte[]`, size from Key Size
    - this field is not present if `isLastShard` is true, in this case shard has no upper bound 


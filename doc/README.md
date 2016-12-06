IODB
==========

IODB is key-value store designed for blockchain applications. Main features include:

- log-structured storage inspired by LevelDB. existing data are never overwritten. 
- versioned snapshots with rollback capability
- sorted keys
- atomic batch updates
- multi-threaded background compaction


Updates
----------

All updates are grouped into batch. 
Each update creates new snapshot with unique `VersionID`
Example update is [here](../src/main/test/scala/examples/Update.scala).

Update method takes list of key and value pairs (`Iterable[(ByteArrayWrapper,ByteArrayWrapper)]`) for update. 

Each update also takes list of keys to delete (`Iterable[ByteArrayWrapper]`). 
IODB does not really delete entries, but places 'tombstone' into log. 
Delete is just special type of value. 

Main Log
---------------

Each update  is placed into single 'log file'. 
Log file is newer overwritten, but might be deleted by compaction process once it becomes obsolete. 
Sequence of updates is 'main log'. 
List of entries for given version (snapshot) can be always reconstructed by replaying all log files from oldest to newest. 

Key lookup (`get`) traverses log files (updates) from newest to oldest, until key (or its tombstone) is fowund. 

Main Log provides durability for store. 
Main Log should survive JVM or hardware crash. 

Merge Files
------------

Traversing entire log to find keys could take long time. 
So compaction process periodically creates Merge Files (with `.merge` extension). 
This file contains all key-value entries from older log files files. 
To create merge file, compaction replays updates from older log files.

When key lookup reaches Merge File, it can stop log traversal. If key is not found in Merge File, it is not going to exist in older version.

Merge File does not have to include key tombstones for deleted keys. 
So compaction process can exclude tombstones when creating merge files.

Merge file is created by compaction process by replaying all older update files.
Keys in log file are sorted, so lazy N-way merge with single traversal can be used. 
To create merge file it takes approximately O(N*log(M)) time, where N is the total number of entries and M is number of versions in merge.
Usually M is small enough, not all versions are used for replay, 
but there is older merge file. 

Sharded Index
------------------

It is not practical to create merge files for large datasets. 
For that reason IODB maintains separate index for lookups. 
It slices (shards) keys into manageable intervals (typically 16 MB), 
and maintains separate log for each interval. 

Compaction process takes data from main log and distributes it across shards. 
It also periodically creates merge files on most updated shards and 
removes obsolete files. 

Number of shards is not fixed. When shard becomes too big (too many keys in interval),
it is sliced into number of smaller shards with smaller intervals. 
If shard becomes too small (too little keys), it is merged with its neighbours.

Shard Index can be reconstructed from main log. 
Its files do not have to be protected from corruption and can be safely deleted.

Background compaction
------------------------

Updates in IODB are very fast. 
Each commit creates only single file and exits. 
Most maintenance  is performed in background by multi-threaded compaction. 

Compaction in IODB is inspired by RocksDB. It is also multi-threaded and runs 
in background. But compaction in IODB solves one big problem:
RocksDB compaction has long running tasks, at worse merge task might have to 
read and reinsert entire store. If this long running tasks is terminated, 
its progress is lost and it must start from beginning.

Compaction in IODB is composed of small atomic tasks. 
In theory single task should always work with batch smaller than 100 MB. 
This greatly simplifies administration of IODB. 
It also  makes it simple to share CPU with other tasks. 
For example compaction process can be temporary paused if other tasks require CPU, memory or disk IO. 

Rollback 
--------------
Updates can be rolled back. 
Rollback will discard all changes made in newer updates and restore store to older version. 
Rollback example is [here](../src/main/test/scala/examples/Rollback.scala).

Rollback will delete newer update files and release some disk space


Clean
-----------
IODB can clean old versions to save disk space. 
Clean example is  [here](../src/main/test/scala/examples/Clean.scala)

Cleanup will remove all older snapshots, except last N versions (N is passed in parameter).
It will no longer be possible to rollback to older versions (except last N versions).
Data inserted at older updates will not be lost, but merged into oldest preserved snapshot.

Cleanup will delete older log files, but will preserve their data in Merge Files. 
Cleanup qis necessary for releasing disk space.

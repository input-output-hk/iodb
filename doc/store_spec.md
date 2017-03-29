IODB Specification
======================

Features
------------

IODB is embedded storage engine designed for blockchain applications.
It is inspired by [RocksDB](http://rocksdb.org).
It provides ordered key-value store, it is similar to `SortedMap<byte[], byte[]>`.
Its main advantage are snapshots with branching and fast rollbacks.  

Main features include:
* Ordered key-value store 
* Written in Scala, functional interface
* Multi-threaded background compaction
* Very fast durable commits
* Atomic updates with MVCC isolation and crash protection
* Snapshots with branching and rollbacks
* Log structured storage, old data are never overwritten for improved crash protection


Technical overview
-----------------------


* Key-value store is backed by Log-Structured Merge-Tree inspired by RocksDB and SSTables from Cassandra
    * Some general ideas about LSMTrees are in separate [blog posts](http://www.mapdb.org/blog/lsm_store_and_updates/) 

* Modifications are appended to end of file, old data are never overwritten
    * Append-only files are more durable than traditional Write-Ahead-Log
    * Writes and commits are faster, only single fsync is needed.
    
* There is compaction process which merges changes into main store and removes duplicates    
    * It runs in background process and does not interfere with normal IO
    * Compaction is multi-threaded
    * Compaction is split into several smaller tasks, each operates on small data set. That improves concurrency and memory usage.
    
* Some features are designed for blockchain applications
    * All keys in store have the same size
    * Keys in store have high entropy, compression would not work and is not implemented
    * Version IDs (identifies snapshots) are variable sized `byte[]` (64bit longs would not be enough)
           
Dictionary
------------

**Update** - Modifications are inserted in batch updates. Each update has **Version ID** and creates Snapshot. Synonym is commit.

**Version ID** - Identifies each Update (snapshot). Is supplied by user on each Update, latter can be used for rollback, 

**Update Entry** - Section of Log File, it contains data from an Update Operation.  

**Merge Entry** - Section of Log File, it contains merged content of multiple Update Operations. 

**Log File** - File that contains multiple Update Entries. More updates can be appended to the end of file. 
   Basic Log File structure is described in this [blog post](http://www.mapdb.org/blog/lsm_store_and_updates/).

**Journal** - One or more Log Files. It stores most recent modifications.
      
**Shard** - One or more Log files. It contains older data. There are multiple Shards, each contains key from its interval. Shard intervals are not overlapping.

Data lifecycle
----------------------

* All modifications (updates and deletes) are inserted into Journal. 

* There is Distribute Task, it runs in background and distributes data from Journal into Shards.
    * This operation inserts new Update Entries into Shard Log File
    * After data are written to Shards, Journal content might get discarded if their Snapshots are expired
 
* There is Shard Compaction Task, it runs in background and makes Shards more efficient 
    * It chooses most fragmented Shard (with most Update Entries)
    * It merges all Update Entries from given shard
    * It writes new Merge Entry into Shard Log File
    * Older Update Entries might get discarded (if in separate file, and their Snapshots are expired)
    
* If Shard becomes too big, Shard Compaction Task will split it into multiple smaller shards.

* If Shard becomes too small, Shard Compaction Task will merge it into its neighbour. 
 
Compaction
----------------

* Compaction runs in background processes and should not block normal operations (get and update). This is not true in current 0.3 release.

* IODB compaction tasks work on small data chunks 
    * Each compaction task should finish within seconds
    * Small data chunks decrease disk space overhead 
    * That improves concurrency
    * Long running tasks are problem in RocksDB and similar storage engines 

Storage format
----------------

Storage format is described in [separate doc](store_format.md)

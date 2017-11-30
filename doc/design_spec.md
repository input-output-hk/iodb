IODB design specification
============================

This document outlines design and implementation of IODB database at 0.4 release.


Data lifecycle
------------------

IODB moves data around to ensure good performance

### Update

Modifications (updated key-value pairs and keys to delete) are inserted in
batches (here referred as updates).
Each Update is identified by `VersionID` and creates new snapshot which
can be rolled back to.

Over time data (key-value pairs) move following way:

- journal contains most recent updates
- over time journal becomes too long, so Distribute Task is triggered
- at start Distribute Task inserts Distribute Placeholder entry into Journal
- Distribute Task moves data from Journal into Shards
- after Distribute Task finishes, it puts file positions in Shards into Journal
- over time (after some Distribute Tasks are finished) some Shards become too long, this triggers Shard Merge task
- Shard Merge tasks select longest Shard and merges content from multiple Update Entries into single Merge Entry


### find key (get)

`Store.get()` returns value associated with the key, or `None` if key is not found

- search traverses journal, until Distribute Placeholder is found (or key is found)
- from Distribute Entry it takes position in Shard
- search continues by traversing Shards from given position
- search finishes when
    - key is found
    - Merge Entry is found in Shard
    - end of Shard is reached


### get all

`Store.getAll()` returns content of store (all key-value pairs) for given `VersionID`.

Content is merged in a following way:

- traverse Journal until Distribute Placeholder is found
- take all data in Journal from VersionID until Distribute Placeholder
    - merge them into single data set
    - store it in temporary file (is currently done in-memory and that causes out-of-memory exceptions)
    - this is referred here as Journal content
- iterate over content of shards and merge them with Journal content
    - Shards are not overlapping
    - Journal content is sorted
    - Shard and Journal content is merged on single iteration


Rollback
----------

Rollback reverts store into state at given VersionID. It discards data inserted after
given VersionID.

Log is chain of updates. It is not possible to overwrite new updates during rollback.
So the new update (Offset Alias Log entry) is written into Journal.
It instruct search and other operation to skip irrelevant data while traversing the log.

Rollback is performed in following way
- traverse Journal until matching VersionID is found
- continue Journal traversal until Distribute Entry is found
    - this is first distribute entry after VersionID
    - we need Shard positions for given VersionID
- insert Offset Alias Log Entry into journal
- rollback Shards into state found in Distribe Entry



### Shards

- binary search on large tables is slow, also compaction on large tables is slow
- only most recent data are stored in Journal
- older data are distributed into shards
- there is Distribute Task, it takes data from Journal and distributes them into Shards


#### Shard Splitting

- ideally Shards should be created dynamically
    - small or empty store starts with single shards
    - if Shard becomes too big, it is sliced into several smaller Shards
    - if Shard becomes too small, it is merged into its neighbours

- dynamic Shard allocation has following advantages
    - less configuration, number of shard scales with store size
    - Shard Boundaries are self-balancing
        - if Key Space is sliced at constant intervals, and key distribution is not random (for example incremental keys), most keys can end in single interval
        - with dynamic Shard allocation new shards are created by slicing old Shards.

- 0.4 release has **static sharding**
    - `ShardedStore` has `shardCount` parameter
        - this is hardcoded into store and can not be changed
    - key space is sliced at constant interval
    - dynamic Sharding was in 0.3 release, but caused concurrency issues


File access and IO
------------------





Background Operations
--------------------------

- background executors versus manual execution
-


### Distribute Task

### Compact Task



Concurrency
-----------


### Internal State

#### File reopen (restore internal state)

### Atomicity
### Rollback
### Distribute task
- distribute task

### Merge task


Source code overview
------------------------


### ShardedStore

#### parameters

- dir
- keySize
- shard count
- executor

#### Concurrency

- distributeLock

#### journal
#### locks and executors
#### taskMerge



### LogStore

### serialize methods
### append methods


### QuickStore


### Utils



File format overview
-----------------------

Binary format is described in `store_format.md`

In short log can contain following types of entries:

- **update log entry** - Is inserted on each update
- **merge log entry** - Is inserted by Compaction Task, merges content from multiple updates into single log entry
- **distribute placeholder log entry** - Inserted into journal when distribute task starts.
- **distribute log entry** - Inserted into journal after distribute task finishes. It contains pointers into shards.
- **offset alias log entry** - inserted by Rollback or Compaction Task. It replaces Update Entry with new one. For example after compaction the most recent update is replaced by Compacted Entry.


Unit tests
-------------------


Issues
------

-

- getAll() loads all data on heap, use merge iterator

- serialize update into `byte[]`

- compaction is slow
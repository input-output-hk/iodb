0.4 release
----------------

- Rewrite LogStore and ShardedStore (former LSMStore)

- Reduce internal state to fix concurrency issues

- Change design and compaction model. Journal is now source of durability, rather than short term cache

- Added background threads and background compaction

Issues:

- See [list on github](https://github.com/input-output-hk/iodb/issues?q=is%3Aissue+is%3Aopen+label%3A0.5) targeted for `0.5` release



0.3.1
----------------

- Improve performance of a task distributing content between shards.

- Add `QuickStore`. It stores data in-memory, but also has durability and rollbacks.

- Improve sorting performance, updates and background tasks are 5x faster. 

- Reduce memory footprint, background task no longer load entire shard into memory.

- Reduce number of opened file handles. 

0.3 release 
-------------

- Use 64 bit non-cryptographic checksum to protect from data corruption. [Issue #7](https://github.com/input-output-hk/iodb/issues/7).

- Rollback would not handle deleted keys correctly. [Issue #17](https://github.com/input-output-hk/iodb/issues/17).

- Reintroduce memory-mapped and Unsafe file access methods.

- Add `Store.getAll()` to access all key-value pairs at given version. 

- Fix `get()` after `close()` causes JVM crash. [Issue #16](https://github.com/input-output-hk/iodb/issues/16).

- Add smoke tests to randomly test insert, delete and rollback.

- Fix NPE in background thread, if folder gets deleted after unit tests finishes. [Issue #15](https://github.com/input-output-hk/iodb/issues/15).


0.2 release
---------------

Rework shards and journal design. Better scalability.
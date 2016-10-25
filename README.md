IODB is persistent key-value store. It is inspired by [RocksDB](http://rocksdb.org) .
It has:

 - ordered key-value store
 - snapshots with rollbacks
 - multi-threaded compaction
 - batch imports

Examples
---------------------

Code examples are in [src/test/scala/examples](src/test/scala/examples) folder.

Compile
---------

- Checkout IODB
- Install SBT
- Compile IODB: `sbt publish`


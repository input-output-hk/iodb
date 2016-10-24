IODB is persistent key-value store. It is inspired by [RocksDB](http://rocksdb.org) 
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

IODB depends on not-yet released snapshot of [Scrypto](https://github.com/input-output-hk/scrypto). To use 
IODB:

- install SBT
- Checkout [Scrypto](https://github.com/input-output-hk/scrypto) from github
- Deploy Scrypto locally: `sbt publish-local` (version in central repo will cause compilation error in IODB)
- Compile IODB: `sbt publish`


Roadmap 
-------------------

###0.1 release 
- Expected at 4th November 2016
- Stable implementation of [Store interface](../src/main/scala/io/iohk/iodb/Store.scala) needed to support Scorex functionality
- Scorex will include example how to configure IODB as one of the backends
- Solve scalability issue on large dataset
- Performance comparable to RocksDB (with Unsafe, normal mmaped files will be 2x slower)
- Publish benchmarks
    - Run bench every week as part of Continuous Integration    
- Publish details about internal architecture
- Short tutorial with examples

### 0.2 release 
- Expected mid-January 2017
- [List of issues](https://github.com/input-output-hk/iodb/milestone/1)
- This milestone replaces Main Log with Journal and Shards. 
- Redesign storage format to 
  - solve scalability issue
  - reduce memory overhead
  - number of open file handles (that might cause JVM crash)

### 0.3 release 
- Expected late-January 2017
- [List of issues](https://github.com/input-output-hk/iodb/milestone/2)
- This release integrates stress testing and crash tests, to verify data survive fatal system failure
- Improves concurrency, fine grained locking, parallel compaction


Roadmap 
-------------------

###0.1 release 
- expected at 4th November 2016
- Stable implementation of [Store interface](../src/main/scala/io/iohk/iodb/Store.scala) needed to support Scorex functionality
- Scorex will include example how to configure IODB as one of the backends
- Solve scalability issue on large dataset
- Performance comparable to RocksDB (with Unsafe, normal mmaped files will be 2x slower)
- Publish benchmarks
    - Run bench every week as part of Continous Integration    
- Publish details about internal architecture
- Short tutorial with examples

### 0.2 release 
- Expected mid-November 2016
- IODB will implement BitcoinJ backend
- Option to force  latest version of data into offheap cache ([Issue #1](https://github.com/input-output-hk/iodb/issues/1))


### Open questions

- MapDB cointains its own Authenticated Skip List. It depends on Scrypto for hashing. 
Move this to Scrypto to reduce dependencies?



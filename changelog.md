
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
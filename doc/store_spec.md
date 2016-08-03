Storage spec
======================

NOTE: please excuse state of this spec. At this point is basically brain-dump to capture requirements for unit tests.

IODB is storage for Scorex. It is key-value store in form of `Map<byte[], byte[]>`.
Keys are sorted and allow range queries. It has snapshots and can rollback several
versions back.

It uses Log-Structured-Merge tree similar to LevelDB. It is also similar to `StoreAppend` from
MapDB and is based on its design. However there are following differences:

- IODB has fixed key-size for entire store (usually 32 bytes). LevelDB supports variable key size. IODB can do faster and simpler binary search on keys

- IODB key size is unlimited in theory (512 bytes seems reasonable max). ``StoreAppend` uses much smaller keys in form of 6-byte or 8-byte recids.

- Keys in IODB have high entropy. It can use delta packing or other compressions on keys.

High level overview
----------------------

- IODB stores data in append-only log files.
    - Old data are never overwritten.
    - File might be deleted by compaction once its data are obsolete, and its snapshots expired.
- Writes are very fast since only small batch is written to log file.
- Background multithreaded compaction is used to keep store fast.

IODB has three types of files

- Log files.
    - Data from each batch update (commit) are written into log.
    - It provide durability, so in case of hard crash
    - Can find latest version of key (reverse traverse updates, binary search on each update)

- Index files
    - Sorted table of keys
    - Key is found using binary search
    - Each Index file has non-durable Write Buffer (log file) of not yet merged keys.
    - Compaction periodically merges changes from Write Bufferr into Index file

Compaction
----------------

Compaction runs on backround. It should work on small batches (move 16MB of data). There are following tasks in compaction:

- Take log file and write its content into Index File Write Buffer

- Generate Write Buffer Sorted Table

- Take Index File with big Write Buffer, and merge Write Buffer into Index File.
    - if file becomes too big, it is splited

- Take Index File which is too small and merge multiple neighbours into single Index File.

- Delete old log file
    - traverse all its keys, check their exist on newer snapshots
    - write new log file, with keys which were not in newer snapshots

Log file
--------------
Append-only file. Contains key-value pairs from batch updates. It can efficiently find latest version of key if end offset is known.

Its format has to support:

- Efficient traversal of batch updates in reverse order.
- Forward replay to reconstruct store in case of crash
- Efficient binary search for keys.

Log file has two uses:
- Main Write log, in this case the log file should be durable (`FileDescriptor.sync()).
- Write Buffer for Index File. Optionally durable.

Format
- Header
- Commit Entries (one entry for each commit)
    - sorted table of keys in commit (for binary search)
    - values in commit (or references to Main Log)
    - at end is commit size to support reverse commit traversal
- Every Nth entries there is Big Sorted Table.
    - it takes all keys previously written into this Log file
    - sorts them
    - write table of their offset into Log File.
        - full keys could be used, but offsets are more space efficient
    - Search for latest keys does not have to continue reverse traversal.

Index file
---------------

Sorted table of keys

- TODO should values be stored here? Reference to Log for more space efficient?


File naming convention
---------------------------
- Use longs for all numbers

- Do not group multiple versions into single file at initial version

Dimensions
- Commit version
- Merged content
- Write Buffer

===Index file name===
- "Index", maybe separate folder, this is not durable
- IndexID
    - points to table with index hints (hi,lo, middle keys)
    - boundaries at the same Commit should not overlap
    - change in Hi&Lo results in new IndexId
- CommitID

###Write buffer name

- is Log with different prefix
- "Index", perhaps separate folder
- IndexId
- No commitId, it is contained in Log structure


###Log file name
- Initial version puts each commit into separate file
- "Log"
- IndexId

Log file format
-------------------

Log file format is limited by following:

- it is append only file created with `FileOutputStream`. It is not possible to write data to start of file. 

- it should detect partially created files, and recover from that (file rename, checksum)

- we use file size to find end of the file. But memory buffers etc do not provide file size.

- keys and values are streamed together, we need to store values in separate file to determine its offset.

- 2GB file size limit, `ByteBuffer` uses `int` for addressing 


Log file has following structure

- 8 byte header and version info
- 4 byte file size, might be 0 on filesystem where file size is provided
- 4 byte key size
- 8 byte versionID
- 8 byte shardID
- payload (keys or values)
- 4 byte EOF marker 
- 4 or 8 byte checksum of entire file

Get operation
----------------

- Traverse log in reverse order, until key is found
- Finish traversal at nearest Index Write Buffer or nearest Index File
- Look up Write Buffer
- Look up Index File



TODO
---------


- compaction
    - limits
        - N log M
        - memory, each M needs one key in memory
    - levels
        - merge multiple levels in one go?
        - parallel between BTree Pump and increasing number of levels?
        - paint some charts

- file types
    - WAL
        -keys with value refs
        -values
    - WAL markers
        - list of obsolete keys
    - keys
        - from file
        - to file
        - replaces file at older level
        - lower levels might not be deleted, if their snapshots are not used
    - values
        - how values are referenced from other files?
            - values markers > points to file where value is referenced from

- mmap
    - create file using BufferedInputStream > FileStream
        - prevents JVM crash if delayed mmap write fails
        - groups smaller keys together
        - use FileStream#getFD() to disk sync
        - FileStream#getFileChannel() can scroll stream -> write headers
    - maximal file size is 2 GB
    - in memory key hints
        - in memory stores max/min key for each file
        - store key on every 1MB for faster initial binary search
            - 1 MB interval can be derived by cache size memory parameter (cache size/total store size)
    - windows problems
        - can not delete files unless unmapped
        - use slow fallback option (RAF)?


 - backup snapshot with hard links in separate directory

 - file number & offset pointers, file generations, cyclic queue of generations.
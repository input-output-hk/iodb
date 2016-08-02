package io.iohk.iodb

import java.io._
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{ConcurrentSkipListMap, Executors, ThreadFactory, TimeUnit}

import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Store which combines append-only log and index files
  */
class LSMStore(
                dir: File,
                keySize: Int = 32,
                backgroundThreads: Int = 1,
                val keepSingleVersion:Boolean = false
              ) extends Store {


  //TODO configurable, perhaps track number of modifications
  protected val shardEvery = 3L

  protected val minMergeSize = 1024*1024L
  protected val minMergeCount = 10L

  protected val logger = LoggerFactory.getLogger(this.getClass)


  protected val fileLocks = new MultiLock[File]()

  /** main log, contains all data in non-sharded form. Is never compacted, compaction happens in shards */
  protected val mainLog = new LogStore(dir, filePrefix = "log-", keySize = keySize,
      fileLocks = fileLocks, keepSingleVersion = keepSingleVersion)

  /** executor used to run background tasks. Threads are set to deamon so JVM process can exit,
    *  while background threads are running */
  protected val executor = Executors.newScheduledThreadPool(backgroundThreads,
    new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r)
        t.setDaemon(true)
        t
      }
    })

  /** map of shards. Key is upper inclusive bound of shard. Value is log containing sharded values */
  protected val shards = new ConcurrentSkipListMap[K, LogStore]()

  /** greatest key for this store */
  protected val maxKey = new ByteArrayWrapper(Utils.greatest(keySize))

  /** generates incremental sequence of shard IDs */
  protected val shardIdSeq = new AtomicLong(0)

  /** concurrent lock, any file creation should be under write lock,
    * data read should be under read lock */
  protected val lock = new ReentrantReadWriteLock()

  protected val _lastVersion = new AtomicLong(-1);

  {
    shardAdd(maxKey)

    def runnable(f: => Unit): Runnable = new Runnable() {
      def run() = f
    }
    //schedule tasks
    executor.scheduleWithFixedDelay(runnable {
      taskShardLog()
    }, 1000L, 1000L, TimeUnit.MILLISECONDS)

    executor.scheduleWithFixedDelay(runnable {
      taskShardMerge()
    }, 1000L, 1000L, TimeUnit.MILLISECONDS)

  }

  override def get(key: K): V = {
    lock.readLock().lock()
    try {
      val shard = shards.ceilingEntry(key).getValue
      val mainLogVersion = lastVersion
      val shardVersion = shard.lastVersion
      if(mainLogVersion!=shardVersion){
        //some entries were not sharded yet, try main log
        val ret = mainLog.get(key=key, versionId = mainLogVersion, stopAtVersion = shardVersion)
        if(ret!=null)
          return ret.getOrElse(null); //null is for tombstones found in main log
      }

      return shard.get(key)
    } finally {
      lock.readLock().unlock()
    }
  }

  /** gets value from sharded log, ignore main log */
  protected[iodb] def getFromShard(key: K): V = {
    lock.readLock().lock()
    try {
      val shard = shards.ceilingEntry(key).getValue
      return shard.get(key)
    } finally {
      lock.readLock().unlock()
    }
  }
  override def lastVersion: Long = {
    _lastVersion.get()
  }

  override def update(versionID: Long, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    lock.writeLock().lock()
    try {
      if(_lastVersion.get()>=versionID){
        throw new IllegalArgumentException("versionID in argument is not greater than Store lastVersion")
      }
      mainLog.update(versionID, toRemove, toUpdate)
      _lastVersion.set(versionID)
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def rollback(versionID: Long): Unit = {
    lock.writeLock().lock()
    try {
      mainLog.rollback(versionID)
      _lastVersion.set(mainLog.lastVersion)
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def clean(version: Long): Unit = {
    lock.writeLock().lock()
    try {
      mainLog.clean(version)
    } finally {
      lock.writeLock().unlock()
    }
  }

  protected[iodb] def closeExecutor() = {
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.DAYS) //TODO better executor shutdown

  }


  override def close(): Unit = {
    lock.writeLock().lock()
    try {
      closeExecutor()
      mainLog.close()
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def cleanStop(): Unit = {
    mainLog.cleanStop()
  }

  //TODO restore this var on file reopen
  protected var lastShardedLogVersion = -1L

  protected[iodb] def taskShardLogForce(): Unit = {
    lock.writeLock().lock()
    try {
      val lastVersion = this.lastVersion
      //iterator over data modified in last shard
      val toDelete = new ArrayBuffer[K]()
      val toUpdate = new ArrayBuffer[(K, V)]()

      var nextKey = shards.firstKey()
      var keyCounter = 0L
      var shardCounter = 0L
      def flushBuffers(): Unit = {
        //flush buffer if not empty
        if (!toDelete.isEmpty || !toUpdate.isEmpty) {
          val shard = shards.get(nextKey)
          //TODO update will sort values, that is unnecessary because buffers are already sorted
          shard.update(lastVersion, toDelete, toUpdate)
          toDelete.clear()
          toUpdate.clear()
          shardCounter += 1;
        }
      }
      for ((key, value) <- mainLog.keyValues(lastVersion, lastShardedLogVersion)) {
        keyCounter += 1
        //progress to the next key if needed
        while (nextKey.compareTo(key) < 0) {
          flushBuffers()
          nextKey = shards.higherKey(nextKey)
        }

        if (value == null) {
          toDelete.append(key)
        } else {
          toUpdate.append((key, value))
        }
      }
      flushBuffers()
      if (logger.isDebugEnabled())
        logger.debug("Task - Log Shard completed. " + keyCounter + " keys into " +
          shardCounter + " shards. Versions between " +
          lastShardedLogVersion + " and " + lastVersion)
      lastShardedLogVersion = lastVersion
      if(keepSingleVersion) {
        //everything is distributed to shards,delete all files
        mainLog.deleteAllFiles()
      }
    } finally {
      lock.writeLock().unlock()
    }
  }

  protected[iodb] def taskShardLog(): Unit = {
    lock.writeLock().lock()
    try {
      val lastVersion = mainLog.lastVersion
      if (lastVersion < lastShardedLogVersion + shardEvery) {
        //do not shard yet
        return
      }

      taskShardLogForce()
    } finally {
      lock.writeLock().unlock()
    }
  }


  protected[iodb] def taskShardMerge(): Unit = {
    lock.writeLock().lock()
    try {
      //TODO select log to compact
      val log = shards.firstEntry().getValue

      //if there is enough unmerged versions, start merging
      val (unmergedCount,unmergedSize) = log.countUnmergedVersionsAndSize()

//      println("Unmerged count: "+unmergedCount + " - "+unmergedSize)
      if(unmergedCount<minMergeCount && (unmergedSize<minMergeSize))
        return;

      log.merge();

      if(logger.isDebugEnabled())
        logger.debug("Task - Log Merge completed, merged "+unmergedCount+" versions")

    } finally {
      lock.writeLock().unlock()
    }
  }

  protected[iodb] def getShards = shards


  protected[iodb] def shardAdd(key: V) = {
    lock.writeLock().lock()
    try {
      val log = new LogStore(dir = dir, filePrefix = "shardBuf-" + shardIdSeq.incrementAndGet() + "-",
        keySize = keySize, fileLocks = fileLocks, keepSingleVersion = keepSingleVersion,
        fileSync=false)

      val old = shards.put(key, log)
      assert(old == null)
    } finally {
      lock.writeLock().unlock()
    }
  }

}

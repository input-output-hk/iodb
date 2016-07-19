package io.iohk.iodb

import java.io._
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{Executors, TimeUnit}

import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
  * Store which combines append-only log and index files
  */
class LSMStore(dir: File, keySize: Int = 32,
               backgroundThreads: Int = 2) extends Store {

  protected val logger = LoggerFactory.getLogger(this.getClass)

  protected val appendLog = new LogStore(dir, filePrefix = "log-", keySize = keySize)
  protected val executor = Executors.newScheduledThreadPool(backgroundThreads)

  protected val shards = new java.util.TreeMap[K, LogStore]()

  protected val maxKey = new ByteArrayWrapper(Utils.greatest(keySize))

  protected val shardCount = new AtomicLong(0)

  protected val lock = new ReentrantReadWriteLock()

  {
    shardAdd(maxKey)

    def runnable(f: => Unit): Runnable = new Runnable() {
      def run() = f
    }
    //schedule tasks
    executor.scheduleWithFixedDelay(runnable {
      taskShardLog()
    }, 1000L, 1000L, TimeUnit.MILLISECONDS)

  }

  override def get(key: K): V = {
    lock.readLock().lock()
    try {
      return appendLog.get(key)
    } finally {
      lock.readLock().unlock()
    }
  }

  /** gets value from sharded buffer, ignore log */
  protected[iodb] def getFromShardBuffer(key: K): V = {
    lock.readLock().lock()
    try {
      val shard = shards.ceilingEntry(key).getValue
      return shard.get(key)
    } finally {
      lock.readLock().unlock()
    }
  }


  /** gets value from sharded index, ignore log */
  protected[iodb] def getFromShardIndex(key: K): V = {
    lock.readLock().lock()
    try {
      val shard = shards.ceilingEntry(key).getValue
      return shard.get(key)
    } finally {
      lock.readLock().unlock()
    }
  }
  override def lastVersion: Long = {
    lock.readLock().lock()
    try {
      return appendLog.lastVersion
    } finally {
      lock.readLock().unlock()
    }
  }

  override def update(versionID: Long, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    lock.writeLock().lock()
    try {
      appendLog.update(versionID, toRemove, toUpdate)
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def rollback(versionID: Long): Unit = {
    lock.writeLock().lock()
    try {
      appendLog.rollback(versionID)
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def clean(version: Long): Unit = {
    lock.writeLock().lock()
    try {
      appendLog.clean(version)
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def close(): Unit = {
    lock.writeLock().lock()
    try {
      executor.shutdown()
      executor.awaitTermination(1, TimeUnit.DAYS) //TODO better executor shutdown
      appendLog.close()
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def cleanStop(): Unit = {
    appendLog.cleanStop()
  }

  //TODO restore this var on file reopen
  protected var lastShardedLogVersion = -1L

  //TODO configurable, perhaps track number of modifications
  protected val shardEvery = 20L

  protected[iodb] def taskShardLogForce(): Unit = {
    lock.writeLock().lock()
    try {
      val lastVersion = this.lastVersion
      if (logger.isDebugEnabled)
        logger.debug("Task - Shard Log started, using data between version " +
          lastShardedLogVersion + " and " + lastVersion)
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
      for ((key, value) <- appendLog.keyValues(lastVersion, lastShardedLogVersion)) {
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
      lastShardedLogVersion = lastVersion
      if (logger.isDebugEnabled())
        logger.debug("Task - Log Shard completed. " + keyCounter + " keys into " + shardCounter + " shards.")
    } finally {
      lock.writeLock().unlock()
    }
  }

  protected[iodb] def taskShardLog(): Unit = {
    lock.writeLock().lock()
    try {
      val lastVersion = appendLog.lastVersion
      if (lastVersion < lastShardedLogVersion + shardEvery) {
        //do not shard yet
        return
      }

      taskShardLogForce()
    } finally {
      lock.writeLock().unlock()
    }
  }

  protected[iodb] def getShards = shards


  protected[iodb] def shardAdd(key: V) = {
    lock.writeLock().lock()
    try {
      val log = new LogStore(dir = dir, filePrefix = "shardBuf-" + shardCount.incrementAndGet() + "-", keySize = keySize)

      val old = shards.put(key, log)
      assert(old == null)
    } finally {
      lock.writeLock().unlock()
    }
  }

}

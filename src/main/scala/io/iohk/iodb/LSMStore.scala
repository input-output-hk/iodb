package io.iohk.iodb

import java.io._
import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{ConcurrentSkipListMap, Executors, ThreadFactory, TimeUnit}

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * Store which combines append-only log and index files
  */
class LSMStore(
                dir: File,
                keySize: Int = 32,
                backgroundThreads: Int = 1,
                val keepSingleVersion: Boolean = false,
                useUnsafe: Boolean = Utils.unsafeSupported()
              ) extends Store {


  {
    //check argument is directory
    if (!dir.exists()) {
      throw new IOException("Directory does not exist")
    }
    if (!dir.isDirectory) {
      throw new IOException("Is not directory")
    }
    if (!dir.canWrite) {
      throw new IOException("Directory is not writable")
    }
  }

  //TODO configurable, perhaps track number of modifications
  protected val shardEvery = 3L

  protected val minMergeSize = 1024*1024L
  protected val minMergeCount = 10L

  protected val splitSize = 16*1024*1024
  protected val splitKeyCount:Int = splitSize/(keySize*2)

  protected val logger = LoggerFactory.getLogger(this.getClass)


  protected val fileLocks = new MultiLock[File]()

  /** main log, contains all data in non-sharded form. Is never compacted, compaction happens in shards */
  protected val mainLog = new LogStore(dir, filePrefix = "log-", keySize = keySize,
    fileLocks = fileLocks, keepSingleVersion = keepSingleVersion, useUnsafe = useUnsafe)

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

  /** Map of shards.
    * Primary key is versionId. Shard bounds change over , so layout for older versions is kept.
    * Key is lower inclusive bound of shard. Value is log with sharded values */
  protected val shards:util.NavigableMap[Long, util.NavigableMap[K, LogStore]] =
      new ConcurrentSkipListMap[Long, util.NavigableMap[K, LogStore]](
        java.util.Collections.reverseOrder[Long]())

  /** smallest key for this store */
  protected def minKey = new ByteArrayWrapper(new Array[Byte](keySize))

  /** generates incremental sequence of shard IDs */
  protected val shardIdSeq = new AtomicLong(0)

  /** concurrent lock, any file creation should be under write lock,
    * data read should be under read lock */
  protected val lock = new ReentrantReadWriteLock()

  {
    shards.put(-1L, new ConcurrentSkipListMap[K,LogStore]());

    shardAdd(minKey)

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
      val shard = shards.firstEntry().getValue.floorEntry(key).getValue
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
      val shard = shards.lastEntry().getValue.floorEntry(key).getValue
      return shard.get(key)
    } finally {
      lock.readLock().unlock()
    }
  }
  override def lastVersion: Long = {
    lock.readLock().lock()
    try {
      return mainLog.lastVersion
    } finally {
      lock.readLock().unlock()
    }
  }

  override def update(versionID: Long, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    lock.writeLock().lock()
    try {
      if(lastVersion>=versionID){
        throw new IllegalArgumentException("versionID in argument is not greater than Store lastVersion")
      }
      mainLog.update(versionID, toRemove, toUpdate)
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def rollback(versionID: Long): Unit = {
    lock.writeLock().lock()
    try {
      mainLog.rollback(versionID)
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

  /** takes values from main log, and distributes them into shards. This task runs in background thread */
  protected[iodb] def taskShardLogForce(): Unit = {
    lock.writeLock().lock()
    try {
      val shardLayout = shards.lastEntry().getValue
      val lastVersion = this.lastVersion
      //buffers which store modified values
      val toDelete = new ArrayBuffer[K]()
      val toUpdate = new ArrayBuffer[(K, V)]()

      // next key in shard, if key becomes equal or greater we must flush the buffer
      // null indicates end of shards (positive infinity)
      // get second key
      var cutOffKey = shardLayout.keySet().asScala.take(1).headOption.getOrElse(null)
      //log where update will be placed, is one entry before cutOffKey
      var cutOffLog = shardLayout.firstEntry.getValue
      // stats
      var keyCounter = 0L
      var shardCounter = 0L

      // this method will flush and clears toDelete and toUpdate buffers
      def flushBuffers(): Unit = {
        //flush buffer if not empty
        if (!toDelete.isEmpty || !toUpdate.isEmpty) {
          //TODO update will sort values, that is unnecessary because buffers are already sorted
          cutOffLog.update(lastVersion, toDelete, toUpdate)

          toDelete.clear()
          toUpdate.clear()
          shardCounter += 1;
        }
      }
      for ((key, value) <- mainLog.keyValues(lastVersion, lastShardedLogVersion)) {
        keyCounter += 1
        //progress to the next key if needed
        while (cutOffKey!=null && cutOffKey.compareTo(key) <= 0) {
          flushBuffers()
          //move to next log
          cutOffLog  = shardLayout.get(cutOffKey)
          cutOffKey = shardLayout.higherKey(cutOffKey)
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
      val shardLayout = shards.lastEntry().getValue
      val log = shardLayout.firstEntry().getValue

      //if there is enough unmerged versions, start merging
      val (unmergedCount,unmergedSize) = log.countUnmergedVersionsAndSize()

//      println("Unmerged count: "+unmergedCount + " - "+unmergedSize)
      if(unmergedCount<minMergeCount && (unmergedSize<minMergeSize))
        return;

      val currVersion = log.lastVersion
      //load all values
      val buf = log.keyValues(currVersion).toBuffer

      //check if it needs splitting
      if(buf.size*keySize >splitSize) {
        //TODO make sure that shardLayout map is at current versionId

        //insert first part into log
        var (merge2, buf2) = buf.splitAt(splitKeyCount)
        log.merge(currVersion, merge2.iterator)
        //now split buf2 into separate buffers
        while(!buf2.isEmpty){
          val (merge2, buf3) = buf2.splitAt(splitKeyCount)
          buf2 = buf3
          val startKey = merge2.head._1
          shardAdd(startKey)
          shardLayout.get(startKey).merge(currVersion, merge2.iterator)
        }

      }else{
        //merge everything into same log
        log.merge(currVersion, buf.iterator)
      }

      if(logger.isDebugEnabled())
        logger.debug("Task - Log Merge completed, merged "+unmergedCount+" versions")

    } finally {
      lock.writeLock().unlock()
    }
  }

  protected[iodb] def getShards = shards.lastEntry().getValue

  protected[iodb] def printShards(out:PrintStream=System.out): Unit ={
    out.println("==== Shard Layout ====")
    for((version, shardLayout)<-shards.asScala){
      out.println(version)
      for((key, log) <- shardLayout.asScala){
        out.println("    "+key)
      }
    }
  }

  protected[iodb] def shardAdd(key: V) = {
    lock.writeLock().lock()
    try {
      val log = new LogStore(dir = dir, filePrefix = "shardBuf-" + shardIdSeq.incrementAndGet() + "-",
        keySize = keySize, fileLocks = fileLocks, keepSingleVersion = keepSingleVersion,
        fileSync = false, useUnsafe = useUnsafe)

      val old = shards.lastEntry().getValue.put(key, log)
      assert(old == null)
    } finally {
      lock.writeLock().unlock()
    }
  }

}

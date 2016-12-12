package io.iohk.iodb

import java.io._
import java.util
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{ConcurrentSkipListMap, Executors, ThreadFactory, TimeUnit}
import java.util.logging.Level

import io.iohk.iodb.Store.{K, VersionID}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * LSMStore provides sorted LSM Tree backed by append-only-log files and sharded index files.
  * It has number of parameter which affects storage performance and background compaction process.
  *
  * @param dir                directory in which store files exist, or will be created
  * @param keySize            size of key byte array
  * @param backgroundThreads  number of background threads used by compaction
  * @param keepSingleVersion  if true compaction will automatically delete older versions, it will not be possible to use rollback
  * @param useUnsafe          if true sun.misc.Unsafe file access is used. This is faster, but may cause JVM process to crash
  * @param shardEveryVersions compaction will trigger sharding after N versions is added
  * @param minMergeSize       compaction will trigger log merge if combined unmerged data are bigger than this
  * @param minMergeCount      compaction will trigger log merge if there are N unmerged log files
  * @param splitSize          maximal size of sharded index. If merged index exceeds this size, it will be split into smaller shards by compaction
  */
class LSMStore(
                dir: File,
                keySize: Int = 32,
                backgroundThreads: Int = 1,
                val keepSingleVersion: Boolean = false,
                useUnsafe: Boolean = Utils.unsafeSupported(),

                protected val shardEveryVersions: Int = 3,

                protected val minMergeSize: Int = 1024 * 1024,
                protected val minMergeCount: Int = 2,

                protected val splitSize: Int = 16 * 1024 * 1024
              ) extends Store {

  import Store._

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

  protected val splitKeyCount:Int = splitSize/(keySize*2)

  protected val logger = LoggerFactory.getLogger(this.getClass)

  /** translates versionId (used in Store methods) to its actual number used in files */
  protected val versionsLookup = new ConcurrentSkipListMap[VersionID, Long]()


  protected val fileLocks = new MultiLock[File]()

  /** main log, contains all data in non-sharded form. Is never compacted, compaction happens in shards */
  protected[iodb] val mainLog = new LogStore(dir, filePrefix = "log-", keySize = keySize,
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
  protected[iodb] val shards: util.NavigableMap[Long, util.NavigableMap[K, LogStore]] = new ConcurrentSkipListMap()

  /** smallest key for this store */
  protected def minKey = new ByteArrayWrapper(new Array[Byte](keySize))

  /** generates incremental sequence of shard IDs */
  protected val shardIdSeq = new AtomicLong(0)


  /** generates incremental sequence of IDs */
  protected val versionIDSeq = new AtomicLong(0)


  /** concurrent lock, any file creation should be under write lock,
    * data read should be under read lock */
  protected val lock = new ReentrantReadWriteLock()

  protected[iodb] var lastShardedLogVersion = 0L

  {
    versionIDSeq.set(lastVersion)
    for ((version, logFile) <- mainLog.files.asScala) {
      val old = versionsLookup.put(logFile.loadVersionID, version)
      //TODO log should start empty, with no version
      //      if(old!=null)
      //        throw new DataCorruptionException("Duplicate VersionID ")
    }
    shards.put(0L, new ConcurrentSkipListMap());

    shardAdd(cutOffKey = minKey, endKey = null, version = 0L,
      versionID = new ByteArrayWrapper(0))

    //restore shards
    Utils.listFiles(dir, Utils.shardInfoFileExt).foreach { f =>
      val si = loadShardInfo(f)
      val prefix = f.getName.stripSuffix("." + Utils.shardInfoFileExt)
      shardAdd(
        cutOffKey = si.startKey,
        endKey = si.endKey,
        versionID = si.startVersionID,
        version = si.startVersion,
        filePrefix = prefix)
    }
    lastShardedLogVersion = getShards.values().asScala.map(_.lastVersion).max

    def runnable(f: => Unit): Runnable = new Runnable() {
      def run() = {
        try {
          f
        } catch {
          case e: Throwable => {
            Utils.LOG.log(Level.SEVERE, "Background task failed", e)
          }
        }
      }
    }
    //schedule tasks
    executor.scheduleWithFixedDelay(runnable {
      taskShardLog()
    }, 200L, 200L, TimeUnit.MILLISECONDS)

    executor.scheduleWithFixedDelay(runnable {
      taskShardMerge()
    }, 200L, 200L, TimeUnit.MILLISECONDS)

  }

  var counter = 0

  override def get(key: K): Option[V] = {
    lock.readLock().lock()
    try {
      counter += 1
      val shard = shards
        .lastEntry()
        .getValue
        .floorEntry(key)
        .getValue
      val mainLogVersion = lastVersion
      val shardVersion = shard.lastVersion
      if (mainLogVersion != shardVersion) {
        //some entries were not sharded yet, try main log
        val ret = mainLog.get(key = key, versionId = mainLogVersion, stopAtVersion = shardVersion)
        if (ret != null)
          return ret
      }

      return shard.get(key)
    } finally {
      lock.readLock().unlock()
    }
  }

  /** gets value from sharded log, ignore main log */
  protected[iodb] def getFromShard(key: K): Option[V] = {
    lock.readLock().lock()
    try {
      val shard = shards.lastEntry().getValue.floorEntry(key).getValue
      return shard.get(key)
    } finally {
      lock.readLock().unlock()
    }
  }

  def lastVersion: Long = {
    lock.readLock().lock()
    try {
      return mainLog.lastVersion
    } finally {
      lock.readLock().unlock()
    }
  }


  override def lastVersionID: VersionID = {
    lock.readLock().lock()
    try {
      return mainLog.lastVersionID
    } finally {
      lock.readLock().unlock()
    }
  }

  override def update(versionID: VersionID, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    lock.writeLock().lock()
    try {
      if (versionsLookup.containsKey(versionID))
        throw new IllegalArgumentException("versionID is already used")

      val version = versionIDSeq.incrementAndGet();
      versionsLookup.put(versionID, version)

      mainLog.update(versionID, version, toRemove, toUpdate)
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def rollback(versionID: VersionID): Unit = {
    lock.writeLock().lock()
    try {
      val version = getVersion(versionID)

      //remove all versionIDs from translation map
      val rollBackVersionIDs = mainLog.files.headMap(version, false).values.asScala.map(_.loadVersionID)
      rollBackVersionIDs.foreach(versionsLookup.remove(_))

      mainLog.rollback(version)

      val lastValidShardLayout = shards.floorEntry(version).getValue

      //delete all newer log files
      shards.tailMap(version, false).asScala.values
        .flatMap(_.entrySet().asScala)
        .foreach { e =>
          //if the same log exists in previous shards under same cutoff key, do not delete it, just roll back
          val cutOffKey = e.getKey
          val log = e.getValue
          if (lastValidShardLayout.get(cutOffKey) eq log) {
            log.rollback(version)
          } else {
            //this log file is newer, just delete it
            e.getValue.deleteAllFiles()
          }
        }
      shards.tailMap(version, false).clear()
      getShards.values().asScala.foreach(_.rollback(version))
    } finally {
      lock.writeLock().unlock()
    }
  }

  protected[iodb] def getVersion(versionID: VersionID): Long = {
    if (!versionsLookup.containsKey(versionID))
      throw new IllegalArgumentException("VersionID not found")
    return versionsLookup.get(versionID)
  }

  protected[iodb] def getVersionID(version: Long): VersionID = {
    val ret = versionsLookup.asScala.find(_._2 == version).map(_._1)
    return ret.getOrElse(throw new DataCorruptionException("versionID not found"))
  }

  override def clean(count: Int): Unit = {
    lock.writeLock().lock()
    try {
      val version2 = mainLog.files.keySet().asScala.take(count).lastOption
      if (version2.isEmpty)
        return
      //nothing to remove, less then N versions
      val version = version2.get
      val versionID = getVersionID(version)
      mainLog.clean(version, versionID)
      //TODO clear should remove old shard versions. Or at least mark them for removal.
      //      shards.values().asScala.flatMap(_.values().asScala).foreach(_.clean(version, versionID))
      //TODO purge old shards
    } finally {
      lock.writeLock().unlock()
    }
  }

  protected[iodb] def closeExecutor() = {
    executor.shutdown()
    executor.awaitTermination(1, TimeUnit.DAYS) //TODO better executor shutdown

  }


  override def close(): Unit = {
    closeExecutor()
    lock.writeLock().lock()
    try {
      mainLog.close()
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def cleanStop(): Unit = {
    mainLog.cleanStop()
  }

  /** takes values from main log, and distributes them into shards. This task runs in background thread */
  protected[iodb] def taskShardLogForce(): Unit = {
    lock.writeLock().lock()
    try {
      val shardLayout = shards.lastEntry().getValue
      val lastVersion = mainLog.lastVersion
      val lastVersionId = mainLog.lastVersionID
      //buffers which store modified values
      val toRemove = new ArrayBuffer[K]()
      val toUpdate = new ArrayBuffer[(K, V)]()

      // next key in shard, if key becomes equal or greater we must flush the buffer
      // null indicates end of shards (positive infinity)
      // get second key
      var cutOffKey = shardLayout.firstKey()
      //log where update will be placed, is one entry before cutOffKey
      var cutOffLog = shardLayout.firstEntry.getValue
      // stats
      var keyCounter = 0L
      var shardCounter = 0L

      // this method will flush and clears toRemove and toUpdate buffers
      def flushBuffers(): Unit = {
        //flush buffer if not empty
        if (!toRemove.isEmpty || !toUpdate.isEmpty) {
          //TODO SPEED: update will sort values, that is unnecessary because buffers are already sorted
          cutOffLog.update(version = lastVersion, versionID = lastVersionId,
            toRemove = toRemove, toUpdate = toUpdate)

          toRemove.clear()
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
          toRemove.append(key)
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
        //everything is distributed to shards, delete all files
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
      if (lastVersion < lastShardedLogVersion + shardEveryVersions) {
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
      var shardLayout = shards.lastEntry().getValue
      //get log with most files for merging
      val (origKey, log) = shardLayout.asScala.toSeq.sortBy(_._2.getFiles().size).last
      val mergeCount = log.getFiles.values().asScala.takeWhile(!_.isMerged).size
      //      println("MERGE COUNT - "+log.files.size() + " - " + mergeCount + " - " + origKey)
      var startKey = origKey
      val nextEndKey = shardLayout.higherKey(origKey)

      //if there is enough unmerged versions, start merging
      val (unmergedCount,unmergedSize) = log.countUnmergedVersionsAndSize()

//      println("Unmerged count: "+unmergedCount + " - "+unmergedSize)
      if(unmergedCount<minMergeCount && (unmergedSize<minMergeSize))
        return;

      val currVersion = log.lastVersion
      val currVersionID = log.lastVersionID

      //load all values
      val buf = log.keyValues(currVersion).toBuffer

      //check if it needs splitting
      if(buf.size*keySize >splitSize) {
        //insert new shard map
        shardLayout = new ConcurrentSkipListMap[K, LogStore](shardLayout)
        shardLayout.remove(startKey)
        assert(shards.lastKey() <= currVersion)
        shards.put(currVersion, shardLayout)

        for (i <- 0 to buf.size by splitKeyCount) {
          val endKey: ByteArrayWrapper =
            if (i + splitKeyCount > buf.size) nextEndKey //is last, take boundary from splited shard
            else buf(i + splitKeyCount)._1 //is not last, take first key from next shard

          shardAdd(cutOffKey = startKey, endKey = endKey,
            version = currVersion, versionID = currVersionID)
          val untilIndex = Math.min(buf.size, i + splitKeyCount)
          shardLayout.get(startKey)
            .merge(
              version = currVersion,
              versionID = currVersionID,
              data = buf.slice(i, untilIndex))
          //first key of next shard (if is not last)
          startKey = endKey
        }
        if (keepSingleVersion) {
          log.close()
          log.deleteAllFiles()
        }

      }else{
        //merge everything into same log
        log.merge(version = currVersion, versionID = currVersionID, data = buf)
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

  protected[iodb] def shardAdd(cutOffKey: K, endKey: K, versionID: VersionID, version: Long,
                               filePrefix: String = "shardBuf-" + shardIdSeq.incrementAndGet() + "-") = {
    lock.writeLock().lock()
    try {
      val log = new LogStore(dir = dir, filePrefix = filePrefix,
        keySize = keySize, fileLocks = fileLocks, keepSingleVersion = keepSingleVersion,
        fileSync = false, useUnsafe = useUnsafe)

      var currShards = shards.get(version);
      if (currShards == null) {
        currShards = new ConcurrentSkipListMap()
        shards.put(version, currShards)
      }
      val old = currShards.put(cutOffKey, log)
      //      assert(old == null)

      //create shard info
      val shardInfoFile = new File(dir, log.filePrefix + "." + Utils.shardInfoFileExt)
      val shardInfo = ShardInfo(startKey = cutOffKey, endKey = endKey, startVersionID = versionID, startVersion = version, keySize = keySize)
      shardInfo.save(shardInfoFile)
    } finally {
      lock.writeLock().unlock()
    }
  }


  protected[iodb] def loadShardInfo(f: File): ShardInfo = {
    val in = new DataInputStream(new FileInputStream(f))
    if (keySize != in.readInt())
      throw new DataCorruptionException("Wrong key size in shard");

    val startVersion = in.readLong()
    val startVersionId = new ByteArrayWrapper(in.readInt())
    in.readFully(startVersionId.data)
    val isLastShard = in.readBoolean()
    val startKey = new K(keySize)
    in.readFully(startKey.data)
    val endKey =
      if (isLastShard) null
      else {
        val e = new K(keySize)
        in.readFully(e.data)
        e
      }

    ShardInfo(startKey = startKey, endKey = endKey, startVersionID = startVersionId, startVersion = startVersion, keySize = keySize)
  }

  /** check structure, shard boundaries, versions... and throw an exception if data are corrupted */
  def verify(): Unit = {
    for ((version, shardLayout) <- shards.asScala) {
      for ((cutOfKey, log) <- shardLayout.asScala) {
        val shardInfo = loadShardInfo(new File(log.dir, log.filePrefix + "." + Utils.shardInfoFileExt))
        for (logVersion <- log.versions) {
          //TODO verify new version id
          //          assert(logVersion >= shardInfo.startVersionId)
          for ((key, value) <- log.versionIterator(logVersion)) {
            assert(key.compareTo(shardInfo.startKey) >= 0)
            assert(shardInfo.endKey == null || key.compareTo(shardInfo.endKey) < 0)
          }
        }
      }
    }
  }

}


protected[iodb] case class ShardInfo(
                                      startKey: K, endKey: K, startVersionID: VersionID, startVersion: Long, keySize: Int) {

  def save(f: File): Unit = {
    val out = new FileOutputStream(f);
    val out2 = new DataOutputStream(out);
    out2.writeInt(keySize)
    out2.writeLong(startVersion)
    out2.writeInt(startVersionID.data.size)
    out2.write(startVersionID.data)
    out2.writeBoolean(isLastShard)
    out2.write(startKey.data)
    if (!isLastShard)
      out2.write(endKey.data)
    out2.flush()
    out.getFD.sync()
    out.close()
  }

  def isLastShard = endKey == null
}

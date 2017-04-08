package io.iohk.iodb

import java.io._
import java.nio.ByteBuffer
import java.util
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.logging.Level
import java.util.{Comparator, NoSuchElementException}

import com.google.common.collect.Iterators
import io.iohk.iodb.Store._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


protected[iodb] object LSMStore {

  val fileJournalPrefix = "journal"
  val fileShardPrefix = "shard-"
  val updateHeaderSize = +8 + 4 + 4 + 4 + 4 + 4 + 1

  val shardLayoutLog = "shardLayoutLog"

  type ShardLayout = util.TreeMap[K, List[LogFileUpdate]]
}

import io.iohk.iodb.LSMStore._
/**
  * Log-Structured Merge Store
  */
class LSMStore(
                val dir: File,
                val keySize: Int = 32,
                val executor: Executor = Executors.newCachedThreadPool(),
                val maxJournalEntryCount: Int = 100000,
                val maxShardUnmergedCount: Int = 4,
                val splitSize: Int = 100 * 1024,
                val keepVersions: Int = 0,
                val maxFileSize: Long = 64 * 1024 * 1024,

                val fileAccess: FileAccess = FileAccess.SAFE
              ) extends Store {


  val lock = new ReentrantReadWriteLock()

  protected[iodb] var journalDirty: List[LogFileUpdate] = Nil
  protected[iodb] val journalCache = new util.TreeMap[K, V]
  protected[iodb] var journalLastVersionID: Option[VersionID] = None

  //TODO mutable buffer, or queue?
  protected[iodb] var journalRollback: List[LogFileUpdate] = Nil

  protected[iodb] var shardLayoutLog: FileOutputStream = null

  protected[iodb] val fileHandles = mutable.HashMap[Long, Any]()
  protected[iodb] val fileOuts = mutable.HashMap[Long, FileOutputStream]()

  protected[iodb] var shards = new ShardLayout()

  protected[iodb] val shardRollback =
    new mutable.LinkedHashMap[VersionID, ShardLayout]

  {
    assert(maxFileSize < 1024 * 1024 * 1024, "maximal file size must be < 1GB")

    //some initialization logic
    if (!dir.exists() || !dir.isDirectory)
      throw new IllegalArgumentException("dir does not exist, or is not an directory")

    // load all shard linked lists
    val shardNums = listFiles()
      .filter(isShardFile(_))
      .map(shardFileToNum(_))

    val journalNumbers = listFiles()
      .filter(isJournalFile(_))
      .map(journalFileToNum(_)) //add file number
      .sorted

    //open journal file handles
    journalNumbers.foreach { p =>
      val f = numToFile(p)
      fileHandles(p) = fileAccess.open(f.getPath)
      fileOuts(p) = new FileOutputStream(f, true)
    }

    //open shard file handles
    shardNums.foreach { fileNum =>
      val f = numToFile(fileNum)
      fileHandles(fileNum) = fileAccess.open(f.getPath)
      //TODO verify file size matches update log, last update in file could be corrupted
      fileOuts(fileNum) = new FileOutputStream(f, true)
    }

    //replay Shard Layout Log and restore shards
    val slogFile = new File(dir, LSMStore.shardLayoutLog)
    if (slogFile.exists()) {
      val slogInF = new FileInputStream(slogFile)
      val slogIn = new DataInputStream(new BufferedInputStream(slogInF))

      val slog = new ArrayBuffer[ShardSpec]()
      while (slogIn.available() > 0) {
        slog += deserializeShardSpec(slogIn)
      }
      slogIn.close()

      //restore shard layouts
      for (s <- slog;
           //check all files for this shard exists
           if (s.shards.map(_.fileNum).forall(fileHandles.contains(_)))
      ) {
        val t = new ShardLayout()
        for (e <- s.shards) {
          val updates = loadUpdates(numToFile(e.fileNum), e.fileNum)
          val linked = linkTogether(versionID = e.versionID, updates)
          t.put(e.startKey, linked)
        }
        shardRollback.put(s.versionID, t)
      }


      if (shardRollback.size > 0)
        shards = shardRollback.last._2.clone().asInstanceOf[ShardLayout]
    }

    val j = loadJournal(journalNumbers.sorted.reverse)
    j.headOption.foreach { h => assert(h.versionID != h.prevVersionID) }
    journalLastVersionID = j.headOption.map(_.versionID)

    //find newest entry in journal present which is also present in shard
    //and cut journal, so it does not contain old entries
    journalDirty = j.takeWhile(u => !shardRollback.contains(u.versionID)).toList
    //restore journal cache
    keyValues(journalDirty, dropTombstones = false)
      .foreach(a => journalCache.put(a._1, a._2))

    journalRollback = j.toList

    if (journalRollback.size > 0)
      taskCleanup(deleteFiles = false)
  }


  protected[iodb] def initShardLayoutLog(): Unit = {
    lock.writeLock().lock()
    try {
      if (shardLayoutLog != null)
        return
      val f = new File(dir, LSMStore.shardLayoutLog)
      shardLayoutLog = new FileOutputStream(f, true)
    } finally {
      lock.writeLock().unlock()
    }
  }

  /** read files and reconstructs linked list of updates */
  protected[iodb] def loadJournal(files: Iterable[Long]): mutable.Buffer[LogFileUpdate] = {
    //read files into structure
    val updates = files
      .flatMap(fileNum => loadUpdates(file = numToFile(fileNum), fileNum = fileNum))
      .toSeq

    //add some indexes
    val cur = updates
      .filter(u => u.versionID != u.prevVersionID) //do not include marker entries into index
      .map(u => (u.versionID, u)).toMap

    //start from last update and reconstruct list
    val list = new ArrayBuffer[LogFileUpdate]
    var b = updates.lastOption.map(u => cur(u.versionID))
    while (b != None) {
      list += b.get
      b = cur.get(b.get.prevVersionID)
    }

    return list
  }

  protected[iodb] def journalDeleteFilesButNewest(): Unit = {
    val knownNums = journalDirty.map(_.fileNum).toSet
    val filesToDelete = journalListSortedFiles()
      .drop(1) //do not delete first file
      .filterNot(f => knownNums.contains(journalFileToNum(f))) //do not delete known files
    filesToDelete.foreach { f =>
      val deleted = f.delete()
      assert(deleted)
    }
  }

  /** creates new journal file, if no file is opened */
  protected def initJournal(): Unit = {
    assert(lock.isWriteLockedByCurrentThread)
    if (journalDirty != Nil)
      return;

    //create new journal
    journalStartNewFile()
    journalDirty = Nil
  }

  protected[iodb] def journalStartNewFile(): Long = {
    assert(lock.isWriteLockedByCurrentThread)

    val fileNum = journalNewFileNum()
    val journalFile = numToFile(fileNum)
    assert(!(journalFile.exists()))

    val out = new FileOutputStream(journalFile, false)
    val fileHandle = fileAccess.open(journalFile.getPath)
    fileOuts(fileNum) = out
    fileHandles(fileNum) = fileHandle

    return fileNum
  }

  protected[iodb] def loadUpdates(file: File, fileNum: Long): Iterable[LogFileUpdate] = {
    val updates = new ArrayBuffer[LogFileUpdate]()

    val fin = new FileInputStream(file)
    val din = new DataInputStream(new BufferedInputStream(fin))
    var offset = 0

    while (offset < fin.getChannel.size()) {
      //verify checksum
      val checksum = din.readLong()

      //read data
      val updateSize = din.readInt()
      val data = new Array[Byte](updateSize)
      val buf = ByteBuffer.wrap(data)
      din.read(data, 8 + 4, updateSize - 8 - 4)
      //put data size back so checksum is preserved
      buf.putInt(8, updateSize)

      //verify checksum
      if (checksum != Utils.checksum(data)) {
        throw new DataCorruptionException("Wrong data checksum")
      }

      //read data from byte buffer
      buf.position(12)
      val keyCount = buf.getInt()
      val keySize2 = buf.getInt()
      assert(keySize2 == keySize)
      val versionIDSize = buf.getInt()
      val prevVersionIDSize = buf.getInt()

      val versionID = new ByteArrayWrapper(versionIDSize)
      val prevVersionID = new ByteArrayWrapper(prevVersionIDSize)
      val isMerged = (buf.get() == 1)

      val verPos = LSMStore.updateHeaderSize + keySize * keyCount + 8 * keyCount
      if (versionIDSize > 0)
        System.arraycopy(data, verPos, versionID.data, 0, versionIDSize)
      if (prevVersionIDSize > 0)
        System.arraycopy(data, verPos + versionIDSize, prevVersionID.data, 0, prevVersionIDSize)

      updates += new LogFileUpdate(
        versionID = versionID,
        prevVersionID = prevVersionID,
        merged = isMerged,
        fileNum = fileNum,
        offset = offset,
        keyCount = keyCount)
      offset += updateSize
    }
    din.close()
    return updates
  }


  protected[iodb] def isJournalFile(f: File): Boolean =
    f.getName.matches(LSMStore.fileJournalPrefix + "-[0-9]+")

  protected[iodb] def journalFileToNum(f: File): Long =
    f.getName().substring(LSMStore.fileJournalPrefix.size).toLong

  protected[iodb] def numToFile(n: Long): File = {
    val prefix = if (n < 0) LSMStore.fileJournalPrefix else LSMStore.fileShardPrefix
    new File(dir, prefix + n)
  }

  protected[iodb] def journalListSortedFiles(): Seq[File] = {
    listFiles()
      .filter(isJournalFile(_))
      .sortBy(journalFileToNum(_))
  }

  protected[iodb] def journalNewFileNum(): Long = journalListSortedFiles()
    .headOption.map(journalFileToNum(_))
    .getOrElse(0L) - 1L

  protected def journalCurrentFileNum(): Long = fileOuts.keys.min

  protected[iodb] def shardFileToNum(f: File): Long =
    f.getName().substring(LSMStore.fileShardPrefix.size).toLong


  protected[iodb] def isShardFile(f: File): Boolean =
    f.getName.matches(LSMStore.fileShardPrefix + "[0-9]+")

  protected def listFiles(): Array[File] = {
    var files: Array[File] = dir.listFiles()
    if (files == null)
      files = new Array[File](0)
    return files
  }

  protected[iodb] def shardListSortedFiles(): Seq[File] = {
    listFiles()
      .filter(isShardFile(_))
      .sortBy(shardFileToNum(_))
      .reverse
  }

  protected[iodb] def shardNewFileNum(): Long = shardListSortedFiles()
    .headOption.map(shardFileToNum(_))
    .getOrElse(0L) + 1


  /** expand shard tails into linked lists */
  protected[iodb] def linkTogether(versionID: VersionID, updates: Iterable[LogFileUpdate]): List[LogFileUpdate] = {
    //index to find previous versions
    val m = updates.map { u => (u.versionID, u) }.toMap

    var ret = new ArrayBuffer[LogFileUpdate]()
    var r = m.get(versionID)
    while (r != None) {
      ret += r.get
      r = m.get(r.get.prevVersionID)
    }

    return ret.toList
  }

  protected[iodb] def createEmptyShard(): Long = {
    val fileNum = shardNewFileNum()
    val shardFile = new File(dir, LSMStore.fileShardPrefix + fileNum)
    assert(!(shardFile.exists()))
    val out = new FileOutputStream(shardFile)
    val fileHandle = fileAccess.open(shardFile.getPath)
    fileOuts(fileNum) = out
    fileHandles(fileNum) = fileHandle
    return fileNum
  }

  def versionIDExists(versionID: VersionID): Boolean = {
    //TODO traverse all files, not just open files
    if (journalRollback.find(u => u.versionID == versionID || u.prevVersionID == versionID) != None)
      return true

    if (shards.values().asScala.flatMap(a => a)
      .find(u => u.versionID == versionID || u.prevVersionID == versionID) != None)
      return true
    return false
  }

  def update(
              versionID: VersionID,
              toRemove: Iterable[K],
              toUpdate: Iterable[(K, V)]
            ): Unit = {
    lock.writeLock().lock()
    try {
      if (versionIDExists(versionID))
        throw new IllegalArgumentException("versionID is already used")

      initJournal()

      //last version from journal
      val prevVersionID = journalLastVersionID.getOrElse(new ByteArrayWrapper(0))

      var journalFileNum = journalCurrentFileNum()
      //if journal file is too big, start new file
      if (numToFile(journalFileNum).length() > maxFileSize) {
        //start a new file
        //        fileOuts.remove(journalFileNum).get.close()
        journalFileNum = journalStartNewFile()
        val file = numToFile(journalFileNum)
        val out = new FileOutputStream(file)
        fileOuts(journalFileNum) = out
        fileHandles(journalFileNum) = fileAccess.open(file.getPath())
      }

      val updateEntry = updateAppend(
        fileNum = journalFileNum,
        toRemove = toRemove,
        toUpdate = toUpdate,
        versionID = versionID,
        prevVersionID = prevVersionID,
        merged = false)

      journalDirty = updateEntry :: journalDirty
      journalRollback = updateEntry :: journalRollback
      journalLastVersionID = Some(versionID)

      //update journal cache
      for (key <- toRemove) {
        journalCache.put(key, Store.tombstone)
      }
      for ((key, value) <- toUpdate) {
        journalCache.put(key, value)
      }

      //TODO if write fails, how to recover from this? close journal.out?

      if (journalCache.size() > maxJournalEntryCount) {
        //run sharding task
        taskRun {
          taskDistribute()
        }
      }
      taskCleanup()
    } finally {
      lock.writeLock().unlock()
    }
  }

  protected[iodb] def updateAppend(
                                    fileNum: Long,
                                    toRemove: Iterable[K],
                                    toUpdate: Iterable[(K, V)],
                                    versionID: VersionID,
                                    prevVersionID: V,
                                    merged: Boolean
                                  ): LogFileUpdate = {
    //insert new Update into journal
    val updateData = serializeUpdate(
      versionID = versionID,
      prevVersionID = prevVersionID,
      toRemove = toRemove,
      toUpdate = toUpdate,
      isMerged = merged)

    val out = fileOuts(fileNum)

    val updateOffset = out.getChannel.position()
    out.write(updateData)
    out.getFD.sync()

    //update file size
    val oldHandle = fileHandles(fileNum)
    val newHandle = fileAccess.expandFileSize(oldHandle)
    if (oldHandle != newHandle) {
      fileHandles.put(fileNum, newHandle)
    }
    //append new entry to journal
    return new LogFileUpdate(
      offset = updateOffset.toInt,
      keyCount = toRemove.size + toUpdate.size,
      merged = merged,
      fileNum = fileNum,
      versionID = versionID,
      prevVersionID = prevVersionID
    )

  }

  def taskRun(f: => Unit) {
    if (executor == null) {
      // execute in foreground
      f
      return
    }
    //send task to executor for background execution
    val runnable = new Runnable() {
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
    executor.execute(runnable)
  }


  protected[iodb] def serializeShardSpec(
                                          versionID: VersionID,
                                          shards: Seq[(K, Long, VersionID)]
                                        ): Array[Byte] = {

    assert(shards.size > 0)
    val out = new ByteArrayOutputStream()
    val out2 = new DataOutputStream(out)

    //placeholder for `checksum` and `update size`
    out2.writeLong(0)
    out2.writeInt(0)

    out2.writeInt(shards.size)
    out2.writeInt(keySize)
    out2.writeInt(versionID.size)
    out2.write(versionID.data)

    for (s <- shards) {
      out2.write(s._1.data)
      out2.writeLong(s._2)
      //versionID
      out2.writeInt(s._3.size)
      out2.write(s._3.data)
    }

    //now write file size and checksum
    val ret = out.toByteArray
    val wrap = ByteBuffer.wrap(ret)
    wrap.putInt(8, ret.size)
    val checksum = Utils.checksum(ret) - 1000 // 1000 is to distinguish different type of data
    wrap.putLong(0, checksum)
    ret
  }


  protected[iodb] def deserializeShardSpec(in: DataInputStream): ShardSpec = {
    val checksum = in.readLong()
    val updateSize = in.readInt()
    val b = new Array[Byte](updateSize)
    in.read(b, 12, updateSize - 12)
    //restore size
    Utils.putInt(b, 8, updateSize)
    //validate checksum
    if (Utils.checksum(b) - 1000 != checksum) {
      throw new DataCorruptionException("Wrong data checksum")
    }

    val in2 = new DataInputStream(new ByteArrayInputStream(b))
    in2.readLong() //skip checksum
    in2.readInt()
    // skip updateSize
    val keyCount = in2.readInt()
    assert(keySize == in2.readInt())

    val versionID = new VersionID(in2.readInt())
    in2.read(versionID.data)
    //read table
    val keyFileNums = (0 until keyCount).map { index =>
      val key = new K(keySize)
      in2.read(key.data)
      val fileNum = in2.readLong()
      val versionID = new ByteArrayWrapper(in2.readInt())
      in2.read(versionID.data)

      (key, fileNum, versionID)
    }

    val specs = (0 until keyCount).map { i =>
      ShardSpecEntry(
        startKey = keyFileNums(i)._1,
        endKey = if (i + 1 == keyCount) null else keyFileNums(i + 1)._1,
        fileNum = keyFileNums(i)._2,
        versionID = keyFileNums(i)._3
      )
    }

    return new ShardSpec(versionID = versionID, shards = specs)
  }

  protected[iodb] def serializeUpdate(
                                        versionID: VersionID,
                                        prevVersionID: VersionID,
                                        toRemove: Iterable[K],
                                        toUpdate: Iterable[(K, V)],
                                        isMerged: Boolean
                                      ): Array[Byte] = {

    //TODO check total size is <2GB

    //merge toRemove and toUpdate into sorted set
    val sorted: mutable.Buffer[Tuple2[ByteArrayWrapper, ByteArrayWrapper]] =
      (toRemove.map(k => (k, null)) ++ toUpdate)
        .toBuffer
        .sortBy(_._1)

    //check for duplicates
    assert(sorted.size == toRemove.size + toUpdate.size, "duplicate key")
    assert(sorted.map(_._1).toSet.size == sorted.size, "duplicate key")
    //check for key size
    assert((versionID == tombstone && prevVersionID == tombstone) ||
      sorted.forall(_._1.size == keySize), "wrong key size")

    val out = new ByteArrayOutputStream()
    val out2 = new DataOutputStream(out)

    //placeholder for `checksum` and `update size`
    out2.writeLong(0)
    out2.writeInt(0)

    //basic data
    out2.writeInt(sorted.size) // number of keys
    out2.writeInt(keySize) // key size

    //versions
    out2.writeInt(versionID.size)
    out2.writeInt(prevVersionID.size)

    out2.writeBoolean(isMerged) //is merged

    //write keys
    sorted.map(_._1.data).foreach(out2.write(_))

    var valueOffset = out.size() + sorted.size * 8 + versionID.size + prevVersionID.size

    //write value sizes and their offsets
    sorted.foreach { t =>
      val value = t._2
      if (value == null) {
        //tombstone
        out2.writeInt(-1)
        out2.writeInt(0)
      } else {
        //actual data
        out2.writeInt(value.size)
        out2.writeInt(valueOffset)
        valueOffset += value.size
      }
    }

    out2.write(versionID.data)
    out2.write(prevVersionID.data)

    //write values
    sorted.foreach { t =>
      if (t._2 != null) //filter out tombstones
        out2.write(t._2.data)
    }

    //write checksum and size at beginning of byte[]
    val ret = out.toByteArray
    val wrap = ByteBuffer.wrap(ret)
    wrap.putInt(8, ret.size)
    wrap.putLong(0, Utils.checksum(ret))
    ret
  }

  override def lastVersionID: Option[VersionID] = {
    lock.readLock().lock()
    try {
      return journalLastVersionID
    } finally {
      lock.readLock().unlock()
    }
  }


  def getUpdates(key: K, logFile: List[LogFileUpdate]): Option[V] = {
    var updates = logFile
    while (updates != null && !updates.isEmpty) {
      //run binary search on current item
      val value = fileAccess.getValue(
        fileHandle = fileHandles(updates.head.fileNum),
        key = key,
        keySize = keySize,
        updateOffset = updates.head.offset)
      if (value != null)
        return value;

      if (updates.head.merged)
        return null // this update is merged, no need to continue traversal

      updates = updates.tail
    }

    return null
  }

  def get(key: K): Option[V] = {
    lock.readLock().lock()
    try {
      val ret2 = journalCache.get(key)
      if (ret2 eq tombstone)
        return None
      if (ret2 != null)
        return Some(ret2)

      //      var ret = getUpdates(key, journal)
      //      if(ret!=null)
      //        return ret

      //not found in journal, look at shards
      val shardEntry = shards.floorEntry(key)
      if (shardEntry == null)
        return None // shards not initialized yet

      val ret = getUpdates(key, shardEntry.getValue)
      if (ret != null)
        return ret

      // not found
      return None
    } finally {
      lock.readLock().unlock()
    }
  }

  /**
    * Task ran periodically in background thread.
    * It distributes content of Journal between Shards.
    */
  def taskDistribute(): Unit = {
    lock.writeLock().lock()
    try {
      val versionID = lastVersionID.getOrElse(null)
      if (versionID == null)
        return // empty store
      if (journalCache.isEmpty)
        return

      Utils.LOG.log(Level.FINE, "Run Sharding Task for " + journalCache.size() + " keys.")

      val groupByShard = journalCache.asScala.groupBy { a => shards.floorKey(a._1) }

      for ((shardKey, entries) <- groupByShard) {
        val toRemove: Iterable[K] = entries.filter(_._2 eq Store.tombstone).map(_._1)
        val toUpdate: Iterable[(K, V)] = entries.filterNot(_._2 eq Store.tombstone)

        var fileNum = 0L
        var logFile: List[LogFileUpdate] = Nil
        var shardKey2 = shardKey
        if (shardKey == null) {
          //shard map is empty, create new shard
          fileNum = createEmptyShard()
          logFile = Nil
          shardKey2 = new ByteArrayWrapper(keySize) //lowest possible key
        } else {
          //use existing shard
          logFile = shards.get(shardKey)
          fileNum = logFile.head.fileNum
        }

        //insert new update into Shard
        val updateEntry = updateAppend(fileNum,
          toRemove = toRemove, toUpdate = toUpdate,
          merged = logFile.isEmpty,
          versionID = versionID,
          prevVersionID = logFile.headOption.map(_.versionID).getOrElse(tombstone)
        )
        logFile = updateEntry :: logFile
        shards.put(shardKey2, logFile)

        //schedule merge on this Shard, if it has enough unmerged changes
        val unmergedCount = logFile.takeWhile(!_.merged).size
        if (unmergedCount > maxShardUnmergedCount)
          taskRun {
            taskShardMerge(shardKey)
          }
      }
      journalCache.clear()
      journalDirty = Nil

      //backup current shard layout
      shardRollback.put(versionID, shards.clone().asInstanceOf[ShardLayout])

      initShardLayoutLog()
      //update shard layout log
      //write first entry into shard layout log
      val data = serializeShardSpec(versionID = versionID,
        shards = shards.asScala.map(u => (u._1, u._2.head.fileNum, u._2.head.versionID)).toSeq)
      shardLayoutLog.write(data)
      shardLayoutLog.getFD.sync()

      taskCleanup()
    } finally {
      lock.writeLock().unlock()
    }
  }


  // takes first N items from iterator,
  // Iterator.take() can not be used, read scaladoc
  private def take(count: Int, iter: Iterator[(K, V)]): Iterable[(K, V)] = {
    val ret = new ArrayBuffer[(K, V)]()
    var counter = 0;
    while (counter < count && iter.hasNext) {
      counter += 1
      ret += iter.next()
    }
    return ret;
  }


  def taskShardMerge(shardKey: K): Unit = {
    lock.writeLock().lock()
    try {
      val shard = shards.get(shardKey)
      if (shard == null)
        return // shard not present

      if (shard.head.merged)
        return // shard is empty or already merged

      Utils.LOG.log(Level.FINE, "Starting sharding for shard: " + shardKey)

      //iterator over merged data set
      val b = keyValues(shard, dropTombstones = true).toBuffer
      val merged = b.iterator

      // decide if this will be put into single shard, or will be split between multiple shards
      // Load on-heap enough elements to fill single shard
      var mergedFirstShard = take(splitSize, merged)

      // Decide if all data go into single shard.
      if (!merged.hasNext) {
        // Data are small enough, keep everything in single shard
        val updateEntry = updateAppend(
          fileNum = createEmptyShard(),
          versionID = shard.head.versionID,
          prevVersionID = Store.tombstone,
          toUpdate = mergedFirstShard,
          toRemove = Nil,
          merged = true)
        shards.put(shardKey, List(updateEntry))
        Utils.LOG.log(Level.FINE, "Sharding finished into single shard")
      } else {
        // split data into multiple shards
        val mergedBuf: BufferedIterator[(K, V)] = (mergedFirstShard.iterator ++ merged).buffered
        mergedFirstShard = null
        //release for GC
        val splitSize2: Int = Math.max(1, (splitSize.toDouble * 0.33).toInt)
        val parentShardEndKey = shards.higherKey(shardKey)

        var startKey: K = shardKey
        var shardCounter = 0
        while (mergedBuf.hasNext) {
          val keyVals = take(splitSize2, mergedBuf)

          val endKey = if (mergedBuf.hasNext) mergedBuf.head._1 else parentShardEndKey
          assert(keyVals.map(_._1).forall(k => k >= startKey && (endKey == null || k < endKey)))

          assert(endKey == null || startKey < endKey)

          val updateEntry = updateAppend(
            fileNum = createEmptyShard(),
            versionID = shard.head.versionID,
            prevVersionID = Store.tombstone,
            toUpdate = keyVals,
            toRemove = Nil,
            merged = true
          )

          shards.put(startKey, List(updateEntry))

          startKey = endKey
          shardCounter += 1
        }
        Utils.LOG.log(Level.FINE, "Sharding finished into " + shardCounter + " shards")
      }
      taskCleanup()
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def getAll(consumer: (K, V) => Unit) = {
    lock.readLock().lock()
    try {
      //iterate over all shards, and add content not present in journal
      val shards2 = shards.values().asScala.toBuffer
      for (shard: List[LogFileUpdate] <- shards2;
           (k, v) <- keyValues(shard, dropTombstones = true)) {
        if (!journalCache.containsKey(k)) {
          consumer(k, v)
        }
      }
      //include journal cache
      for ((k, v) <- journalCache.asScala.filterNot(a => a._2 eq tombstone)) {
        consumer(k, v)
      }
    } finally {
      lock.readLock().unlock()
    }
  }

  override def clean(count: Int): Unit = {
    taskCleanup()
  }

  override def cleanStop(): Unit = {

  }

  override def rollback(versionID: VersionID): Unit = {
    def notFound() = throw new NoSuchElementException("versionID not found, can not rollback")

    lock.writeLock().lock()
    try {
      if (journalRollback.isEmpty)
        notFound()

      journalRollback.find(_.versionID == versionID).getOrElse(notFound())

      if (journalDirty != Nil && journalDirty.head.versionID == versionID)
        return

      //cut journal
      var notFound2 = true;
      var lastShard: ShardLayout = null
      journalRollback = journalRollback
        //find starting version
        .dropWhile { u =>
        notFound2 = u.versionID != versionID
        notFound2
      }
      //find end version, where journal was sharded, also restore Shard Layout in side effect
      val j = journalRollback.takeWhile { u =>
        lastShard = shardRollback.get(u.versionID).map(_.clone().asInstanceOf[ShardLayout]).getOrElse(null)
        lastShard == null
      }

      if (notFound2)
        notFound()

      if (lastShard == null) {
        //Reached end of journalRollback, without finding shard layout
        //Check if journal log was not truncated
        if (journalRollback.last.prevVersionID != tombstone)
          notFound()
        shards.clear()
      } else {
        shards = lastShard
      }
      //rebuild journal cache
      journalCache.clear()

      keyValues(j, dropTombstones = false).foreach { e =>
        journalCache.put(e._1, e._2)
      }
      journalLastVersionID = Some(versionID)
      journalDirty = j

      //insert new marker update to journal, so when reopened we start with this version
      val updateEntry = updateAppend(
        fileNum = journalCurrentFileNum(),
        toRemove = Nil,
        toUpdate = Nil,
        versionID = versionID,
        prevVersionID = versionID,
        merged = false)

      taskCleanup()
    } finally {
      lock.writeLock().unlock()
    }
  }


  override def close(): Unit = {
    lock.writeLock().lock()
    try {
      fileOuts.values.foreach(_.close())
      fileHandles.values.foreach(fileAccess.close(_))
      fileOuts.clear()
      fileHandles.clear()
      if (shardLayoutLog != null)
        shardLayoutLog.close()
      taskCleanup()
    } finally {
      lock.writeLock().unlock()
    }
  }


  protected[iodb] def keyValues(fileLog: List[LogFileUpdate], dropTombstones: Boolean): Iterator[(K, V)] = {
    //assert that all updates except last are merged

    val iters = fileLog.zipWithIndex.map { a =>
      val u = a._1
      val index = a._2
      fileAccess.readKeyValues(fileHandle = fileHandles(u.fileNum),
        offset = u.offset, keySize = keySize)
        .map(p => (p._1, index, p._2)).asJava
    }.asJava.asInstanceOf[java.lang.Iterable[util.Iterator[(K, Int, V)]]]

    //merge multiple iterators, result iterator is sorted union of all iters
    var prevKey: K = null
    val iter = Iterators.mergeSorted[(K, Int, V)](iters, KeyOffsetValueComparator)
      .asScala
      .filter { it =>
        val include =
          (prevKey == null || //first key
            !prevKey.equals(it._1)) && //is first version of this key
            (!dropTombstones || it._3 != null) // only include if is not tombstone
        prevKey = it._1
        include
      }
      //drop the tombstones
      .filter(it => !dropTombstones || it._3 != Store.tombstone)
      //remove update
      .map(it => (it._1, it._3))

    return iter
  }

  def verify(): Unit = {
    //verify shard boundaries
    (List(shards) ++ shardRollback.values).foreach { s =>
      s.asScala.foldRight(null.asInstanceOf[K]) { (e: Tuple2[K, List[LogFileUpdate]], endKey: K) =>
        val fileNum = e._2.head.fileNum
        assert(fileHandles.contains(fileNum))
        val updates = loadUpdates(file = numToFile(fileNum), fileNum = fileNum)

        e._1
      }
    }

    val existFiles =
      (listFiles().filter(isJournalFile(_)).map(journalFileToNum(_)) ++
        listFiles().filter(isShardFile(_)).map(shardFileToNum(_))).toSet

    assert(existFiles == fileHandles.keySet)
    assert(existFiles == fileOuts.keySet)
  }


  def takeWhileExtra[A](iter: Iterable[A], f: A => Boolean): Iterable[A] = {
    val ret = new ArrayBuffer[A]
    val it = iter.iterator
    while (it.hasNext) {
      val x = it.next()
      ret += x
      if (!f(x))
        return ret.result()
    }
    ret.result()
  }

  /** closes and deletes files which are no longer needed */
  def taskCleanup(deleteFiles: Boolean = true): Unit = {
    lock.writeLock().lock()
    try {
      if (fileOuts.isEmpty)
        return
      //this updates will be preserved
      var index = Math.max(keepVersions, journalDirty.size)
      //expand index until we find shard or reach end
      while (index < journalRollback.size && !shardRollback.contains(journalRollback(index).versionID)) {
        index += 1
      }
      index += 1

      journalRollback = journalRollback.take(index)

      //build set of files to preserve
      val journalPreserveNums: Set[Long] = (journalRollback.map(_.fileNum) ++ List(journalCurrentFileNum())).toSet
      assert(journalPreserveNums.contains(journalCurrentFileNum()))


      //shards to preserve
      val shardNumsToPreserve: Set[Long] =
        (List(shards) ++ journalRollback.flatMap(u => shardRollback.get(u.versionID)))
          .flatMap(_.values().asScala)
          .flatMap(_.map(_.fileNum))
          .toSet

      val numsToPreserve = (journalPreserveNums ++ shardNumsToPreserve)
      assert(numsToPreserve.forall(fileHandles.contains(_)))
      assert(numsToPreserve.forall(fileOuts.contains(_)))

      val deleteNums = (fileHandles.keySet diff numsToPreserve)

      //delete unused journal files
      for (num <- deleteNums) {
        fileAccess.close(fileHandles.remove(num).get)
        fileOuts.remove(num).get.close()
        if (deleteFiles) {
          val f = numToFile(num)
          assert(f.exists())
          val deleted = f.delete()
          assert(deleted)
        }
      }

      //cut unused shards
      val versionsInJournalRollback = journalRollback.map(_.versionID).toSet
      (shardRollback.keySet diff versionsInJournalRollback).foreach(shardRollback.remove(_))

      //      if(!shardRollback.isEmpty)
      //        journalRollback.lastOption.foreach(u=>assert(shardRollback.contains(u.versionID)))

    } finally {
      lock.writeLock().unlock()
    }
  }

  def rollbackVersions(): Iterable[VersionID] = journalRollback.map(_.versionID)

}

/** Compares key-value pairs. Key is used for comparation, value is ignored */
protected[iodb] object KeyOffsetValueComparator extends Comparator[(K, Int, V)] {
  def compare(o1: (K, Int, V),
              o2: (K, Int, V)): Int = {
    //compare by key
    var c = o1._1.compareTo(o2._1)
    //compare by index, higher index means newer entry
    if (c == 0)
      c = o1._2.compareTo(o2._2)
    return c
  }
}


//TODO this consumes too much memory, should be represented using single primitive long[]
case class LogFileUpdate(
                          offset: Int,
                          keyCount: Int,
                          merged: Boolean,
                          fileNum: Long,
                          versionID: VersionID,
                          prevVersionID: VersionID
                        ) {
}

case class ShardSpec(versionID: VersionID, shards: Seq[ShardSpecEntry])

case class ShardSpecEntry(startKey: K, endKey: K, fileNum: Long, versionID: VersionID) {
  assert((endKey == null && startKey != null) || startKey < endKey)
}
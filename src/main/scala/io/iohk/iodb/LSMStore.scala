package io.iohk.iodb

import java.io._
import java.nio.ByteBuffer
import java.util
import java.util.Comparator
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.logging.Level

import com.google.common.collect.Iterators
import io.iohk.iodb.Store._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


protected[iodb] object LSMStore {

  val fileJournalPrefix = "journal"
  val fileShardPrefix = "shard-"
  val updateHeaderSize = +8 + 4 + 4 + 4 + 4 + 4 + 1
}

/**
  * Created by jan on 18.1.17.
  */
class LSMStore(
                dir: File,
                keySize: Int = 32,
                executor: Executor = Executors.newCachedThreadPool(),
                maxJournalEntryCount: Int = 1000000,
                maxShardUnmergedCount: Int = 4,
                splitSize: Int = 1024 * 1024,
                keepVersions: Int = 0
              ) extends Store {

  val fileAccess: FileAccess = FileAccess.SAFE

  val lock = new ReentrantReadWriteLock()

  @volatile protected[iodb] var journal: List[LogFileUpdate] = Nil
  protected[iodb] val journalCache = new util.TreeMap[K, V]

  protected[iodb] val fileHandles = mutable.HashMap[Long, Any]()
  protected[iodb] val fileOuts = mutable.HashMap[Long, FileOutputStream]()

  protected[iodb] val shards = new util.TreeMap[K, List[LogFileUpdate]]()

  {
    //some initialization logic
    if (!dir.exists() || !dir.isDirectory)
      throw new IllegalArgumentException("dir does not exist, or is not an directory")

    val journalNumbers = dir.listFiles()
      .filter(isJournalFile(_))
      .map(journalFileToNum(_)) //add file number

    //open journal file handles
    journalNumbers.foreach(p => fileHandles(p) = fileAccess.open(numToFile(p).getPath))

    journal = loadJournal(journalNumbers.sorted.reverse)

    // load all shard linked lists
    val shardNums = dir.listFiles()
      .filter(isShardFile(_))
      .map(shardFileToNum(_))

    //open shard file handles
    shardNums.foreach { fileNum =>
      val f = numToFile(fileNum)
      fileHandles(fileNum) = fileAccess.open(f.getPath)

      //TODO verify file size matches update log, last update in file could be corrupted
      val out = new FileOutputStream(f, true)
      fileOuts(fileNum) = out
    }

    val shardLinks: Seq[List[LogFileUpdate]] = shardNums.flatMap { num =>
      shardExpandHeads(
        loadUpdates(file = numToFile(num), fileNum = num)
      )
    }

    //elimite older shard links
    val shardTailsVersionIDs = shardLinks.map(_.last.versionID).toSet

    //restore shards
    shardLinks
      //      .filterNot(l=> shardTailsVersionIDs.contains(l.head.versionID))
      .foreach { u =>
      val shardKey = u.head.shardStartKey
      val oldShard = shards.putIfAbsent(shardKey, u)
      //if already exists, replace merged
      if (oldShard != null && !oldShard.head.merged && u.head.merged) {
        shards.put(shardKey, u)
      }
    }

    val shardHeadVersionIDs = shardLinks.map(_.head.versionID).toSet

    //find newest entry in journal present which is also present in shard
    //and cut journal, so it does not contain old entries
    val newJournal = new ArrayBuffer[LogFileUpdate]()
    var j = journal
    while (j != Nil) {
      if (shardHeadVersionIDs.contains(j.head.versionID)) {
        j = Nil
      } else {
        newJournal += j.head
        j = j.tail
      }
    }

    journal = newJournal.toList
    //restore journal cache
    keyValues(journal, isJournal = true)
      .foreach(a => journalCache.put(a._1, a._2))

    //close journal files which are not present in current journal
    val fileNumsInJournal = journal.map(_.fileNum).toSet
    fileHandles
      .filter(_._1 < 0)
      .filterNot(u => fileNumsInJournal.contains(u._1))
      .foreach { u =>
        fileHandles.remove(u._1)
        fileAccess.close(u._2)
      }

    //open last journal file for writing
    //TODO check if end is corrupted
    journal.headOption.foreach { u =>
      val f = numToFile(u.fileNum)
      val out = new FileOutputStream(f, true)
      fileOuts.put(u.fileNum, out)
    }
  }


  /** read files and reconstructs linked list of updates */
  protected[iodb] def loadJournal(files: Iterable[Long]): List[LogFileUpdate] = {
    //read files into structure
    val updates = files
      .flatMap(fileNum => loadUpdates(file = numToFile(fileNum), fileNum = fileNum))
      .toSeq

    //add some indexes
    val cur = updates.map(u => (u.versionID, u)).toMap

    //start from last update and reconstruct list
    val list = new ArrayBuffer[LogFileUpdate]
    var b = updates.lastOption
    while (b != None) {
      list += b.get
      b = cur.get(b.get.prevVersionID)
    }

    return list.toList
  }

  protected[iodb] def journalDeleteFilesButNewest(): Unit = {
    val knownNums = journal.map(_.fileNum).toSet
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
    if (journal != Nil)
      return;

    //create new journal
    val fileNum = journalNewFileNum()
    val journalFile = numToFile(fileNum)
    assert(!(journalFile.exists()))

    val out = new FileOutputStream(journalFile)
    val fileHandle = fileAccess.open(journalFile.getPath)
    fileOuts(fileNum) = out
    fileHandles(fileNum) = fileHandle
    journal = Nil
  }

  protected[iodb] def loadUpdates(file: File, fileNum: Long): Iterable[LogFileUpdate] = {
    val updates = new ArrayBuffer[LogFileUpdate]()

    val fin = new FileInputStream(file)
    val din = new DataInputStream(new BufferedInputStream(fin))
    var shardStartKey: K = null
    var shardEndKey: K = null

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

      //verify checksum, TODO better checksum, IOException
      assert(checksum == data.sum, "broken checksum")

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

      val update = new LogFileUpdate(
        versionID = versionID,
        prevVersionID = prevVersionID,
        merged = isMerged,
        fileNum = fileNum,
        offset = offset,
        keyCount = keyCount,
        shardStartKey = shardStartKey,
        shardEndKey = shardEndKey
      )

      //first entry in shard contains info about start/end key, in that case do not load
      if (offset == 0 && keyCount <= 2 && versionID == tombstone && prevVersionID == tombstone) {
        //read start and end key
        shardStartKey = new ByteArrayWrapper(keySize)
        System.arraycopy(data, LSMStore.updateHeaderSize, shardStartKey.data, 0, keySize)
        if (keyCount == 1) {
          shardEndKey = null
        } else {
          shardEndKey = new ByteArrayWrapper(keySize)
          System.arraycopy(data, LSMStore.updateHeaderSize + keySize, shardEndKey.data, 0, keySize)
        }
      } else {
        updates += update
      }
      offset += updateSize
    }
    din.close()
    return updates
  }

  //
  //  protected def initShards(): Unit = {
  //    assert(lock.isWriteLockedByCurrentThread)
  //    if(!shards.isEmpty)
  //      return
  //
  //    val shard = createEmptyShard()
  //
  //    val lowestKey = new ByteArrayWrapper(keySize)
  //    shards.put(lowestKey, shard)
  //    lastShardingVersionID=Store.tombstone
  //  }


  protected[iodb] def isJournalFile(f: File): Boolean =
    f.getName.matches(LSMStore.fileJournalPrefix + "-[0-9]+")

  protected[iodb] def journalFileToNum(f: File): Long =
    f.getName().substring(LSMStore.fileJournalPrefix.size).toLong

  protected[iodb] def numToFile(n: Long): File = {
    val prefix = if (n < 0) LSMStore.fileJournalPrefix else LSMStore.fileShardPrefix
    new File(dir, prefix + n)
  }

  protected[iodb] def journalListSortedFiles(): Seq[File] = {
    dir.listFiles()
      .filter(isJournalFile(_))
      .sortBy(journalFileToNum(_))
  }

  protected[iodb] def journalNewFileNum(): Long = journalListSortedFiles()
    .headOption.map(journalFileToNum(_))
    .getOrElse(0L) - 1L

  def journalCurrentFileNum(): Long = fileOuts.keys.min

  protected[iodb] def shardFileToNum(f: File): Long =
    f.getName().substring(LSMStore.fileShardPrefix.size).toLong


  protected[iodb] def isShardFile(f: File): Boolean =
    f.getName.matches(LSMStore.fileShardPrefix + "[0-9]+")

  protected[iodb] def shardListSortedFiles(): Seq[File] = {
    dir.listFiles()
      .filter(isShardFile(_))
      .sortBy(shardFileToNum(_))
      .reverse
  }

  protected[iodb] def shardNewFileNum(): Long = shardListSortedFiles()
    .headOption.map(shardFileToNum(_))
    .getOrElse(0L) + 1


  /** find entries, which are not linked from any other entry */
  protected[iodb] def shardFindHeads(updates: Iterable[LogFileUpdate]): Seq[LogFileUpdate] = {
    val prevLinks = updates
      .map(u => (u.prevVersionID, u.shardStartKey, u.shardEndKey))
      .toSet

    val prevLinks2 = updates
      .map(u => (u.versionID, u.shardStartKey, u.shardEndKey))
      .toSet

    return updates
      .filterNot(u => prevLinks.contains(
        (u.versionID, u.shardStartKey, u.shardEndKey)
      ))
      .toBuffer
  }

  /** expand shard tails into linked lists */
  protected[iodb] def shardExpandHeads(updates: Iterable[LogFileUpdate]): Seq[List[LogFileUpdate]] = {
    //index to find previous versions
    val m = updates.map { u => ((u.versionID, u.shardStartKey, u.shardEndKey), u) }.toMap

    def buildList(u: LogFileUpdate): List[LogFileUpdate] = {
      val r = new ArrayBuffer[LogFileUpdate]()
      var a = u;
      while (a != null) {
        r += a
        a = m.getOrElse((a.prevVersionID, a.shardStartKey, a.shardEndKey), null)
      }
      return r.toList
    }

    return shardFindHeads(updates).map(buildList(_)).toBuffer
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
    //TODO traverse all files, not just open
    if (journal.find(u => u.versionID == versionID || u.prevVersionID == versionID) != None)
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
      val prevVersionID = lastVersionID().getOrElse(new ByteArrayWrapper(0))

      //TODO ensure journal file is <2GB, else start new file

      val updateEntry = updateAppend(fileNum = journalCurrentFileNum(),
        toRemove = toRemove, toUpdate = toUpdate,
        versionID = versionID, prevVersionID = prevVersionID, merged = false,
        shardStartKey = null, shardEndKey = null)

      journal = updateEntry :: journal

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
          taskSharding()
        }
      }
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
                                    merged: Boolean,
                                    shardStartKey: K,
                                    shardEndKey: K
                                  ): LogFileUpdate = {
    //insert new Update into journal
    val updateData = createUpdateData(
      versionID = versionID,
      prevVersionID = prevVersionID,
      toRemove = toRemove,
      toUpdate = toUpdate,
      isMerged = merged)

    val out = fileOuts(fileNum)

    val updateOffset = out.getChannel.position()
    out.write(updateData)
    out.getFD.sync()

    //append new entry to journal
    return new LogFileUpdate(
      offset = updateOffset.toInt,
      keyCount = toRemove.size + toUpdate.size,
      merged = merged,
      fileNum = fileNum,
      versionID = versionID,
      prevVersionID = prevVersionID,
      shardStartKey = shardStartKey,
      shardEndKey = shardEndKey
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


  protected[iodb] def createUpdateData(
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
    wrap.putLong(0, ret.sum) //TODO better checksum
    ret
  }

  override def lastVersionID(): Option[VersionID] = {
    //TODO this does not work if Journal is empty
    if (journal == Nil) None
    else journal.headOption.map(_.versionID)
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
      //run binary search from newest to oldest updates on journal
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
  def taskSharding(): Unit = {
    lock.writeLock().lock()
    try {
      val versionID = lastVersionID().getOrElse(null)
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
          //write first entry into shard with start and end keys
          updateAppend(fileNum = fileNum,
            toRemove = List(shardKey2),
            toUpdate = Nil,
            versionID = tombstone,
            prevVersionID = tombstone,
            merged = false,
            shardStartKey = shardKey2,
            shardEndKey = null)
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
          prevVersionID = logFile.headOption.map(_.versionID).getOrElse(tombstone),
          shardStartKey = shardKey2,
          shardEndKey = logFile.headOption.map(_.shardEndKey).getOrElse(null)
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

      //destroy journal files if needed
      if (keepVersions == 0) {
        fileOuts.keySet.filter(_ < 0).foreach { fileNum =>
          fileOuts.remove(fileNum).get.close()
        }
        fileHandles.keySet.filter(_ < 0).foreach { fileNum =>
          fileAccess.close(fileHandles.remove(fileNum))
        }
        journalDeleteFilesButNewest()
        journal = Nil

      }
    } finally {
      lock.writeLock().unlock()
    }
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

      val merged = keyValues(shard).toBuffer
      var mergedSliced: mutable.Buffer[mutable.Buffer[(K, V)]] =
        if (merged.size < splitSize) mutable.Buffer(merged) //no need to split shards
        else merged.grouped(splitSize * 2 / 3).toBuffer // split large shard into multiple items

      var shardKey2 = shardKey
      val parentShardEndKey = shard.head.shardEndKey
      assert(parentShardEndKey == shards.higherKey(shardKey2))

      var shard2 = if (mergedSliced.size > 1) Nil else shard
      for (i <- 0 until mergedSliced.size) {
        val keyVal = mergedSliced(i)
        val shardEndKey: K =
          if (i + 1 == mergedSliced.size) parentShardEndKey
          else mergedSliced(i + 1).head._1

        val shardFileNum = createEmptyShard()
        if (shard2 == null) {
          //start new shard at given key
          shardKey2 = keyVal.head._1
          shard2 = Nil
        }
        //write start and end keys
        updateAppend(
          fileNum = shardFileNum,
          versionID = tombstone,
          prevVersionID = tombstone,
          toUpdate = Nil,
          toRemove = if (shardEndKey == null) List(shardKey2) else List(shardKey2, shardEndKey),
          merged = false,
          shardStartKey = null,
          shardEndKey = null
        )
        assert(shardKey2 <= keyVal.head._1)
        assert(shardEndKey == null || keyVal.last._1 < shardEndKey)
        //TODO data are already sorted, updateAppend does not have to perform another sort
        val updateEntry = updateAppend(
          fileNum = shardFileNum,
          versionID = shard.head.versionID,
          prevVersionID = Store.tombstone,
          toUpdate = keyVal,
          toRemove = Nil,
          merged = true,
          shardStartKey = shardKey2,
          shardEndKey = shardEndKey)

        shards.put(shardKey2, List(updateEntry))
        shard2 = null
        shardKey2 = null
      }
      //release old file handles
      val oldShardNum = shard.head.fileNum
      fileOuts.remove(oldShardNum).get.close()
      fileAccess.close(fileHandles.remove(oldShardNum).get)
      if (keepVersions == 0) {
        val deleted = numToFile(oldShardNum).delete()
        assert(deleted, "shard file was not deleted")
      }
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def clean(count: Int): Unit = ???

  override def cleanStop(): Unit = ???

  override def rollback(versionID: VersionID): Unit = ???


  override def close(): Unit = {
    lock.writeLock().lock()
    try {
      fileOuts.values.foreach(_.close())
      fileHandles.values.foreach(fileAccess.close(_))
      fileOuts.clear()
      fileHandles.clear()
    } finally {
      lock.writeLock().unlock()
    }
  }


  protected[iodb] def keyValues(fileLog: List[LogFileUpdate], isJournal: Boolean = false): Iterator[(K, V)] = {
    //assert that all updates except last are merged
    assert(fileLog.isEmpty || (fileLog.last.merged == !isJournal))
    assert(!isJournal || fileLog.forall(!_.merged))
    //    assert(fileLog.isEmpty || fileLog.takeWhile(!_.merged).size==fileLog.size-1)

    val iters = fileLog.map { u =>
      fileAccess.readKeyValues(fileHandle = fileHandles(u.fileNum),
        offset = u.offset, keySize = keySize)
        .map(p => (p._1, u.offset, p._2)).asJava
    }.asJava.asInstanceOf[java.lang.Iterable[util.Iterator[(K, Int, V)]]]

    //merge multiple iterators, result iterator is sorted union of all iters
    var prevKey: K = null
    val iter = Iterators.mergeSorted[(K, Int, V)](iters, KeyOffsetValueComparator)
      .asScala
      .filter { it =>
        val include =
          (prevKey == null || //first key
            !prevKey.equals(it._1)) && //is first version of this key
            it._3 != null // only include if is not tombstone
        prevKey = it._1
        include
      }
      //drop the tombstones
      .filter(it => it._3 != Store.tombstone)
      //remove update
      .map(it => (it._1, it._3))

    return iter
  }

  def verify(): Unit = {
    //verify shard boundaries
    shards.asScala.foldRight(null.asInstanceOf[K]) { (e: Tuple2[K, List[LogFileUpdate]], endKey: K) =>
      e._2.foreach { u =>
        assert(u.shardStartKey == e._1)
        assert(u.shardEndKey == endKey)
      }
      val fileNum = e._2.head.fileNum
      val updates = loadUpdates(file = numToFile(fileNum), fileNum = fileNum)

      updates.foreach { u =>
        assert(u.shardStartKey == e._1)
        assert(u.shardEndKey == endKey)
      }

      e._1
    }
  }
}

/** Compares key-value pairs. Key is used for comparation, value is ignored */
protected[iodb] object KeyOffsetValueComparator extends Comparator[(K, Int, V)] {
  def compare(o1: (K, Int, V),
              o2: (K, Int, V)): Int = {
    //compare by key
    val c = o1._1.compareTo(o2._1)
    //compare by reverse offset
    if (c != 0) c else -o1._2.compareTo(o2._2)
  }
}


//TODO this consumes too much memory, should be represented using single primitive long[]
case class LogFileUpdate(
                          offset: Int,
                          keyCount: Int,
                          merged: Boolean,
                          fileNum: Long,
                          versionID: VersionID,
                          prevVersionID: VersionID,
                          shardStartKey: K,
                          shardEndKey: K
                        ) {
  assert(shardEndKey == null || shardStartKey < shardEndKey)
}
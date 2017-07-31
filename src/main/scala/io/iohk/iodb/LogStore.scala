package io.iohk.iodb

import java.io.{ByteArrayOutputStream, DataOutputStream, File, FileOutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}

import com.google.common.collect.Iterators
import io.iohk.iodb.Store._

import scala.collection.JavaConverters._
import scala.collection.mutable


object LogStore {
  val updatePrevFileNum = 4 + 1
  val updatePrevFileOffset = updatePrevFileNum + 8
  val updateKeyCountOffset = updatePrevFileOffset + 8

  val updateVersionIDSize = updateKeyCountOffset + 8


  val headUpdate = 1.toByte
  val headDistribute = 2.toByte
  val headAlias = 3.toByte
  val headMerge = 4.toByte
}

case class FilePos(fileNum: FileNum, offset: FileOffset) {
}

/**
  * Implementation of Store over series of Log Files
  */
class LogStore(
                val dir: File,
                val keySize: Int = 32,
                val keepVersions: Int = 0,
                val fileAccess: FileAccess = FileAccess.SAFE,
                val filePrefix: String = "store",
                val fileSuffix: String = ".journal"
              ) extends Store {

  protected val appendLock = new ReentrantLock()

  /** position of last valid entry in log file.
    *
    * Is read without lock, is updated under `appendLock` after file sync
    */
  protected[iodb] val validPos = new AtomicReference(new FilePos(fileNum = 1L, offset = -1L))

  /** End of File. New records will be appended here
    */
  protected[iodb] var eof = new FilePos(fileNum = 1L, offset = 0L)

  protected var fout: FileOutputStream = null

  protected[iodb] val fileHandles = new ConcurrentSkipListMap[FileNum, Any]()

  protected[iodb] val fileToDelete = new ConcurrentSkipListMap[FileNum, Any]()

  protected[iodb] val offsetAliases = new ConcurrentHashMap[FilePos, FilePos]()

  /** handles operation which add/delete files */
  protected[iodb] val fileLock = new ReentrantLock()

  protected[iodb] val fileSemaphore = new ConcurrentHashMap[java.lang.Long, java.lang.Long]()

  {
    //open all files in folder
    for (f <- dir.listFiles();
         name = f.getName;
         if (name.startsWith(filePrefix) && name.endsWith(fileSuffix))
    ) {
      val num2 = name.substring(filePrefix.size, name.length - fileSuffix.length)
      val num = java.lang.Long.valueOf(num2) //TODO better way to check for numbers
      val handle = fileAccess.open(f.getPath)
      fileHandles.put(num, handle)
    }

    //replay all files to restore offsetAliases
    for ((fileNum, fileHandle) <- fileHandles.asScala) {
      var offset = 0L
      var length = fileAccess.fileSize(fileHandle)
      while (offset < length) {
        val size = fileAccess.readInt(fileHandle, offset)
        //verify checksum
        val b = new Array[Byte](size - 8)
        fileAccess.readData(fileHandle, offset, b)
        val calcChecksum = Utils.checksum(b)
        val expectChecksum = fileAccess.readLong(fileHandle, offset + size - 8)

        assert(calcChecksum == expectChecksum)

        val head = b(4)

        if (head == LogStore.headUpdate)
          validPos.set(new FilePos(fileNum = fileNum, offset = offset))

        if (head == LogStore.headAlias) {
          val oldPos = loadFilePos(fileHandle, offset + 16 + LogStore.updatePrevFileNum)
          val newPos = loadFilePos(fileHandle, offset + 16 + LogStore.updatePrevFileOffset + 8)
          offsetAliases.put(oldPos, newPos)
        }
        offset += size
      }

    }
    //replay log if it exists
    if (!fileHandles.isEmpty) {
      //start replay at beginning of last file
      var fileNum = fileHandles.lastKey()
      var fileHandle = fileHandles.lastEntry().getValue
      var offset = 0
      val length = fileAccess.fileSize(fileHandle)

      //FIXME howto handle data corruption? still allow store to be opened, if there is corruption
    }
  }


  protected def loadFilePos(fileHandle: Any, offset: Long): FilePos = {
    val fileNum = fileAccess.readLong(fileHandle, offset)
    val fileOffset = fileAccess.readLong(fileHandle, offset + 8)
    return new FilePos(fileNum = Math.abs(fileNum), offset = fileOffset)

  }
  protected def fileNumToFile(fileNum: Long) = new File(dir, filePrefix + fileNum + fileSuffix)

  protected def finalizeLogEntry(out: ByteArrayOutputStream, out2: DataOutputStream): Array[Byte] = {
    //write placeholder for checksum
    out2.writeLong(0L)

    //write checksum and size placeholders
    val ret = out.toByteArray
    val wrap = ByteBuffer.wrap(ret)
    assert(wrap.getInt(0) == 0)

    wrap.putInt(0, ret.size)
    wrap.putLong(ret.size - 8, Utils.checksum(ret, 0, ret.size - 8))

    return ret
  }

  protected[iodb] def serializeUpdate(
                                       versionID: VersionID,
                                       data: Iterable[(K, V)],
                                       isMerged: Boolean,
                                       prevFileNumber: Long,
                                       prevFileOffset: Long

                                     ): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val out2 = new DataOutputStream(out)

    //placeholder for  `update size`
    out2.writeInt(0)

    //update type
    out2.writeByte(if (isMerged) LogStore.headMerge else LogStore.headUpdate)
    out2.writeLong(prevFileNumber)
    out2.writeLong(prevFileOffset)

    out2.writeInt(data.size)
    out2.writeInt(keySize)
    out2.writeInt(versionID.size)

    //write keys
    data.map(_._1.data).foreach(out2.write(_))

    //write value sizes and their offsets
    var valueOffset = out.size() + data.size * 8 + versionID.size
    data.foreach { t =>
      val value = t._2
      if (value == Store.tombstone) {
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

    //write values
    data.foreach { t =>
      if (t._2 != null) //filter out tombstones
        out2.write(t._2.data)
    }
    return finalizeLogEntry(out, out2)
  }

  def update(
              versionID: VersionID,
              toRemove: Iterable[K],
              toUpdate: Iterable[(K, V)]
            ): Unit = {

    //produce sorted and merged data set
    val data = new mutable.TreeMap[K, V]()
    for (key <- toRemove) {
      val old = data.put(key, Store.tombstone)
      if (old.isDefined)
        throw new IllegalArgumentException("duplicate key in `toRemove`")
    }
    for ((key, value) <- toUpdate) {
      val old = data.put(key, value)
      if (old.isDefined)
        throw new IllegalArgumentException("duplicate key in `toUpdate`")
    }

    updateDistribute(versionID = versionID, data = data, triggerCompaction = true)
  }

  def updateDistribute(versionID: VersionID, data: Iterable[(K, V)], triggerCompaction: Boolean): FilePos = {
    appendLock.lock()
    try {
      val oldPos = validPos.get
      val curPos = eof
      //TODO optimistic serialization outside appendLock, retry offset (and reserialize) under lock
      val serialized = serializeUpdate(
        versionID = versionID,
        data = data,
        prevFileNumber = oldPos.fileNum,
        prevFileOffset = oldPos.offset,
        isMerged = false
      )

      //flush
      append(serialized)
      //and update pointers
      validPos.set(curPos)
      eof = new FilePos(fileNum = curPos.fileNum, offset = eof.offset + serialized.length)

      if (triggerCompaction && validPos.get().offset != 0 && eof.offset > 1e8) {
        //trigger compaction if too big
        compact()
      }
      return curPos
    } finally {
      appendLock.unlock()
    }
  }


  protected[iodb] def startNewFile() {
    appendLock.lock()
    try {
      if (fout != null) {
        fout.close()
        fout = null
      }
      val offsets = loadUpdateOffsets()
      //FIXME thread unsafe `fileHandles` access
      val newFileNumber = fileHandles.lastKey() + 1
      val fileJournal = fileNumToFile(newFileNumber)
      fout = new FileOutputStream(fileJournal)
      val fileHandle = fileAccess.open(fileJournal.getPath)
      fileHandles.put(newFileNumber, fileHandle)
      eof = new FilePos(fileNum = newFileNumber, offset = 0)
    } finally {
      appendLock.unlock()
    }
  }

  protected[iodb] def appendFileAlias(oldFileNum: FileNum, oldFileOffset: FileOffset, newFileNumber: FileNum, newFileOffset: FileOffset): Unit = {
    appendLock.lock()
    try {
      val b = ByteBuffer.allocate(4 + 1 + 16 + 4 * 8 + 8)
      b.putInt(b.array().length)
      b.put(LogStore.headAlias)
      b.putLong(-1)
      b.putLong(-1)
      b.putLong(oldFileNum)
      b.putLong(oldFileOffset)
      b.putLong(newFileNumber)
      b.putLong(newFileOffset)

      val checksum = Utils.checksum(b.array(), 0, b.array().length - 8)
      b.putLong(checksum)

      append(b.array())
      offsetAliases.put(
        new FilePos(fileNum = oldFileNum, offset = newFileOffset),
        new FilePos(fileNum = newFileNumber, offset = newFileOffset))
    } finally {
      appendLock.unlock()
    }
  }


  protected[iodb] def appendDistributeEntry(journalPos: FilePos, shards: Map[K, FilePos]): Unit = {
    appendLock.lock()
    try {
      val curPos = eof

      val updateLen = 4 + 1 + 16 + 16 + 4 + shards.size * (16 + keySize) + 8
      val b = ByteBuffer.allocate(updateLen)
      b.putInt(b.array().length)
      b.put(LogStore.headDistribute)
      b.putLong(-1)
      b.putLong(-1)

      b.putLong(journalPos.fileNum)
      b.putLong(journalPos.offset)

      b.putInt(shards.size)
      for ((shardKey, shardPos) <- shards) {
        b.putLong(shardPos.fileNum)
        b.putLong(shardPos.offset)
        b.put(shardKey.data)
      }

      val checksum = Utils.checksum(b.array(), 0, b.array().length - 8)
      b.putLong(checksum)

      append(b.array())

      //and update pointers
      validPos.set(curPos)
      eof = new FilePos(fileNum = curPos.fileNum, offset = eof.offset + updateLen)

    } finally {
      appendLock.unlock()
    }
  }


  protected[iodb] def append(data: Array[Byte]): Unit = {
    //append to end of the file
    appendLock.lock()
    try {
      //FIXME thread unsafe `fileHandles` access
      if (fout == null && fileHandles.isEmpty) {
        val fileNum = 1L
        val fileJournal = fileNumToFile(fileNum)
        //open file
        fout = new FileOutputStream(fileJournal)
        //add to file handles
        val fileHandle = fileAccess.open(fileJournal.getPath)
        fileHandles.put(fileNum, fileHandle)
      }
      fout.write(data)

      //flush changes
      fout.getChannel.force(false)
    } finally {
      appendLock.unlock()
    }
  }


  def loadPrevFilePos(fileHandle: Any, fileOffset: FileOffset): FilePos = {
    if (fileHandle == null) {
      return null
      //      throw new DataCorruptionException("File not found:  " + filePos.fileNum)
    }

    val prevFilePos = loadFilePos(fileHandle, fileOffset + LogStore.updatePrevFileNum)
    return offsetAliases.getOrDefault(prevFilePos, prevFilePos)
  }

  def loadIsMerged(filePos: FilePos): Boolean = {
    //FIXME thread unsafe access
    val fileHandle = fileHandles.get(filePos.fileNum)
    val header = fileAccess.readByte(fileHandle, filePos.offset + 4)
    return header match {
      case LogStore.headUpdate => false
      case LogStore.headMerge => true
      case _ => throw new DataCorruptionException("wrong header")
    }
  }


  def get(key: K): Option[V] = {
    var filePos = validPos.get

    while (filePos != null && filePos.offset >= 0) {
      val fileNum = filePos.fileNum
      val fileHandle = fileLock(fileNum)
      try {
        if (fileHandle == null)
          throw new DataCorruptionException("File Number not found")
        val header = fileAccess.readByte(fileHandle, filePos.offset + 4)

        header match {
          case LogStore.headMerge => {}
          case LogStore.headUpdate => {}
          case _ => throw new DataCorruptionException("unexpected header")
        }

        //binary search
        val result = fileAccess.getValue(fileHandle, key, keySize, filePos.offset)
        if (result != null)
          return result
        // no need to continue traversal
        if (header == LogStore.headMerge)
          return None

        //move to previous update
        filePos = loadPrevFilePos(fileHandle, filePos.offset)
      } finally {
        fileUnlock(fileNum)
      }
    }
    return None
  }


  def loadDistributeShardPositions(filePos: FilePos): Map[K, FilePos] = {
    val fileHandle = fileLock(filePos.fileNum)
    try {
      val journalPos = loadFilePos(fileHandle, filePos.offset + 4 + 1 + 16)
      val mapSize = fileAccess.readInt(fileHandle, filePos.offset + 4 + 1 + 16 + 16)
      val baseOffset = filePos.offset + 4 + 1 + 16 + 16 + 4

      return (0 until mapSize)
        .map(baseOffset + _ * (16 + keySize)) //offset
        .map { offset =>
        val pos = loadFilePos(fileHandle, offset)
        val key = new K(keySize)
        fileAccess.readData(fileHandle, offset + 16, key.data)
        (key, pos)
      }.toMap
    } finally {
      fileUnlock(filePos.fileNum)
    }
  }

  def getDistribute(key: K): (Option[V], Option[Map[K, FilePos]]) = {
    var filePos = validPos.get

    while (filePos != null && filePos.offset >= 0) {
      val fileNum = filePos.fileNum
      val fileHandle = fileLock(fileNum)
      try {
        if (fileHandle == null)
          throw new DataCorruptionException("File Number not found")
        val header = fileAccess.readByte(fileHandle, filePos.offset + 4)
        header match {
          case LogStore.headMerge => {}
          case LogStore.headUpdate => {}
          case LogStore.headDistribute => {}
          case _ => throw new DataCorruptionException("unexpected header")
        }

        if (header == LogStore.headDistribute) {
          val shardPositions = loadDistributeShardPositions(filePos)
          return (None, Some(shardPositions))
        }

        //binary search
        val result = fileAccess.getValue(fileHandle, key, keySize, filePos.offset)
        if (result != null)
          return (result, None)
        // no need to continue traversal
        if (header == LogStore.headMerge)
          return (None, None)

        //move to previous update
        filePos = loadPrevFilePos(fileHandle, filePos.offset)
      } finally {
        fileUnlock(fileNum)
      }
    }
    return (None, None)
  }

  def close(): Unit = {
    appendLock.lock()
    try {
      fileLock.lock()
      try {
        if (fout != null) {
          fout.close()
          fout = null
        }

        fileHandles.values().asScala.foreach(fileAccess.close(_))
        fileHandles.clear()
      } finally {
        fileLock.unlock()
      }
    } finally {
      appendLock.lock()
    }
  }

  protected[iodb] def loadUpdateOffsets(): Iterable[FilePos] = {
    // load offsets of all log entries
    var filePos = validPos.get
    val offsets = mutable.ArrayBuffer[FilePos]()

    //TODO merged stop assertions
    while (filePos != null && filePos.offset >= 0) {
      offsets += filePos
      val fileNum = filePos.fileNum

      val fileHandle = fileLock(fileNum)
      try {
        //move to previous update
        filePos = if (fileHandle == null) null else loadPrevFilePos(fileHandle, filePos.offset)
      } finally {
        fileUnlock(fileNum)
      }
    }
    return offsets
  }

  override def rollbackVersions(): Iterable[VersionID] = {
    loadUpdateOffsets()
      .map(loadVersionID(_))
      .toBuffer.reverse
  }


  def loadVersionID(filePos: FilePos): VersionID = {
    val fileNum = filePos.fileNum
    val fileHandle = fileLock(fileNum)
    try {
      val keyCount = fileAccess.readInt(fileHandle, filePos.offset + LogStore.updateKeyCountOffset)
      val versionIDSize = fileAccess.readInt(fileHandle, filePos.offset + LogStore.updateVersionIDSize)
      val versionIDOffset = filePos.offset + LSMStore.updateHeaderSize + (keySize + 4 + 4) * keyCount
      val ret = new VersionID(versionIDSize)
      fileAccess.readData(fileHandle, versionIDOffset, ret.data)
      return ret
    } finally {
      fileUnlock(fileNum)
    }
  }

  protected[iodb] def loadKeyValues(
                                 offsets: Iterable[FilePos],
                                 dropTombstones: Boolean)
  : Iterator[(K, V)] = {

    // open iterators over all log entries
    // it is iterator of iterators, each entry in subiterator contains key, index in `offsets` and value
    val iters = offsets
      .zipWithIndex
      //convert to iterators over log entries
      // each key/value pair comes with index in `offsets`, most recent pairs have lower index
      //FIXME thread unsafe access to `fileHandles`
      .map(a => fileAccess.readKeyValues(fileHandles.get(a._1.fileNum), a._1.offset, keySize).map(e => (e._1, a._2, e._2)).asJava)
      //add entry index to iterators
      .asJava

    //merge multiple iterators, result iterator is sorted union of all iterators
    // duplicate keys are handled by comparing `offset` index, and including only most recent entry
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
      //drop tombstones
      .filter(it => !dropTombstones || it._3 != Store.tombstone)
      //remove update
      .map(it => (it._1, it._3))

    iter
  }


  def compact(): Unit = {
    appendLock.lock()
    try {
      val offsets = loadUpdateOffsets()
      if (offsets.size <= 1)
        return

      val keyVals = loadKeyValues(offsets, dropTombstones = true).toBuffer //TODO serialize keys lazily, without loading entire chunk to memory
      //load versionID for newest offset
      val newestPos = offsets.head
      val versionID = loadVersionID(newestPos)

      startNewFile()
      val data = serializeUpdate(versionID, keyVals,
        isMerged = true,
        prevFileNumber = newestPos.fileNum,
        prevFileOffset = newestPos.offset
      )
      startNewFile()
      append(data)
      //and update pointers
      validPos.set(eof)
      eof = new FilePos(fileNum = eof.fileNum, offset = eof.offset + data.length)

    } finally {
      appendLock.unlock()
    }

  }

  override def getAll(consumer: (K, V) => Unit): Unit = {
    val offsets = loadUpdateOffsets()
    if (offsets.size <= 1)
      return
    val keyVals = loadKeyValues(offsets, dropTombstones = true)
    for ((key, value) <- keyVals) {
      consumer(key, value)
    }
  }

  override def clean(count: Int): Unit = {
    compact()
    appendLock.lock()
    try {
      val offsets = loadUpdateOffsets()
      //files which are used in last
      //remove all files which are lower than lowest used file N versions
      val preserve = offsets.take(count).map(_.fileNum).toSet

      //FIXME thread unsafe access??
      for (fileNum <- fileHandles.keySet().asScala.filterNot(preserve.contains(_))) {
        fileDelete(fileNum)
      }
    } finally {
      appendLock.unlock()
    }
  }

  override def lastVersionID: Option[VersionID] = {
    val lastPos = validPos.get()
    if (lastPos == null || lastPos.offset < 0)
      return None
    return Some(loadVersionID(lastPos))
  }

  override def rollback(versionID: VersionID): Unit = {
    appendLock.lock()
    try {
      //find offset for version ID
      for (pos <- loadUpdateOffsets()) {
        val versionID2 = loadVersionID(pos)
        if (versionID2 == versionID) {
          //insert new link to log
          val serialized = serializeUpdate(
            versionID = versionID,
            data = Nil,
            prevFileOffset = pos.offset,
            prevFileNumber = pos.fileNum,
            isMerged = false
          )

          //flush
          append(serialized)
          //and update pointers
          validPos.set(eof)
          eof = new FilePos(fileNum = eof.fileNum, offset = eof.offset + serialized.length)

          //update offsets
          validPos.set(pos)
          return
        }
      }
      //version not found
      throw new IllegalArgumentException("Version not found")
    } finally {
      appendLock.unlock()
    }
  }

  override def verify(): Unit = {
    //check offsets, check at least one is merged
    val offs = loadUpdateOffsets().toIndexedSeq
    // offset should start at zero position, or contain at least one merged update
    if (!offs.isEmpty) {
      val genesisEntry = offs.last.fileNum == 0 && offs.last.offset == 0
      val containsMerge = offs.contains(loadIsMerged(_))

      if (genesisEntry || containsMerge)
        throw new DataCorruptionException("update chain broken")
    }
  }

  def fileLock(fileNum: FileNum): Any = {
    fileLock.lock()
    try {
      val ret = fileHandles.get(fileNum)
      if (ret == null)
        return null
      Utils.fileReaderIncrement(fileSemaphore, fileNum)
      return ret
    } finally {
      fileLock.unlock()
    }
  }

  def fileUnlock(fileNum: FileNum): Unit = {
    fileLock.lock()
    try {
      Utils.fileReaderDecrement(fileSemaphore, fileNum)
    } finally {
      fileLock.unlock()
    }
  }

  def fileDelete(fileNum: FileNum): Unit = {
    fileLock.lock()
    try {
      val handle = fileHandles.remove(fileNum)
      if (handle == null)
        return

      if (!fileSemaphore.containsKey(fileNum)) {
        //if there are no readers attached, delete file
        fileAccess.close(handle)
        fileNumToFile(fileNum).delete()
      } else {
        //some readers are attached, so schedule for deletion latter
        fileToDelete.put(fileNum, handle)
      }
    } finally {
      fileLock.unlock()
    }
  }

  def taskFileDeleteScheduled(): Unit = {
    fileLock.lock()
    try {
      for ((fileNum, handle) <- fileToDelete.asScala) {
        if (!fileSemaphore.containsKey(fileNum)) {
          fileAccess.close(handle)
          fileNumToFile(fileNum).delete()
          fileToDelete.remove(fileNum)
        }
      }
    } finally {
      fileLock.unlock()
    }
  }
}

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

case class FilePos(fileNum: FileNum, offset: FileOffset)

/**
  * Implementation of Store over series of Log Files
  */
class LogStore(
                val dir: File,
                val keySize: Int = 32,
                val fileAccess: FileAccess = FileAccess.SAFE,
                val filePrefix: String = "store",
                val fileSuffix: String = ".journal"
              ) extends Store {


  protected val appendLock = new ReentrantLock()

  /** position of last valid entry in log file.
    *
    * Is read without lock, is updated under `appendLock` after file sync
    */
  protected[iodb] val validPos = new AtomicReference(new FilePos(fileNum = -1L, offset = -1L))

  /** End of File. New records will be appended here
    */
  protected[iodb] var eof = new FilePos(fileNum = 0L, offset = 0L)

  protected var fileJournal = new File(dir, filePrefix + "0" + fileSuffix)
  protected var fout: FileOutputStream = null

  protected[iodb] val fileHandles = new ConcurrentSkipListMap[Long, Any]()

  protected[iodb] val offsetAliases = new ConcurrentHashMap[FilePos, FilePos]()

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
          val oldPos = new FilePos(
            fileNum = fileAccess.readLong(fileHandle, offset + LogStore.updatePrevFileNum),
            offset = fileAccess.readLong(fileHandle, offset + LogStore.updatePrevFileOffset)
          )
          val newPos = new FilePos(
            fileNum = fileAccess.readLong(fileHandle, offset + LogStore.updatePrevFileOffset + 8),
            offset = fileAccess.readLong(fileHandle, offset + LogStore.updatePrevFileOffset + 8 + 8)
          )
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
    out2.writeByte(LogStore.headUpdate)

    out2.writeLong(prevFileNumber)
    out2.writeLong(prevFileOffset)

    out2.writeInt(data.size)
    out2.writeInt(keySize)
    out2.writeInt(versionID.size)
    out2.writeBoolean(isMerged)

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


    appendLock.lock()
    try {
      val oldPos = validPos.get

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
      validPos.set(eof)
      eof = new FilePos(fileNum = eof.fileNum, offset = eof.offset + serialized.length)
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
      val newFileNumber = fileHandles.lastKey() + 1
      fileJournal = new File(dir, filePrefix + newFileNumber + fileSuffix)
      fout = new FileOutputStream(fileJournal)
      val fileHandle = fileAccess.open(fileJournal.getPath)
      fileHandles.put(newFileNumber, fileHandle)
      eof = new FilePos(fileNum = newFileNumber, offset = 0)
    } finally {
      appendLock.unlock()
    }
  }

  protected[iodb] def appendFileAlias(oldFileNum: Long, oldFileOffset: Long, newFileNumber: Long, newFileOffset: Long): Unit = {
    appendLock.lock()
    try {
      val b = ByteBuffer.allocate(4 + 1 + 4 * 8 + 8)
      b.putInt(b.array().length)
      b.put(LogStore.headAlias)
      b.putLong(oldFileNum)
      b.putLong(oldFileOffset)
      b.putLong(newFileNumber)
      b.putLong(newFileOffset)

      val checksum = Utils.checksum(b.array(), 0, b.array().length - 8)
      b.putLong(checksum)

      append(b.array())
      offsetAliases.put(new FilePos(fileNum = oldFileNum, offset = newFileOffset), new FilePos(fileNum = newFileNumber, offset = newFileOffset))
    } finally {
      appendLock.unlock()
    }
  }

  protected[iodb] def append(data: Array[Byte]): Unit = {
    //append to end of the file
    appendLock.lock()
    try {
      if (!fileJournal.exists()) {
        //open file
        fout = new FileOutputStream(fileJournal)
        //add to file handles
        val fileHandle = fileAccess.open(fileJournal.getPath)
        fileHandles.put(0L, fileHandle)

      }
      fout.write(data)

      //flush changes
      fout.getChannel.force(false)
    } finally {
      appendLock.unlock()
    }
  }

  def readPrevFilePos(filePos: FilePos): FilePos = {
    val fileHandle = fileHandles.get(filePos.fileNum)

    val prevFilePos = new FilePos(
      fileNum = fileAccess.readLong(fileHandle, filePos.offset + LogStore.updatePrevFileNum),
      offset = fileAccess.readLong(fileHandle, filePos.offset + LogStore.updatePrevFileOffset))

    return offsetAliases.getOrDefault(prevFilePos, prevFilePos)
  }


  def get(key: K): Option[V] = {
    var filePos = validPos.get

    while (filePos != null && filePos.offset >= 0) {
      val fileHandle = fileHandles.get(filePos.fileNum)
      if (fileHandle == null)
        throw new DataCorruptionException("File Number not found")
      //binary search
      val result = fileAccess.getValue(fileHandle, key, keySize, filePos.offset)
      if (result != null)
        return result

      //move to previous update
      filePos = readPrevFilePos(filePos)
    }
    return None
  }

  def close(): Unit = {
    appendLock.lock()
    try {
      if (fout != null) {
        fout.close()
        fout = null
      }

      fileHandles.values().asScala.foreach(fileAccess.close(_))
      fileHandles.clear()
    } finally {
      appendLock.lock()
    }
  }

  protected[iodb] def loadUpdateOffsets(): Iterable[FilePos] = {
    // load offsets of all log entries
    var filePos = validPos.get
    val offsets = mutable.ArrayBuffer[FilePos]()

    while (filePos.offset >= 0) {
      offsets += filePos
      //move to previous update
      filePos = readPrevFilePos(filePos)
    }
    return offsets
  }

  override def rollbackVersions(): Iterable[VersionID] = {
    loadUpdateOffsets()
      .map(loadVersionID(_))
      .toBuffer.reverse
  }


  def loadVersionID(filePos: FilePos): VersionID = {
    val fileHandle = fileHandles.get(filePos.fileNum)
    val keyCount = fileAccess.readInt(fileHandle, filePos.offset + LogStore.updateKeyCountOffset)
    val versionIDSize = fileAccess.readInt(fileHandle, filePos.offset + LogStore.updateVersionIDSize)
    val versionIDOffset = filePos.offset + LSMStore.updateHeaderSize + (keySize + 4 + 4) * keyCount
    val ret = new VersionID(versionIDSize)
    fileAccess.readData(fileHandle, versionIDOffset, ret.data)
    return ret
  }

  protected[iodb] def keyValues(
                                 offsets: Iterable[FilePos],
                                 dropTombstones: Boolean)
  : Iterator[(K, V)] = {

    // open iterators over all log entries
    // it is iterator of iterators, each entry in subiterator contains key, index in `offsets` and value
    val iters = offsets
      .zipWithIndex
      //convert to iterators over log entries
      // each key/value pair comes with index in `offsets`, most recent pairs have lower index
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
    val offsets = loadUpdateOffsets()
    if (offsets.size <= 1)
      return
    val keyVals = keyValues(offsets, dropTombstones = true).toBuffer //TODO serialize keys lazily, without loading entire chunk to memory
    //load versionID for newest offset
    val newestPos = offsets.head
    val versionID = loadVersionID(newestPos)

    val data = serializeUpdate(versionID, keyVals,
      isMerged = true,
      //TODO how to handle link to previous number?
      prevFileNumber = newestPos.fileNum,
      prevFileOffset = newestPos.offset
    )
    append(data)
  }

  override def getAll(consumer: (K, V) => Unit): Unit = {
    val offsets = loadUpdateOffsets()
    if (offsets.size <= 1)
      return
    val keyVals = keyValues(offsets, dropTombstones = true)
    for ((key, value) <- keyVals) {
      consumer(key, value)
    }
  }

  override def clean(count: Int): Unit = {
  }

  override def cleanStop(): Unit = {}

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
}

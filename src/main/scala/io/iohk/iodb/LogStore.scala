package io.iohk.iodb

import java.io.{ByteArrayOutputStream, DataOutputStream, File, FileOutputStream}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import com.google.common.collect.Iterators
import io.iohk.iodb.Store._

import scala.collection.JavaConverters._
import scala.collection.mutable


object LogStore {
  val updatePrevFileNum = 4 + 1
  val updatePrevFileOffset = updatePrevFileNum + 8
  val updateKeyCountOffset = updatePrevFileOffset + 8

  val updateVersionIDSize = updateKeyCountOffset + 8


}

case class FilePos(fileNum: FileNum, offset: FileOffset)

/**
  * Implementation of Store over series of Log Files
  */
class LogStore(
                val dir: File,
                val keySize: Int = 32,
                val fileAccess: FileAccess = FileAccess.SAFE
              ) extends Store {

  protected val appendLock = new ReentrantLock()

  protected val ENTRY_TYPE_UPDATE: Byte = 1

  /** position of last valid entry in log file.
    *
    * Is read without lock, is updated under `appendLock` after file sync
    */
  protected[iodb] val validPos = new AtomicReference(new FilePos(-1L, -1L))

  /** End of File. New records will be appended here
    *
    *
    */
  protected[iodb] var eof = new FilePos(0L, 0L)

  protected val fileJournal = new File(dir, "journal")
  protected var fout: FileOutputStream = null
  protected[iodb] var fileHandle: Any = null

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
                                       prevFileOffset: Long,
                                       prevFileNumber: Long

                                     ): Array[Byte] = {

    val out = new ByteArrayOutputStream()
    val out2 = new DataOutputStream(out)

    //placeholder for  `update size`
    out2.writeInt(0)

    //update type
    out2.writeByte(ENTRY_TYPE_UPDATE)

    out2.writeLong(prevFileOffset)
    out2.writeLong(prevFileNumber)

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
        prevFileOffset = oldPos.offset,
        prevFileNumber = oldPos.fileNum,
        isMerged = false
      )

      //flush
      append(serialized)
      //and update pointers
      validPos.set(eof)
      eof = new FilePos(eof.fileNum, eof.offset + serialized.length)
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
      }
      fout.write(data)

      //flush changes
      fout.getChannel.force(false)
    } finally {
      appendLock.unlock()
    }
  }


  def get(key: K): Option[V] = {
    val filePos = validPos.get
    var fileNum: FileNum = filePos.fileNum
    var fileOffset: FileOffset = filePos.offset

    if (fileHandle == null) {
      // FIXME fopen is not thread safe
      fileHandle = fileAccess.open(fileJournal.getPath)
    }

    while (fileOffset >= 0) {
      //binary search
      val result = fileAccess.getValue(fileHandle, key, keySize, fileOffset)
      if (result != null)
        return result

      //move to previous update
      fileNum = fileAccess.readLong(fileHandle, fileOffset + 5 + 8)
      fileOffset = fileAccess.readLong(fileHandle, fileOffset + 5)
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

      if (fileHandle != null) {
        fileAccess.close(fileHandle) //TODO prevent concurrent reads while DirectByteBuffer is opened
        fileHandle = null
      }
    } finally {
      appendLock.lock()
    }
  }

  protected[iodb] def loadUpdateOffsets(): Iterable[FilePos] = {
    // load offsets of all log entries
    val filePos = validPos.get
    var fileNum: FileNum = filePos.fileNum
    var fileOffset: FileOffset = filePos.offset

    val offsets = mutable.ArrayBuffer[FilePos]()

    while (fileOffset >= 0) {
      offsets += FilePos(fileNum, fileOffset)
      //move to previous update
      fileNum = fileAccess.readLong(fileHandle, fileOffset + 5 + 8)
      fileOffset = fileAccess.readLong(fileHandle, fileOffset + 5)
    }
    return offsets
  }


  override def rollbackVersions(): Iterable[VersionID] = {
    loadUpdateOffsets()
      .map(pos => loadVersionID(pos.fileNum, pos.offset))
      .toBuffer.reverse
  }


  def loadVersionID(fileNum: FileNum, fileOffset: FileOffset): VersionID = {
    val keyCount = fileAccess.readInt(fileHandle, fileOffset + LogStore.updateKeyCountOffset)
    val versionIDSize = fileAccess.readInt(fileHandle, fileOffset + LogStore.updateVersionIDSize)
    val versionIDOffset = fileOffset + LSMStore.updateHeaderSize + (keySize + 4 + 4) * keyCount
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
      .map(a => fileAccess.readKeyValues(fileHandle, a._1.offset, keySize).map(e => (e._1, a._2, e._2)).asJava)
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
    val versionID = loadVersionID(newestPos.fileNum, newestPos.offset)

    val data = serializeUpdate(versionID, keyVals,
      isMerged = true,
      //TODO how to handle link to previous number?
      prevFileNumber = newestPos.fileNum,
      prevFileOffset = newestPos.offset
    )
    append(data)

    //TODO insert alias?
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

  override def clean(count: Int): Unit = ???

  override def cleanStop(): Unit = ???

  override def lastVersionID: Option[VersionID] = ???

  override def rollback(versionID: VersionID): Unit = ???
}

package io.iohk.iodb

import java.io.{ByteArrayOutputStream, DataOutputStream, File, FileOutputStream}
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}

import com.google.common.collect.Iterators
import io.iohk.iodb.Store._

import scala.collection.JavaConverters._
import scala.collection.mutable


/**
  * Implementation of Store over series of Log Files
  */
class LogStore(
                val dir: File,
                val keySize: Int = 32,
                val fileAccess: FileAccess = FileAccess.SAFE
              ) {


  protected val structuralLock = new ReentrantReadWriteLock()
  protected val appendLock = new ReentrantLock()

  protected val ENTRY_TYPE_UPDATE: Byte = 1

  protected var journalFileOffset: FileOffset = -1L
  protected[iodb] var journalFileNum: FileNum = 0L
  protected[iodb] var journalLastEntryOffset: FileOffset = -1L

  protected val fileJournal = new File(dir, "journal")
  protected var fout: FileOutputStream = null
  protected[iodb] var fAccess: Any = null

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


    var serialized: Array[Byte] = null
    var expectedOffsetBefore: FileOffset = -1
    var expectedOffsetAfter: FileOffset = -1
    structuralLock.writeLock().lock()
    try {
      // TODO performance: serialize outside lock, update sizes in second step
      serialized = serializeUpdate(
        versionID = versionID,
        data = data,
        prevFileOffset = this.journalLastEntryOffset,
        prevFileNumber = this.journalFileNum,
        isMerged = false
      )

      //increase pointer to EOF
      if (this.journalFileOffset < 0)
        this.journalFileOffset = 0
      this.journalLastEntryOffset = this.journalFileOffset
      this.journalFileOffset += serialized.length
      expectedOffsetAfter = journalFileOffset
      expectedOffsetBefore = journalLastEntryOffset

    } finally {
      structuralLock.writeLock().unlock()
    }

    //append to end of the file
    appendLock.lock()
    try {

      if (!fileJournal.exists()) {
        //open file
        fout = new FileOutputStream(fileJournal)
      }
      assert(expectedOffsetBefore < 0 || fout.getChannel.position() == expectedOffsetBefore)
      fout.write(serialized)
      assert(fout.getChannel.position() == expectedOffsetAfter)

      //flush changes
      fout.getChannel.force(false)
    } finally {
      appendLock.unlock()
    }
  }

  def getLastEntryOffset(): (FileNum, FileOffset) = {
    structuralLock.readLock().lock()
    try {
      return (this.journalFileNum, this.journalLastEntryOffset)
    } finally {
      structuralLock.readLock().unlock()
    }
  }

  def get(key: K): Option[V] = {
    var (fileNum: FileNum, fileOffset: FileOffset) = getLastEntryOffset()

    if (fAccess == null) {
      // FIXME fopen is not thread safe
      fAccess = fileAccess.open(fileJournal.getPath)
    }

    while (fileOffset >= 0) {
      //binary search
      val result = fileAccess.getValue(fAccess, key, keySize, fileOffset)
      if (result != null)
        return result

      //move to previous update
      fileNum = fileAccess.readLong(fAccess, fileOffset + 5 + 8)
      fileOffset = fileAccess.readLong(fAccess, fileOffset + 5)
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

      if (fAccess != null) {
        fileAccess.close(fAccess) //TODO prevent concurrent reads while DirectByteBuffer is opened
        fAccess = null
      }
    } finally {
      appendLock.lock()
    }
  }


  protected[iodb] def keyValues(dropTombstones: Boolean): Iterator[(K, V)] = {
    //load offsets of all log entries
    var (fileNum: FileNum, fileOffset: FileOffset) = getLastEntryOffset()
    val offsets = mutable.ArrayBuffer[(FileNum, FileOffset)]()

    while (fileOffset >= 0) {
      offsets += ((fileNum, fileOffset))
      //move to previous update
      fileNum = fileAccess.readLong(fAccess, fileOffset + 5 + 8)
      fileOffset = fileAccess.readLong(fAccess, fileOffset + 5)
    }

    /*

        //assert that all updates except last are merged
        val iters = fileLog.zipWithIndex.map { a =>
          val u = a._1
          val index = a._2
          fileAccess.readKeyValues(fileHandle = fileHandles(u.fileNum),
            offset = u.offset, keySize = keySize)
            .map(p => (p._1, index, p._2)).asJava
        }.asJava.asInstanceOf[java.lang.Iterable[util.Iterator[(K, Int, V)]]]

    */
    val iters =
      offsets
        .zipWithIndex
        //convert to iterators over log entries
        // each key/value pair comes with index in `offsets`, most recent pairs have lower index
        .map(a => fileAccess.readKeyValues(fAccess, a._1._2, keySize).map(e => (e._1, a._2, e._2)).asJava)
        //add entry index to iterators
        .asJava.asInstanceOf[java.lang.Iterable[util.Iterator[(K, Int, V)]]]

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


    iter
  }


}

package io.iohk.iodb

import java.io.{ByteArrayOutputStream, DataOutputStream, File}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.util
import java.util.Comparator
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent._

import com.google.common.collect.Iterators
import io.iohk.iodb.Store._

import scala.collection.JavaConverters._
import scala.collection.mutable


object LogStore {

  /* offsets */
  val updatePrevFileNum = 4 + 1
  val updatePrevFileOffset = updatePrevFileNum + 8
  val updateKeyCountOffset = updatePrevFileOffset + 8

  val updateVersionIDSize = updateKeyCountOffset + 8

  /* size of header */
  val updateHeaderSize = +4 + 1 + 8 + 8 + 4 + 4 + 4


  /* headers of log entries */
  val headUpdate = 1.toByte
  val headDistributeFinished = 2.toByte
  val headAlias = 3.toByte
  val headMerge = 4.toByte
  val headDistributePlaceholder = 5.toByte

}

/** position in files; file number and file offset */
case class FilePos(fileNum: FileNum, offset: FileOffset) {

}

/** offset withing single file, also contains log entry type */
case class LogEntryPos(pos: FilePos, entryType: Byte) {
}


case class DistributeEntry(
                            journalPos: FilePos,
                            prevPos:FilePos,
                            versionID:VersionID,
                            shards: util.TreeMap[K, FilePos])
/**
  * Implementation of Store over series of Log Files
  */
class LogStore(
                val dir: File,
                val keySize: Int = 32,
                val keepVersions: Int = 0,
                val filePrefix: String = "store",
                val fileSuffix: String = ".journal",
                //unlimited thread executor, but the threads will exit faster to prevent memory leak
                val executor:Executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 1, TimeUnit.SECONDS, new SynchronousQueue[Runnable]()),
                val compactEnabled:Boolean = false
              ) extends Store {


  /** locked when file is expanded*/
  protected[iodb] val appendLock = new ReentrantLock()

  /** position of last valid entry in log file.
    *
    * Is read without lock, is updated under `appendLock` after file sync
    */
  protected[iodb] val _validPos = new AtomicReference(new FilePos(fileNum = 1L, offset = -1L))


  /** End of File. New records will be appended here
    */
  @volatile protected[iodb] var eof = new FilePos(fileNum = 1L, offset = 0L)

  /** file output over latest file */
  protected var fout: FileChannel = null


  protected[iodb] val unmergedUpdates = new AtomicLong(0)

  /** Lock used when creating or deleting old files  */
  protected[iodb] val fileLock = new ReentrantLock()

  /** Prevents file from being deleted, while it is read.
    * Key is File Number, value is number of readers in given file.
    * File can be only deleted if number of readers is zero.
    * If memory mapped file is unmapped during IO, it causes illegal memory access and JVM process crash.
    */
  protected[iodb] val fileSemaphore = new ConcurrentHashMap[java.lang.Long, java.lang.Long]()

  /** opened files, protected by `fileLock` */
  protected[iodb] val fileHandles = new ConcurrentSkipListMap[FileNum, FileChannel]()

  /** files scheduled for deletion */
  protected[iodb] val fileToDelete = new ConcurrentSkipListMap[FileNum, FileChannel]()

  /** Alternative positions within log file.
    *
    * Offset Alias is generated when new update entry replaces older, for example during compaction.
    */
  protected[iodb] val offsetAliases = new ConcurrentHashMap[FilePos, FilePos]()

  {
    if(!dir.exists() || !dir.isDirectory() )
      throw new IllegalArgumentException("Directory does not exist: "+dir)
    if(!dir.canWrite())
      throw new IllegalArgumentException("Directory is not writable: "+dir )
    if(!dir.canRead())
      throw new IllegalArgumentException("Directory is not readable: "+dir )

    //open all files in folder
    for (f <- dir.listFiles();
         name = f.getName;
         if (name.startsWith(filePrefix) && name.endsWith(fileSuffix))
    ) {
      val num2 = name.substring(filePrefix.size, name.length - fileSuffix.length)
      val num = java.lang.Long.valueOf(num2) //TODO better way to check for numbers
      val handle = fileOpen(num)
    }

    //replay all files to restore offsetAliases
    for ((fileNum, fileHandle) <- fileHandles.asScala) {
      var offset = 0L
      var length = fileHandle.size()
      while (offset < length) {
        val size = fileReadInt(fileHandle, offset)
        //verify checksum
        val b = new Array[Byte](size - 8)
        fileReadData(fileHandle, offset, b)
        val calcChecksum = Utils.checksum(b)
        val expectChecksum = fileReadLong(fileHandle, offset + size - 8)

        if(calcChecksum != expectChecksum)
          throw new DataCorruptionException("wrong checksum")

        val head = b(4)

        if (head == LogStore.headUpdate)
          setValidPos(new FilePos(fileNum = fileNum, offset = offset))

        if (head == LogStore.headAlias) {
          val oldPos = loadFilePos(fileHandle, offset + 16 + LogStore.updatePrevFileNum)
          val newPos = loadFilePos(fileHandle, offset + 16 + LogStore.updatePrevFileOffset + 8)
          offsetAliases.put(oldPos, newPos)
        }
        offset += size
        eof = FilePos(fileNum, offset)
        fout = fileHandle
      }
    }

    //replay log if it exists
    if (!fileHandles.isEmpty) {
      //start replay at beginning of last file
      var fileNum = fileHandles.lastKey()
      var fileHandle = fileHandles.lastEntry().getValue
      var offset = 0
      val length = fileHandle.size()

      //FIXME howto handle data corruption? store should open, even if there is a corruption
    }

    val unmergedCount = loadUpdateOffsets(stopAtDistribute = true, stopAtMerge = true).size
    this.unmergedUpdates.set(unmergedCount)
  }


  /** read File Position (file number, file offset) from given file and position */
  protected def loadFilePos(fileHandle: FileChannel, offset: Long): FilePos = {
    val fileNum = fileReadLong(fileHandle, offset)
    val fileOffset = fileReadLong(fileHandle, offset + 8)
    return new FilePos(fileNum = Math.abs(fileNum), offset = fileOffset)

  }


  protected def fileNumToFile(fileNum: Long) = new File(dir, filePrefix + fileNum + fileSuffix)

  /** takes content from OutputStream, adds checksum and returns it as `byte[]` */
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

  /** serialize Update Entry into `byte[]` */
  protected[iodb] def serializeUpdate(
                                       versionID: VersionID,
                                       data: Iterable[(K, V)],
                                       isMerged: Boolean,
                                       prevFileNumber: Long,
                                       prevFileOffset: Long

                                     ): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val out2 = new DataOutputStream(out)

    //placeholder for  update size
    out2.writeInt(0)

    //update type
    out2.writeByte(if (isMerged) LogStore.headMerge else LogStore.headUpdate)
    out2.writeLong(prevFileNumber)
    out2.writeLong(prevFileOffset)

    out2.writeInt(data.size)
    out2.writeInt(keySize)
    out2.writeInt( if(versionID==null) -1 else versionID.size)

    //write keys
    data.map(_._1.data).foreach(out2.write(_))

    //write value sizes and their offsets
    var valueOffset = out.size() + data.size * 8 + (if(versionID==null) 0 else versionID.size)
    data.foreach { t =>
      val value = t._2
      if (value eq Store.tombstone) {
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

    if(versionID!=null)
      out2.write(versionID.data)

    //write values
    data.foreach { t =>
      if (t._2 != null) //filter out tombstones
        out2.write(t._2.data)
    }
    return finalizeLogEntry(out, out2)
  }

  /** add Update Entry into log. It assumes input data are not sorted */
  def update(
              versionID: VersionID,
              toRemove: Iterable[K],
              toUpdate: Iterable[(K, V)]
            ): Unit = {


    //produce sorted and merged data set
    val data = new mutable.TreeMap[K, V]()
    for (key <- toRemove) {
      assert(key.size == keySize)
      val old = data.put(key, Store.tombstone)
      if (old.isDefined)
        throw new IllegalArgumentException("duplicate key in `toRemove`")
    }
    for ((key, value) <- toUpdate) {
      assert(key.size == keySize)
      val old = data.put(key, value)
      if (old.isDefined)
        throw new IllegalArgumentException("duplicate key in `toUpdate`")
    }
    updateDistribute(versionID = versionID, data = data, triggerCompaction = true)

    //TODO background operations
    taskFileDeleteScheduled()
  }


  /** Update called from ShardedStore distribute task */
  protected[iodb] def updateDistribute(versionID: VersionID, data: Iterable[(K, V)], triggerCompaction: Boolean): FilePos = {
    appendLock.lock()
    try {
      unmergedUpdates.incrementAndGet()
      val oldPos = getValidPos()
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
      setValidPos(curPos)
      if (compactEnabled && triggerCompaction && getValidPos().offset != 0 && eof.offset > 1e8) {
        //trigger compaction if too big
        executor.execute(runnable {
          taskCompact()
        })
      }
      return curPos
    } finally {
      appendLock.unlock()
    }
  }


  /** closes current append file, and starts new empty file */
  protected[iodb] def startNewFile() {
    appendLock.lock()
    try {
      if (fout != null) {
        //TODO reopen in readonly mode
        //fout.close()
        fout = null
      }
      //FIXME thread unsafe `fileHandles` access
      val newFileNumber = fileHandles.lastKey() + 1
      fout = fileOpen(newFileNumber)
      eof = new FilePos(fileNum = newFileNumber, offset = fout.size())
    } finally {
      appendLock.unlock()
    }
  }


  /** last valid update position */
  def getValidPos(): FilePos = {
    val pos = _validPos.get()
    return offsetAliases.getOrDefault(pos, pos)
  }

  def setValidPos(pos: FilePos) {
    _validPos.set(pos)
  }

  def appendDistributePlaceholder(): (FilePos, FilePos) = {
    appendLock.lock()
    try {
      val prev = getValidPos()
      val b = ByteBuffer.allocate(4 + 1 + 16 + +8)
      b.putInt(b.array().length)
      b.put(LogStore.headDistributePlaceholder)
      b.putLong(prev.fileNum)
      b.putLong(prev.offset)

      val checksum = Utils.checksum(b.array(), 0, b.array().length - 8)
      b.putLong(checksum)

      val pos = eof
      append(b.array())
      setValidPos(pos)
      return (prev,pos)
    } finally {
      appendLock.unlock()
    }

  }


  protected[iodb] def appendFileAlias(oldFileNum: FileNum, oldFileOffset: FileOffset, newFileNumber: FileNum, newFileOffset: FileOffset, updateEOF: Boolean = false): Unit = {
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
        new FilePos(fileNum = oldFileNum, offset = oldFileOffset),
        new FilePos(fileNum = newFileNumber, offset = newFileOffset))
    } finally {
      appendLock.unlock()
    }
  }


  protected[iodb] def appendDistributeEntry(journalPos: FilePos, prevPos:FilePos, versionID:VersionID, shards: Iterable[(K, FilePos)]): FilePos = {
    appendLock.lock()
    try {
      val curPos = eof
      val updateLen = 4 + 1 + 16 + 16 + 4 + versionID.size + 4 + shards.size * (16 + keySize) + 8
      val b = ByteBuffer.allocate(updateLen)
      b.putInt(b.array().length)
      b.put(LogStore.headDistributeFinished)
      assert(curPos!=prevPos)
      b.putLong(prevPos.fileNum)
      b.putLong(prevPos.offset)

      b.putLong(journalPos.fileNum)
      b.putLong(journalPos.offset)

      assert(versionID.size>0)
      b.putInt(versionID.size)
      b.put(versionID.data)

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
      //      validPos.set(curPos)
      return curPos
    } finally {
      appendLock.unlock()
    }
  }


  protected[iodb] def append(data: Array[Byte]): FilePos = {
    //append to end of the file
    appendLock.lock()
    try {

      //FIXME force handling EOF
      //FIXME thread unsafe `fileHandles` access
      if (fout == null && fileHandles.isEmpty) {
        val fileNum = 1L
        //add to file handles
        fout = fileOpen(fileNum)
        eof = FilePos(fileNum = fileNum, offset = fout.size())
      }

      assert(fout.size() == eof.offset)

      val data2 = ByteBuffer.wrap(data)
      val offset = eof.offset
      Utils.writeFully(fout, offset, data2)

      val ret = eof;
      eof = eof.copy(offset = eof.offset + data.size)
      assert(fout.size() == eof.offset)

      //flush changes
      fout.force(true)
      return ret
    } finally {
      appendLock.unlock()
    }
  }


  def loadPrevFilePos(fileHandle: FileChannel, fileOffset: FileOffset): FilePos = {
    if (fileHandle == null) {
      return null
      //      throw new DataCorruptionException("File not found:  " + filePos.fileNum)
    }

    val prevFilePos = loadFilePos(fileHandle, fileOffset + LogStore.updatePrevFileNum)
    return offsetAliases.getOrDefault(prevFilePos, prevFilePos)
  }


  def loadPrevFilePos(filePos:FilePos): FilePos = {
    val fileHandle = fileLock(filePos.fileNum)
    try {
      val header = fileReadByte(fileHandle, filePos.offset+4)
      assert(header==LogStore.headMerge || header==LogStore.headUpdate ||
        header==LogStore.headDistributeFinished || header==LogStore.headDistributePlaceholder)
      return loadPrevFilePos(fileHandle, filePos.offset)
    } finally {
      if (fileHandle != null)
        fileUnlock(filePos.fileNum)
    }
  }


  def get(key: K): Option[V] = {
    return get(key = key, pos = getValidPos())
  }

  def get(key: K, pos: FilePos): Option[V] = {
    var filePos = pos
    var counter = 0

    while (filePos != null && filePos.offset >= 0) {
      val fileNum = filePos.fileNum
      val fileHandle = fileLock(fileNum)
      try {
        if (fileHandle == null)
          throw new DataCorruptionException("File Number not found " + fileNum)
        val header = fileReadByte(fileHandle, filePos.offset + 4)

        header match {
          case LogStore.headMerge => {}
          case LogStore.headUpdate => {}
          case _ => throw new DataCorruptionException("unexpected header "+header.toInt+" - "+fileNum+" - "+filePos.offset)
        }

        //binary search
        val result = fileGetValue(fileHandle, key, keySize, filePos.offset)
        if (result != null)
          return result
        // no need to continue traversal
        if (header == LogStore.headMerge)
          return None

        //move to previous update
        filePos = loadPrevFilePos(fileHandle, filePos.offset)
        counter+=1
      } finally {
        if (fileHandle != null)
          fileUnlock(fileNum)
      }
    }
    return None
  }



  def loadDistributeEntry(filePos: FilePos): DistributeEntry = {
    val fileHandle = fileLock(filePos.fileNum)
    try {
      var pos = filePos.offset+4+1
      val prevPos = loadFilePos(fileHandle, pos)
      pos+=16
      val journalPos = loadFilePos(fileHandle, pos)
      pos+=16
      val versionIDSize = fileReadInt(fileHandle, pos)
      pos+=4
      val versionID = fileReadData(fileHandle, offset=pos, size=versionIDSize)
      pos+=versionIDSize

      val mapSize = fileReadInt(fileHandle, pos)
      pos+=4

      val shards = new util.TreeMap[K, FilePos]()
      (0 until mapSize)
        .map(pos + _ * (16 + keySize)) //offset
        .map { offset =>
        val pos2 = loadFilePos(fileHandle, offset)
        val key = new K(keySize)
        fileReadData(fileHandle, offset + 16, key.data)
        shards.put(key, pos2)
      }
      return DistributeEntry(prevPos=prevPos, journalPos=journalPos, versionID=versionID, shards=shards)
    } finally {
      if (fileHandle != null)
        fileUnlock(filePos.fileNum)
    }
  }

  def getDistribute(key: K): (Option[V], Option[DistributeEntry]) = {
    var filePos = getValidPos()
    while (filePos != null && filePos.offset >= 0) {

      val fileNum = filePos.fileNum
      val fileHandle = fileLock(fileNum)
      try {
        if (fileHandle == null)
          throw new DataCorruptionException("File Number not found: " + fileNum)

        val header = fileReadByte(fileHandle, filePos.offset + 4)
        header match {
          case LogStore.headMerge => {}
          case LogStore.headUpdate => {}
          case LogStore.headDistributeFinished => {}
          case LogStore.headDistributePlaceholder => {}
          case _ => throw new DataCorruptionException("unexpected header")
        }


        if (header == LogStore.headDistributeFinished) {
          val distEntry = loadDistributeEntry(filePos)
          return (None, Some(distEntry))
        }

        if (header == LogStore.headMerge || header == LogStore.headUpdate) {
          //binary search
          val result = fileGetValue(fileHandle, key, keySize, filePos.offset)
          if (result != null)
            return (result, None)
          // no need to continue traversal
          if (header == LogStore.headMerge)
            return (None, None)
        }

        //move to previous update
        filePos = loadPrevFilePos(fileHandle, filePos.offset)
      } finally {
        if (fileHandle != null)
          fileUnlock(fileNum)
      }
    }
    return (None, None)
  }


  def getAllDistribute(): (util.TreeMap[K,V], DistributeEntry) = {
    val data = new util.TreeMap[K,V]
    var filePos = getValidPos()
    while (filePos != null && filePos.offset >= 0) {


      val fileNum = filePos.fileNum
      val fileHandle = fileLock(fileNum)
      try {
        if (fileHandle == null)
          throw new DataCorruptionException("File Number not found: " + fileNum)

        val header = fileReadByte(fileHandle, filePos.offset + 4)
        header match {
          case LogStore.headMerge => {}
          case LogStore.headUpdate => {}
          case LogStore.headDistributeFinished => {}
          case LogStore.headDistributePlaceholder => {}
          case _ => throw new DataCorruptionException("unexpected header")
        }


        if (header == LogStore.headDistributeFinished) {
          val distEntry = loadDistributeEntry(filePos)
          return (data, distEntry)
        }

        if (header == LogStore.headMerge || header == LogStore.headUpdate) {
          val keyvals = fileReadKeyValues(c=fileHandle, offset=filePos.offset, keySize = keySize)
          while(keyvals.hasNext){
            val  (k,v) = keyvals.next()
            data.putIfAbsent(k,v)
          }
          if (header == LogStore.headMerge)
            return (data, null)
        }

        //move to previous update
        filePos = loadPrevFilePos(fileHandle, filePos.offset)
      } finally {
        if (fileHandle != null)
          fileUnlock(fileNum)
      }
    }
    return (data, null)
  }

  def close(): Unit = {
    appendLock.lock()
    try {
      fileLock.lock()
      try {
        if (fout != null) {
          //          fout.close()
          fout = null
        }

        fileHandles.values().asScala.foreach(fileClose(_))
        fileHandles.clear()
      } finally {
        fileLock.unlock()
      }
    } finally {
      appendLock.lock()
    }
  }

  protected[iodb] def loadUpdateOffsets(stopAtMerge: Boolean, startPos: FilePos = getValidPos(), stopAtDistribute: Boolean): Iterable[LogEntryPos] = {
    // load offsets of all log entries
    var filePos = getValidPos()
    val offsets = mutable.ArrayBuffer[LogEntryPos]()

    //TODO merged stop assertions
    while (filePos != null && filePos.offset >= 0) {
      val fileNum = filePos.fileNum

      val fileHandle = fileLock(fileNum)
      try {
        if (fileHandle == null || fileHandle.size() <= filePos.offset) {
          return offsets
        }

        val header = fileReadByte(fileHandle, filePos.offset + 4)
        offsets += LogEntryPos(filePos, header)
        val shouldStop =
          if (!stopAtMerge || !stopAtDistribute) false
          else if (stopAtDistribute && header == LogStore.headDistributeFinished) true
          else if (stopAtMerge && header == LogStore.headMerge) true
          else false

        //move to previous update
        filePos =
          if (fileHandle == null || shouldStop) null
          else loadPrevFilePos(fileHandle, filePos.offset)
      } finally {
        if(fileHandle!=null)
          fileUnlock(fileNum)
      }
    }
    return offsets
  }

  override def rollbackVersions(): Iterable[VersionID] = {
    loadUpdateOffsets(stopAtMerge = false, stopAtDistribute = false)
      .map(p => loadVersionID(p.pos))
      .filter(_!=null)
      .toBuffer.reverse
  }


  def loadVersionID(filePos: FilePos): VersionID = {
    val fileNum = filePos.fileNum
    val fileHandle = fileLock(fileNum)
    try {
      val header = fileReadByte(fileHandle, filePos.offset+4)

      if(header!=LogStore.headUpdate && header!=LogStore.headMerge)
        return null

      val keyCount = fileReadInt(fileHandle, filePos.offset + LogStore.updateKeyCountOffset)
      val versionIDSize = fileReadInt(fileHandle, filePos.offset + LogStore.updateVersionIDSize)
      if(versionIDSize<=0)
        return null
      val versionIDOffset = filePos.offset + LogStore.updateHeaderSize + (keySize + 4 + 4) * keyCount
      val ret = new VersionID(versionIDSize)
      fileReadData(fileHandle, versionIDOffset, ret.data)
      return ret
    } finally {
      if (fileHandle != null)
        fileUnlock(fileNum)
    }
  }

  protected[iodb] def loadKeyValues(
                                     offsets: Iterable[LogEntryPos],
                                     files: Map[FileNum, FileChannel],
                                     dropTombstones: Boolean)
  : Iterator[(K, V)] = {

    // open iterators over all log entries
    // it is iterator of iterators, each entry in subiterator contains key, index in `offsets` and value
    val iters = offsets
      .zipWithIndex
      //convert to iterators over log entries
      // each key/value pair comes with index in `offsets`, most recent pairs have lower index
      .map(a => fileReadKeyValues(files(a._1.pos.fileNum), a._1.pos.offset, keySize).map(e => (e._1, a._2, e._2)).asJava)
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
      .filter(it => !dropTombstones || !(it._3 eq Store.tombstone))
      //remove update
      .map(it => (it._1, it._3))

    iter
  }


  def taskCompact(): Unit = {
    appendLock.lock()
    try {
      val offsets = loadUpdateOffsets(stopAtMerge = true, stopAtDistribute = true)
      if (offsets.size <= 1)
        return

      //lock all files for reading
      val files:Map[FileNum, FileChannel] = offsets.map(o=> o.pos.fileNum).toSet.map{n:FileNum => (n, fileLock(n))}.toMap
      try {
        val keyVals = loadKeyValues(offsets, files, dropTombstones = true).toBuffer //TODO serialize keys lazily, without loading entire chunk to memory
        //load versionID for newest offset
        val newestPos = offsets.head
        val versionID = loadVersionID(newestPos.pos)

        startNewFile()
        val eof2 = eof
        val data = serializeUpdate(versionID, keyVals,
          isMerged = true,
          prevFileNumber = newestPos.pos.fileNum,
          prevFileOffset = newestPos.pos.offset
        )
        append(data)
        //and update pointers
        setValidPos(eof2)
        unmergedUpdates.set(0)
      } finally {
        for ((fileNum, fileHandle) <- files) {
          if (fileHandle != null)
            fileUnlock(fileNum)
        }
      }
    } finally {
      appendLock.unlock()
    }

    //TODO background operations
    taskFileDeleteScheduled()
  }

  override def getAll(consumer: (K, V) => Unit): Unit = {
    getAll(consumer, dropTombstone = true)
  }

  def getAll(consumer: (K, V) => Unit, dropTombstone: Boolean, startPos: FilePos = getValidPos()): Unit = {
    val offsets = loadUpdateOffsets(stopAtMerge = true, stopAtDistribute = false, startPos = startPos)
    if (offsets.size <= 0)
      return
    //lock files for reading
    val files:Map[FileNum, FileChannel] = offsets.map(o=> o.pos.fileNum).toSet.map{fileNum:FileNum => (fileNum, fileLock(fileNum))}.toMap
    try {

      val keyVals = loadKeyValues(offsets, files, dropTombstones = dropTombstone)
      for ((key, value) <- keyVals) {
        consumer(key, value)
      }
    } finally {
      for ((fileNum, fileHandle) <- files) {
        if (fileHandle != null)
          fileUnlock(fileNum)
      }
    }

  }

  override def clean(count: Int): Unit = {
    taskCompact()
    appendLock.lock()
    try {
      val offsets = loadUpdateOffsets(stopAtMerge = false, stopAtDistribute = false).toBuffer

      //find first merge entry after N (count) versions
      var splitPos = count - 1
      do {
        splitPos += 1
      } while (splitPos < offsets.size && offsets(count).entryType != LogStore.headMerge)

      //remove all files which are lower than lowest used file N versions

      if (splitPos >= offsets.size)
        return

      val preserveFiles = offsets.take(splitPos + 1).map(_.pos.fileNum).toSet

      //FIXME thread unsafe access??
      for (fileNum <- fileHandles.keySet().asScala.filterNot(preserveFiles.contains(_)).toArray) {
        fileDelete(fileNum)
      }
    } finally {
      appendLock.unlock()
    }
  }

  override def lastVersionID: Option[VersionID] = {
    var pos = getValidPos()
    while(pos != null && pos.offset >= 0) {
      val ver = loadVersionID(pos)
      if(ver!=null) {
        return Some(ver)
      }
      pos = loadPrevFilePos(pos)
    }
    return None
  }

  def rollbackToOffset(pos:FilePos): Unit ={
    appendLock.lock()
    try {
      //find offset for version ID
      //insert new link to log
      val serialized = serializeUpdate(
        versionID = null,
        data = Nil,
        prevFileOffset = pos.offset,
        prevFileNumber = pos.fileNum,
        isMerged = false
      )

      //flush
      val pos2 = append(serialized)

      //update offsets
      setValidPos(pos2)

    } finally {
      appendLock.unlock()
    }

    //TODO background operations
    taskFileDeleteScheduled()

  }

  def rollbackToZero(): Unit = {
    appendLock.lock()
    try {
      //find offset for version ID
      //insert new link to log
      val serialized = serializeUpdate(
        versionID = null,
        data = Nil,
        prevFileOffset = -1,
        prevFileNumber = 0,
        isMerged = true
      )

      //flush
      val pos = append(serialized)

      //update offsets
      setValidPos(pos)

    } finally {
      appendLock.unlock()
    }

    //TODO background operations
    taskFileDeleteScheduled()
  }

  override def rollback(versionID: VersionID): Unit = {
    appendLock.lock()
    try {
      //find offset for version ID
      for (pos <- loadUpdateOffsets(stopAtMerge = false, stopAtDistribute = false)) {
        val versionID2 = loadVersionID(pos.pos)
        if (versionID2 == versionID) {
          //insert new link to log
          val serialized = serializeUpdate(
            versionID = versionID,
            data = Nil,
            prevFileOffset = pos.pos.offset,
            prevFileNumber = pos.pos.fileNum,
            isMerged = false
          )

          //flush
          append(serialized)

          //update offsets
          setValidPos(pos.pos)
          //restore update counts
          unmergedUpdates.set(loadUpdateOffsets(stopAtDistribute = true, stopAtMerge = true).size)
          //TODO background operations
          taskFileDeleteScheduled()
          return
        }
      }
      //version not found
      throw new IllegalArgumentException("Version not found")
    } finally {
      appendLock.unlock()
    }

    //TODO background operations
    taskFileDeleteScheduled()
  }

  override def verify(): Unit = {
    //check offsets, check at least one is merged
    val offs = loadUpdateOffsets(stopAtMerge = false, stopAtDistribute = false).toBuffer
    // offset should start at zero position, or contain at least one merged update
    if (!offs.isEmpty) {
      val genesisEntry = offs.last.pos.fileNum == 1 && offs.last.pos.offset == 0
      val containsMerge = offs.find(_.entryType == LogStore.headMerge).isDefined

      if (!genesisEntry && !containsMerge)
        throw new DataCorruptionException("update chain broken")
    }
  }

  def fileLock(fileNum: FileNum): FileChannel = {
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
        fileClose(handle)
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
          fileClose(handle)
          fileNumToFile(fileNum).delete()
          fileToDelete.remove(fileNum) //TODO delete outside file lock
        }
      }
    } finally {
      fileLock.unlock()
    }
  }

  def fileOpen(fileNum: FileNum): FileChannel = {
    val file = fileNumToFile(fileNum)
    val c = fileChannelOpen(file)
    c.position(c.size())
    val oldFile = fileHandles.put(fileNum, c)
    assert(oldFile == null)
    return c
  }

  def fileChannelOpen(file: File) = FileChannel.open(file.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)


  def fileClose(c: FileChannel): Unit = {
    c.force(true)
    c.close()
  }


  def fileGetValue(c: FileChannel, key: K, keySize: Int, updateOffset: Long): Option[V] = {
    assert(updateOffset >= 0, "negative updateOffset: " + updateOffset)
    assert(key.size == keySize)
    val tempBuf = ByteBuffer.allocate(8)

    val keyCount: Long = fileReadInt(c, updateOffset + LogStore.updateKeyCountOffset, tempBuf)

    val baseKeyOffset = updateOffset + LogStore.updateHeaderSize

    val key2 = new Array[Byte](keySize)
    val key2B = ByteBuffer.wrap(key2)
    var lo: Long = 0
    var hi: Long = keyCount - 1

    while (lo <= hi) {
      //split interval
      val mid = (lo + hi) / 2
      val keyOffset = baseKeyOffset + mid * keySize
      //load key
      key2B.clear()
      Utils.readFully(c, keyOffset, key2B)
      //compare keys and split intervals if not equal
      val comp = Utils.BYTE_ARRAY_COMPARATOR.compare(key2, key.data)
      if (comp < 0) lo = mid + 1
      else if (comp > 0) hi = mid - 1
      else {
        //key found, read size and offset
        val valuePointersOffset = baseKeyOffset + keyCount * keySize + mid * 8
        val valueSize = fileReadInt(c, valuePointersOffset, tempBuf)
        if (valueSize == -1)
          return None
        //tombstone, return nothing
        val valueOffset = fileReadInt(c, valuePointersOffset + 4, tempBuf)
        //load value
        return Some(fileReadData(c, updateOffset + valueOffset, valueSize))
      }
    }
    null
  }

  protected def fileReadData(c: FileChannel, offset: Long, size: Int): ByteArrayWrapper = {
    val ret = new Array[Byte](size)
    val ret2 = ByteBuffer.wrap(ret)
    Utils.readFully(c, offset, ret2)
    return ByteArrayWrapper(ret)
  }

  protected def fileReadLong(c: FileChannel, offset: Long, buf: ByteBuffer = ByteBuffer.allocate(8)): Long = {
    buf.clear();
    buf.limit(8)
    Utils.readFully(c, offset, buf)
    return buf.getLong(0)
  }


  protected def fileReadInt(c: FileChannel, offset: Long, buf: ByteBuffer = ByteBuffer.allocate(4)): Int = {
    buf.position(0);
    buf.limit(4)
    Utils.readFully(c, offset, buf)
    return buf.getInt(0)
  }

  def fileReadData(c: FileChannel, offset: Long, data: Array[Byte]): Unit = {
    val b = ByteBuffer.wrap(data)
    c.position(offset.toInt)
    Utils.readFully(c, offset, b)
  }


  def fileReadByte(file: FileChannel, offset: Long): Byte = {
    val b = ByteBuffer.allocate(1)
    fileReadData(file, offset, b.array())
    return b.get(0)
  }


  def fileReadKeyValues(c: FileChannel, offset: Long, keySize: Int): Iterator[(K, V)] = {
    assert(offset >= 0)
    val tempBuf = ByteBuffer.allocate(8)

    //get size
    val updateSize = fileReadInt(c, offset, tempBuf)
    val head = fileReadByte(c, offset + 4)


    //TODO should this section be here? It gives placeholder...
    if ((head != LogStore.headMerge && head != LogStore.headUpdate))
      return None.iterator

    val keyCount = fileReadInt(c, offset + LogStore.updateKeyCountOffset, tempBuf)
    assert(keyCount >= 0)
    assert(keyCount * keySize >= 0 && keyCount * keySize < updateSize)

    val baseKeyOffset = offset + LogStore.updateHeaderSize

    object ret extends Iterator[(K,V)](){

      var i = 0;

      override def hasNext:Boolean = i<keyCount

      override def next():(K,V) = {
        if(i>=keyCount)
          throw new NoSuchElementException()
        val keyOffset = baseKeyOffset + i * keySize
        val key = fileReadData(c, keyOffset, keySize)

        val pointersOffsets = baseKeyOffset + keyCount * keySize + i * 8
        val valueSize = fileReadInt(c, pointersOffsets, tempBuf)
        val value =
          if (valueSize == -1) Store.tombstone
          else {
            val valueOffset = fileReadInt(c, pointersOffsets + 4, tempBuf)
            fileReadData(c, offset + valueOffset, valueSize)
          }

        i+=1
        (key, value)
      }
    }

    return ret
  }

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
    c
  }
}

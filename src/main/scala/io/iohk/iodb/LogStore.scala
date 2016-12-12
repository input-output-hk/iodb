package io.iohk.iodb

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util.Comparator
import java.util.concurrent.ConcurrentSkipListMap

import com.google.common.collect.Iterators
import io.iohk.iodb.Store.VersionID
import io.iohk.iodb.Utils._

import scala.collection.JavaConverters._


object LogStore {

  /** file extension for log files */
  val logExt: String = ".log"

  /** file extension for merge files */
  val mergedExt: String = ".merged"

  /** size of log file header, without VersionID which has variable size */
  protected[iodb] val headerSizeWithoutVersionID =
    8 + //file header
      8 + //file checksum
      8 + // file size
      4 + // number of keys
      4 + //keys size
      8 + //version
      4 //version size

  /**
    * Memory maps file into read-only ByteBuffer. File must be smaller than 2GB due to addressing limit.
    *
    * @param file to be mapped
    * @return ByteByffer of memory mapped file
    */
  def mmap(file: File): ByteBuffer = {
    val c = FileChannel.open(file.toPath, StandardOpenOption.READ)
    val ret = c.map(MapMode.READ_ONLY, 0, file.length())
    c.close()
    ret
  }

  def fileDelete(f: File): Unit = {
    assert(f.exists())
    val deleted = f.delete()
    assert(deleted)
  }


  /**
    * Construct log file
    *
    * @param version    version for which LogFile is created
    * @param dir        directory where file is placed
    * @param filePrefix file name prefix
    * @param isMerged   true if this file is merged (it contains all values from older versions).
    * @return mapped buffer
    */
  def logFile(version: Long, dir: File, filePrefix: String, isMerged: Boolean = false): File = {
    new File(dir, filePrefix + version + (if (isMerged) mergedExt else logExt))
  }

}

/**
  * Single log file.
  */
class LogStore(
                val dir: File,
                val filePrefix: String,
                val keySize: Int = 32,
                protected val fileLocks: MultiLock[File] = new MultiLock[File](),
                val keepSingleVersion: Boolean = false,
                val fileSync: Boolean = true,
                useUnsafe: Boolean = unsafeSupported()
              ) {

  import LogStore._
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

  /**
    * Set of active files sorted in descending order (newest first).
    * First value is key file, second value is value file.
    * Java collection is used, it supports mutable iterator.
    */
  protected[iodb] val files = new ConcurrentSkipListMap[Long, LogFile](
    java.util.Collections.reverseOrder[Long]())

  def loadVersionIDLength(f: File): Int = {
    val raf = new RandomAccessFile(f, "r")
    raf.seek(headerSizeWithoutVersionID - 4)
    val ret = raf.readInt()
    raf.close()
    return ret;
  }

  {

    val files2 = dir.listFiles()
      .filter(f => f.getName.startsWith(filePrefix) && f.isFile)
      .map(_.getName().substring(filePrefix.length))
      .filter { f =>
        f.matches("[0-9]+" + logExt) ||
          f.matches("[0-9]+" + mergedExt)
      }
      .map { f =>
        val isMerged = f.endsWith(mergedExt)
        val version = f.split('.')(0).toLong
        LogFile(version = version,
          versionIDLength = loadVersionIDLength(new File(dir, filePrefix + f)),
          isMerged = isMerged, dir = dir, filePrefix = filePrefix)
      }

    files2.foreach { lf =>
      //only put if does not contain merged version
      if (!files.containsKey(lf.version) || !files.get(lf.version).isMerged)
        files.put(lf.version, lf)
    }

    for ((key, value) <- files.asScala) {
      assert(key == value.version)
    }


  }


  private val keySizeExtra: Long = keySize + 4 + 8

  private val tombstone = ByteArrayWrapper(new Array[Byte](0))


  /** iterates over all values in single version. Null value is tombstone. */
  protected[iodb] def versionIterator(version: Long): Iterator[(K, V)] = {
    val logFile = files.get(version)
    val buf = logFile.buf.duplicate()
    val valueBuf = buf.duplicate()
    buf.position(logFile.baseKeyOffset.toInt)

    val count = buf.getInt(keyCountOffset)
    (0 until count).map { i =>
      val key = new Array[Byte](keySize)
      buf.get(key)

      //load value
      val valueSize = buf.getInt()
      val valueOffset = buf.getLong
      val value =
        if (valueSize == -1) null
        else {
          //TODO seek should not be necessary here
          valueBuf.position(valueOffset.toInt)
          val value = new Array[Byte](valueSize)
          valueBuf.get(value)
          ByteArrayWrapper(value)
        }
      ByteArrayWrapper(key) -> value
    }.iterator
  }

  def versions: Iterable[Long] = files.keySet().asScala

  def keyValues(fromVersion: Long, toVersion: Long = Long.MinValue): Iterable[(K, V)] = {

    // compares result of iterators,
    // Second tuple val is Version, is descending so we get only newest version
    object comparator extends Comparator[(K, Long, V)] {
      def compare(o1: (K, Long, V),
                  o2: (K, Long, V)): Int = {
        val c = o1._1.compareTo(o2._1)
        if (c != 0) c else -o1._2.compareTo(o2._2)
      }
    }

    val versions = files.subMap(fromVersion, true, toVersion, false).keySet().asScala
    // iterator of iterators over all files
    val iters: java.lang.Iterable[java.util.Iterator[(K, Long, V)]] = versions.map { version =>
      val iter = versionIterator(version)
      iter.map { e =>
        (e._1, version, e._2)
      }.asJava
    }.asJavaCollection

    //TODO end iters at first merged (including)

    //merge multiple iterators, result iterator is sorted union of all iters
    var prevKey: K = null
    val iter = Iterators.mergeSorted[(K, Long, V)](iters, comparator)
      .asScala
      .filter { it =>
        val include =
          (prevKey == null || //first key
            !prevKey.equals(it._1)) && //is first version of this key
            it._3 != null // only include if is not tombstone
        prevKey = it._1
        include
      }.map(it => (it._1, it._3))

    iter.toBuffer
  }

  def get(key: K): Option[V] = {
    return get(key, lastVersion)
  }

  protected[iodb] def get(key: K, versionId: Long, stopAtVersion: Long = 0): Option[V] = {
    if (files.isEmpty)
      return None
    val versions =
      if (stopAtVersion > 0)
        files.subMap(versionId, true, stopAtVersion, false).asScala
      else
        files.tailMap(versionId).asScala
    for ((version, logFile) <- versions) {
      val ret = versionGet(logFile, key)
      if (tombstone eq ret)
        return None //deleted key
      if (ret != null)
        return Some(ret) // value was found
      if (logFile.isMerged)
        return None //contains all versions, will not be found in next versions
    }
    None
  }

  protected def versionGet(logFile: LogFile, key: K): V = {
    val buf = logFile.buf.duplicate()

    def loadValue(): V = {
      //key found, load value
      val valueSize = buf.getInt()
      if (valueSize == -1)
        return tombstone //tombstone, return nothing

      //load value
      val valueOffset = buf.getLong()
      buf.position(valueOffset.toInt)
      val ret = new Array[Byte](valueSize)
      buf.get(ret)
      ByteArrayWrapper(ret)
    }

    if (useUnsafe) {
      val r = Utils.unsafeBinarySearch(logFile.buf, key.data, logFile.baseKeyOffset.toInt)
      if (r < 0)
        return null
      val keyOffset = logFile.baseKeyOffset + r * keySizeExtra
      //load key
      buf.position(keyOffset.toInt + keySize)
      return loadValue
    }

    val key2 = new Array[Byte](keySize)
    val keyCount: Long = buf.getInt(keyCountOffset)
    var lo: Long = 0
    var hi: Long = keyCount - 1

    while (lo <= hi) {

      //split interval
      val mid = (lo + hi) / 2
      val keyOffset = logFile.baseKeyOffset + mid * keySizeExtra
      //load key

      buf.position(keyOffset.toInt)
      buf.get(key2)
      //compare keys and split intervals if not equal
      val comp = Utils.BYTE_ARRAY_COMPARATOR.compare(key2, key.data)
      if (comp < 0) lo = mid + 1
      else if (comp > 0) hi = mid - 1
      else {
        return loadValue()
      }
    }
    null
  }


  def update(versionID: VersionID,
             version: Long,
             toRemove: Iterable[K],
             toUpdate: Iterable[(K, V)]): Unit = {
    if (lastVersion >= version) {
      throw new IllegalArgumentException("version in argument is not greater than Store lastVersion")
    }

    val all = new java.util.TreeMap[K, V].asScala

    for ((key, value) <- toUpdate ++ toRemove.map {
      (_, tombstone)
    }) {
      if (key == null || value == null)
        throw new NullPointerException()

      if (key.data.length != keySize)
        throw new IllegalArgumentException("Wrong key size")

      val old = all.put(key, value)
      if (old.isDefined)
        throw new IllegalArgumentException("Duplicate keys found in single update")
    }

    updateSorted(versionID = versionID, version = version, isMerged = false, toUpdate = all.seq)
    files.put(version, new LogFile(
      version = version,
      versionIDLength = versionID.size,
      dir = dir,
      filePrefix = filePrefix,
      isMerged = false))
  }


  protected[iohk] def updateSorted(version: Long, versionID: VersionID,
                                   isMerged: Boolean, toUpdate: Iterable[(K, V)]): Unit = {

    //calculate file size
    val valuesSize = toUpdate.map(_._2.data.size).sum
    val valueOffsetStart = headerSizeWithoutVersionID +
      versionID.size +
      keySizeExtra * toUpdate.size
    val totalFileSize = valueOffsetStart + valuesSize

    // OutputStream
    val file = logFile(version = version, dir = dir, filePrefix = filePrefix, isMerged = isMerged)
    val fileStream = new FileOutputStream(file)
    val dataStream = new DataOutputStream(new BufferedOutputStream(fileStream))

    //write header
    dataStream.writeLong(0L) //header
    dataStream.writeLong(0L) //checksum
    dataStream.writeLong(totalFileSize) //file size
    dataStream.writeInt(toUpdate.size) //number of keys
    dataStream.writeInt(keySize) //size of single key
    dataStream.writeLong(version) // version
    dataStream.writeInt(versionID.data.length)
    dataStream.write(versionID.data)

    //write keys
    var valueOffset = valueOffsetStart
    for ((key, value) <- toUpdate) {
      if (key.size != keySize)
        throw new DataCorruptionException("wrong key size")
      dataStream.write(key.data)
      dataStream.writeInt(if (tombstone.eq(value)) -1 else value.size)
      dataStream.writeLong(valueOffset)
      valueOffset += value.size
    }

    //write values
    for ((key, value) <- toUpdate) {
      dataStream.write(value.data)
    }

    //close stream
    dataStream.flush()
    fileStream.flush()
    if (fileSync) //flush disk cache if needed
      fileStream.getFD.sync()
    dataStream.close()
    fileStream.close()
  }

  def lastVersion: Long = if (files.isEmpty) 0 else files.firstKey()

  def lastVersionID: VersionID = files.firstEntry().getValue.loadVersionID

  /** reverts to older version. Higher (newer) versions are discarded and their versionID can be reused */
  def rollback(versionID: Long): Unit = {
    val toDelete = files.headMap(versionID, false).keySet().asScala.toBuffer
    for (versionToDelete <- toDelete) {
      val logFile = files.remove(versionToDelete)
      logFile.deleteFiles()
    }
  }

  def clean(version: Long, versionID: VersionID): Unit = {
    if (files.isEmpty || version <= files.lastKey())
      return //already lowest entry

    //iterate over data at one version, and save the merged result
    val merged = keyValues(version)
    //remove old files
    //TODO check if last file is merged, in that case it does not have to be deleted.
    val logFiles = files.tailMap(version, true).values().asScala.toBuffer
    for (logFile <- logFiles) {
      files.remove(logFile.version)
      logFile.deleteFiles()
    }
    updateSorted(version = version, versionID = versionID, isMerged = true, toUpdate = merged)
    files.put(version, LogFile(version, versionIDLength = versionID.size,
      dir = dir, filePrefix = filePrefix, isMerged = true))
  }

  def close(): Unit = {
    //unmap all buffers
    files.values().asScala.foreach {
      _.close()
    }
  }

  def cleanStop(): Unit = {
  }


  protected[iodb] def merge(version: Long, versionID: VersionID, data: Iterable[(K, V)]): Unit = {
    updateSorted(version, versionID, isMerged = true, toUpdate = data)
    if (keepSingleVersion) {
      //delete all files
      deleteAllFiles()
    }
    files.put(version, LogFile(version = version,
      versionIDLength = versionID.size,
      dir = dir, filePrefix = filePrefix, isMerged = true))
  }

  protected[iodb] def deleteAllFiles(): Unit = {
    for (f <- files.values.asScala) {
      f.deleteFiles()
    }
    files.clear()
  }

  /** returns copy of opened files */
  protected[iodb] def getFiles() = new java.util.TreeMap(files)

  protected[iodb] def countUnmergedVersionsAndSize(): (Long, Long) = {
    var count = 0L
    var size = 0L
    files.asScala.values.find { log =>
      if (!log.isMerged) {
        count += 1
        size += log.buf.limit()
      }
      log.isMerged
    }
    (count, size)
  }

  protected[iodb] def lockFiles(fromVersion: Long, toVersion: Long): Unit = {
    for (file <- files.subMap(fromVersion, toVersion).values().asScala) {
      fileLocks.lock(file.logFile)
    }
  }


  protected[iodb] def unlockFiles(fromVersion: Long, toVersion: Long): Unit = {
    for (file <- files.subMap(fromVersion, toVersion).values().asScala) {
      fileLocks.unlock(file.logFile)
    }
  }

  def fileCount() = files.size()


  def canEqual(other: Any): Boolean = other.isInstanceOf[LogStore]

  override def equals(other: Any): Boolean = {
    val ret = other match {
      case that: LogStore =>
        (that canEqual this) &&
          files == that.files &&
          keySizeExtra == that.keySizeExtra &&
          dir == that.dir &&
          filePrefix == that.filePrefix &&
          keySize == that.keySize &&
          keepSingleVersion == that.keepSingleVersion &&
          fileSync == that.fileSync
      case _ => false
    }
    ret
  }

  override def hashCode(): Int = {
    val state = Seq(files, keySizeExtra, dir, filePrefix, keySize, keepSingleVersion, fileSync)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}


/**
  * Represents single log file.
  * In practice thats are two files (keys, values).
  * Both files are memory mapped on start.
  *
  * @param version  versionID for which LogFile is created
  * @param isMerged true if this file is merged (it contains all values from older versions).
  */
case class LogFile(
                    version: Long,
                    versionIDLength: Int,
                    isMerged: Boolean,
                    dir: File,
                    filePrefix: String) {

  import LogStore._

  def logFile: File = LogStore.logFile(version = version, dir = dir, filePrefix = filePrefix, isMerged)

  def baseKeyOffset: Long = LogStore.headerSizeWithoutVersionID + versionIDLength

  def loadVersionID: VersionID = {
    val w = new ByteArrayWrapper(versionIDLength)
    val buf2 = buf.duplicate()
    buf2.position(LogStore.headerSizeWithoutVersionID)
    buf2.get(w.data)
    return w;
  }

  val buf = mmap(logFile)

  var unmapped = false;

  def deleteFiles(): Unit = {
    unmapped = true
    Utils.unmap(buf)
    fileDelete(logFile)
  }

  def close(): Unit = {
    Utils.unmap(buf)
  }


}

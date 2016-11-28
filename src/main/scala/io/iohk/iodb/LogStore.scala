package io.iohk.iodb

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util.Comparator
import java.util.concurrent.ConcurrentSkipListMap

import com.google.common.collect.Iterators
import io.iohk.iodb.Utils._

import scala.collection.JavaConverters._


object LogStore {

  /** file extension for keys */
  val fileKeyExt: String = ".keys"
  /** file extension for values */
  val fileValueExt: String = ".values"
  /** file extension for merge files */
  val mergedExt: String = ".merged"


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
    * @param version    versionID for which LogFile is created
    * @param dir        directory where file is placed
    * @param filePrefix file name prefix
    * @param isMerged   true if this file is merged (it contains all values from older versions).
    * @return mapped buffer
    */
  def keyFile(version: Long, dir: File, filePrefix: String, isMerged: Boolean = false): File = {
    new File(dir, filePrefix + version + fileKeyExt + (if (isMerged) mergedExt else ""))
  }

  /**
    * Construct value file
    *
    * @param version    versionID for which LogFile is created
    * @param dir        directory where file is placed
    * @param filePrefix file name prefix
    * @param isMerged   true if this file is merged (it contains all values from older versions).
    * @return mapped buffer
    */
  def valueFile(version: Long, dir: File, filePrefix: String, isMerged: Boolean = false): File = {
    new File(dir, filePrefix + version + fileValueExt + (if (isMerged) mergedExt else ""))
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
              ) extends Store {

  import LogStore._

  /*
  There are two files, one with keys, second with values.

  Format of key file:
   - 8 bytes header
   - 8 bytes checksum
   - 8 bytes file size
   - 4 bytes number of keys
   - 4 bytes key size (all keys have the equal size)

  That is followed by keys. Structure of each key is following:
   - byte[] with content of key, size depends on keySize (typically 32)
   - 4 bytes with value size
   - 8 bytes with offset in value file

  Format of values file:
   - 8 bytes header
   - 8 bytes checksum
   - 8 bytes file size
   */


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

  {

    val files2 = dir.listFiles()
      .filter(f => f.getName.startsWith(filePrefix) && f.isFile)
      .map(_.getName().substring(filePrefix.length))
      .filter { f =>
        f.matches("[0-9]+" + fileKeyExt) ||
          f.matches("[0-9]+" + fileKeyExt + mergedExt)
      }
      .map { f =>
        val isMerged = f.endsWith(mergedExt)
        val version = f.split('.')(0).toLong
        LogFile(version = version, isMerged = isMerged, dir = dir, filePrefix = filePrefix)
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
    val keyBuf = logFile.keyBuf.duplicate()
    keyBuf.position(baseKeyOffset.toInt)
    val valueBuf = logFile.valueBuf.duplicate()

    val count = keyBuf.getInt(keyCountOffset)
    (0 until count).map { i =>
      val key = new Array[Byte](keySize)
      keyBuf.get(key)

      //load value
      val valueSize = keyBuf.getInt()
      val valueOffset = keyBuf.getLong
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

  def keyValues(fromVersion: Long, toVersion: Long = Long.MinValue): Iterator[(K, V)] = {

    // compares result of iterators,
    // Second tuple val is Version, is descending so we get only newest version
    object comparator extends Comparator[(K, Long, V)] {
      override def compare(o1: (K, Long, V),
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

    iter
  }

  override def get(key: K): V = {
    val v = get(key, lastVersion)
    if (v == null || v.isEmpty)
      return null
    v.get
  }

  protected[iodb] def get(key: K, versionId: Long, stopAtVersion: Long = 0): Option[V] = {
    if (files.isEmpty)
      return null
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
        return null //contains all versions, will not be found in next versions
    }
    null
  }

  protected def versionGet(logFile: LogFile, key: K): V = {
    val keyBuf = logFile.keyBuf.duplicate()
    val valueBuf = logFile.valueBuf.duplicate()


    def loadValue(): V = {
      //key found, load value
      val valueSize = keyBuf.getInt()
      if (valueSize == -1)
        return tombstone //tombstone, return nothing

      //load value
      val valueOffset = keyBuf.getLong()
      valueBuf.position(valueOffset.toInt)
      val ret = new Array[Byte](valueSize)
      valueBuf.get(ret)
      ByteArrayWrapper(ret)
    }

    if (useUnsafe) {
      val r = Utils.unsafeBinarySearch(logFile.keyBuf, key.data)
      if (r < 0)
        return null
      val keyOffset = baseKeyOffset + r * keySizeExtra
      //load key
      keyBuf.position(keyOffset.toInt + keySize)
      return loadValue
    }

    val key2 = new Array[Byte](keySize)
    val keyCount: Long = keyBuf.getInt(keyCountOffset)
    var lo: Long = 0
    var hi: Long = keyCount - 1

    while (lo <= hi) {

      //split interval
      val mid = (lo + hi) / 2
      val keyOffset = baseKeyOffset + mid * keySizeExtra
      //load key

      keyBuf.position(keyOffset.toInt)
      keyBuf.get(key2)
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


  def update(version: Long, toRemove: Iterable[K],
             toUpdate: Iterable[(K, V)]): Unit = {
    if (lastVersion >= version) {
      throw new IllegalArgumentException("versionID in argument is not greater than Store lastVersion")
    }

    val all = new java.util.TreeMap[K, V].asScala

    var valueFileSize: Long = baseValueOffset
    for ((key, value) <- toUpdate ++ toRemove.map {
      (_, tombstone)
    }) {
      if (key.data.length != keySize)
        throw new IllegalArgumentException("Wrong key size")

      val old = all.put(key, value)
      if (old.isDefined)
        throw new IllegalArgumentException("Duplicate key")

      valueFileSize += value.data.length
    }


    // keys OutputStream
    val keysFile = keyFile(version = version, dir = dir, filePrefix = filePrefix)
    val keysFS = new FileOutputStream(keysFile)
    val keysB = new DataOutputStream(new BufferedOutputStream(keysFS))

    val keysFileSize = baseKeyOffset + keySizeExtra * all.size
    keysB.writeLong(0L) //header
    keysB.writeLong(0L) //checksum
    keysB.writeLong(keysFileSize) //file size
    keysB.writeInt(all.size) //number of keys
    keysB.writeInt(keySize)


    // values OutputStream
    val valuesFile = valueFile(version, dir = dir, filePrefix = filePrefix)
    val valuesFS = new FileOutputStream(valuesFile)
    val valuesB = new DataOutputStream(new BufferedOutputStream(valuesFS))

    valuesB.writeLong(0L) //header
    valuesB.writeLong(0L) //checksum
    valuesB.writeLong(valueFileSize) //file size

    var valueOffset = baseValueOffset
    for ((key, value) <- all) {
      keysB.write(key.data)
      keysB.writeInt(if (value eq tombstone) -1 else value.data.length)
      keysB.writeLong(if (value eq tombstone) -1 else valueOffset)

      if (!(value eq tombstone)) {
        valueOffset += value.data.length
        valuesB.write(value.data)
      }
    }
    assert(valueOffset == valueFileSize)

    keysB.flush()
    keysFS.flush()
    if (fileSync)
      keysFS.getFD.sync()
    assert(keysFileSize == keysFS.getChannel.position())
    keysB.close()
    keysB.close()

    valuesB.flush()
    valuesFS.flush()
    if (fileSync)
      valuesFS.getFD.sync()
    assert(valueOffset == valuesFS.getChannel.position())
    valuesB.close()
    valuesB.close()

    files.put(version, new LogFile(version = version, dir = dir, filePrefix = filePrefix, isMerged = false))
  }


  protected[iohk] def updateSorted(versionId: Long, isMerged: Boolean, toUpdate: Iterator[(K, V)], fileSizeLimit: Int = -1): Unit = {

    // keys OutputStream
    val keysFile = keyFile(version = versionId, dir = dir, filePrefix = filePrefix, isMerged = isMerged)
    val keysFS = new FileOutputStream(keysFile)
    val keysB = new DataOutputStream(new BufferedOutputStream(keysFS))

    keysB.writeLong(0L) //header
    keysB.writeLong(0L) //checksum
    keysB.writeLong(-1L) //file size, will be written latter
    keysB.writeInt(-1) //number of keys, will be written latter
    keysB.writeInt(keySize)


    // values OutputStream
    val valuesFile = valueFile(version = versionId, dir = dir, filePrefix = filePrefix, isMerged = isMerged)
    val valuesFS = new FileOutputStream(valuesFile)
    val valuesB = new DataOutputStream(new BufferedOutputStream(valuesFS))

    valuesB.writeLong(0L) //header
    valuesB.writeLong(0L) //checksum
    valuesB.writeLong(-1) //file size, will be written latter


    var keysCount = 0
    def keysFileSize = baseKeyOffset + keySizeExtra * keysCount
    var valueOffset = baseValueOffset

    //iterate until toUpdate has more entries, or it becomes too big
    while (toUpdate.hasNext && (fileSizeLimit == -1 || keysFileSize < fileSizeLimit)) {
      val (key, value) = toUpdate.next()
      keysB.write(key.data)
      keysB.writeInt(if (value eq tombstone) -1 else value.data.length)
      keysB.writeLong(if (value eq tombstone) -1 else valueOffset)
      keysCount += 1

      if (!(value eq tombstone)) {
        valueOffset += value.data.length
        valuesB.write(value.data)
      }
    }

    keysB.flush()
    assert(keysFileSize == keysFS.getChannel.position())
    //seek back to write file size
    keysFS.getChannel.position(fileSizeOffset)
    val keysB2 = new DataOutputStream(keysFS)
    keysB2.writeLong(keysFileSize) //file size
    keysB2.writeInt(keysCount) //number of keys
    keysB2.flush()

    keysFS.flush()
    if (fileSync)
      keysFS.getFD.sync()
    keysB.close()
    keysB.close()

    valuesB.flush()
    //seek back to write file size
    assert(valueOffset == valuesFS.getChannel.position())
    valuesFS.getChannel.position(fileSizeOffset)
    val valuesB2 = new DataOutputStream(valuesFS)
    valuesB2.writeLong(valueOffset) //file size
    valuesB2.flush()

    valuesFS.flush()
    if (fileSync)
      valuesFS.getFD.sync()
    valuesB.close()
    valuesB.close()
  }

  override def lastVersion: Long = if (files.isEmpty) 0 else files.firstKey()

  /** reverts to older version. Higher (newer) versions are discarded and their versionID can be reused */
  override def rollback(versionID: Long): Unit = {
    val toDelete = files.headMap(versionID, false).keySet().asScala.toBuffer
    for (versionToDelete <- toDelete) {
      val logFile = files.remove(versionToDelete)
      logFile.deleteFiles()
    }
  }

  override def clean(versionId: Long): Unit = {
    if (files.isEmpty || versionId <= files.lastKey())
      return //already lowest entry

    //iterate over data at one version, and save the merged result
    val merged = keyValues(versionId)
    //remove old files
    //TODO check if last file is merged, in that case it does not have to be deleted.
    val logFiles = files.tailMap(versionId, true).values().asScala.toBuffer
    for (logFile <- logFiles) {
      files.remove(logFile.version)
      logFile.deleteFiles()
    }
    updateSorted(versionId, isMerged = true, toUpdate = merged)
    files.put(versionId, LogFile(versionId, dir = dir, filePrefix = filePrefix, isMerged = true))
  }

  override def close(): Unit = {
    //unmap all buffers
    files.values().asScala.foreach {
      _.close()
    }
  }

  override def cleanStop(): Unit = {
  }


  protected[iodb] def merge(versionId: Long, data: Iterator[(K, V)]): Unit = {
    updateSorted(versionId, isMerged = true, toUpdate = data)
    if (keepSingleVersion) {
      //delete all files
      deleteAllFiles()
    }
    files.put(versionId, LogFile(versionId, dir = dir, filePrefix = filePrefix, isMerged = true))
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
        size += log.keyBuf.limit()
      }
      log.isMerged
    }
    (count, size)
  }

  protected[iodb] def lockFiles(fromVersion: Long, toVersion: Long): Unit = {
    for (file <- files.subMap(fromVersion, toVersion).values().asScala) {
      fileLocks.lock(file.keyFile)
    }
  }


  protected[iodb] def unlockFiles(fromVersion: Long, toVersion: Long): Unit = {
    for (file <- files.subMap(fromVersion, toVersion).values().asScala) {
      fileLocks.unlock(file.keyFile)
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
    if (ret == false)
      println("aa")
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
                    isMerged: Boolean,
                    dir: File,
                    filePrefix: String) {

  import LogStore._

  def keyFile: File = LogStore.keyFile(version = version, dir = dir, filePrefix = filePrefix, isMerged)

  def valueFile: File = LogStore.valueFile(version = version, dir = dir, filePrefix = filePrefix, isMerged)

  val keyBuf = mmap(keyFile)
  val valueBuf = mmap(valueFile)

  var unmapped = false;

  def deleteFiles(): Unit = {
    unmapped = true
    Utils.unmap(keyBuf)
    Utils.unmap(valueBuf)
    fileDelete(keyFile)
    fileDelete(valueFile)
  }


  def close(): Unit = {
    Utils.unmap(keyBuf)
    Utils.unmap(valueBuf)
  }


}

package io.iohk.iodb

import java.io.{BufferedOutputStream, DataOutputStream, File, FileOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util.{Collections, Comparator}

import com.google.common.collect.Iterators

import scala.collection.JavaConverters._



/**
  * Single log file
  */
class LogStore(val dir:File, val filePrefix:String, val keySize:Int=32 ) extends Store {


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

  /**
    * Set of active files sorted in descending order (newest first).
    * First value is key file, second value is value file.
    * Java collection is used, it supports mutable iterator.
    */
  protected val files = new java.util.TreeMap[Long, (ByteBuffer, ByteBuffer)](java.util.Collections.reverseOrder[Long]())


  protected val fileKeyExt = ".keys"
  protected val fileValueExt = ".values"

  {
    //load existing files
    dir.listFiles()
        .map(_.getName)
        .filter(_.matches(filePrefix+"[0-9]+"+fileKeyExt)) //get key files
        .map(_.substring(filePrefix.size)) //remove prefix
        .map(s=>s.substring(0,s.size-fileKeyExt.size)) //remove suffix
        .map(_.toLong)
        .foreach { version =>
          val keyBuf = mmap(keyFile(version))
          val valueBuf = mmap(valueFile(version))
          files.put(version, (keyBuf,valueBuf))
        }
  }



  private val checksumOffset = 8 + 8
  private val fileSizeOffset = 8 + 8
  private val keyCountOffset = 8 + 8 + 8
  private val keySizeOffset = keyCountOffset+4

  private val baseKeyOffset: Long = 8 + 8 + 8 + 4 + 4
  private val baseValueOffset: Long = 8 + 8 + 8

  private val keySizeExtra: Long = keySize + 4 + 8

  private val tombstone = new ByteArrayWrapper(new Array[Byte](0))


  protected def keyFile(version:Long) = new File(dir, filePrefix+version+fileKeyExt)
  protected def valueFile(version:Long) = new File(dir, filePrefix+version+fileValueExt)

  protected def mmap(file: File): ByteBuffer = {
    val c = FileChannel.open(file.toPath, StandardOpenOption.READ)
    val ret = c.map(MapMode.READ_ONLY, 0, file.length())
    c.close()
    return ret
  }



  /** iterates over all values in single version. Null value is tombstone. */
  protected def versionIterator(version: Long): Iterator[(K, V)] = {
    var (keyBuf, valueBuf) = files.get(version)
    keyBuf = keyBuf.duplicate()
    keyBuf.position(baseKeyOffset.toInt)
    valueBuf = valueBuf.duplicate()

    val count = keyBuf.getInt(keyCountOffset)
    return (0 until count).map { i =>
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
          new ByteArrayWrapper(value)
        }
      (new ByteArrayWrapper(key), value)
    }.iterator
  }

  def versions:Iterable[Long] = files.keySet().asScala

  def keyValues(versionId:Long):Iterator[(K, V)] = {

    // compares result of iterators,
    // Second tuple val is Version, is descending so we get only newest version
    object comparator extends Comparator[(K, Long, V)]{
      override def compare(o1: (K, Long, V),
                           o2: (K, Long, V)): Int = {
        val c = o1._1.compareTo(o2._1)
        if(c!=0) return c
        return -o1._2.compareTo(o2._2)
      }
    }

    val versions = files.tailMap(versionId, true).keySet().asScala
    // iterator of iterators over all files
    val iters:java.lang.Iterable[java.util.Iterator
        [(K, Long, V)]] = versions.map { version =>
      val iter = versionIterator(version)
      iter.map{e=>
        (e._1, version, e._2)
      }.asJava
    }.asJavaCollection

    //merge multiple iterators, result iterator is sorted union of all iters
    var prevKey:K = null
    val iter = Iterators.mergeSorted[(K, Long, V)] (iters, comparator)
        .asScala
        .filter{it=>
          val include =
            (prevKey == null || //first key
            !prevKey.equals(it._1)) && //is first version of this key
            it._3!=null  // only include if is not tomstone
          prevKey = it._1
          include
        }.map(it=>(it._1, it._3))

    return iter;
  }


  override def get(key:K):V = get(key, lastVersion)

  def get(key:K, versionId:Long): V ={
    val versions = files.tailMap(versionId).keySet().asScala
    for(version <- versions){
      val ret = versionGet(version, key)
      if(tombstone eq ret)
        return null
      if(ret!=null)
        return ret
    }
    return null;
  }


  protected def versionGet(versionId:Long, key:K): V ={
    var (keyBuf,valueBuf) = files.get(versionId)
    keyBuf = keyBuf.duplicate()
    valueBuf = valueBuf.duplicate()
    val keyCount: Long = keyBuf.getInt(keyCountOffset)

    var lo: Long = 0
    var hi: Long = keyCount - 1

    val key2 = new Array[Byte](keySize)
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
        //key found, load value
        val valueSize = keyBuf.getInt()
        if (valueSize == -1)
          return tombstone //tombstone, return nothing

        //load value
        val valueOffset = keyBuf.getLong()
        valueBuf.position(valueOffset.toInt)
        val ret = new Array[Byte](valueSize)
        valueBuf.get(ret)
        return new ByteArrayWrapper(ret)
      }
    }
    return null
  }



  def update(version:Long, toRemove:Iterable[K],
             toUpdate:Iterable[(K, V)]): Unit ={
    val all = new java.util.TreeMap[K, V].asScala

    var valueFileSize: Long = baseValueOffset
    for ((key, value) <- toUpdate ++ toRemove.map {
      (_, tombstone)
    }) {
      if (key.data.length != keySize)
        throw new IllegalArgumentException("Wrong key size")

      val old = all.put(key, value)
      if (!old.isEmpty)
        throw new IllegalArgumentException("Duplicate key")

      valueFileSize += value.data.length
    }


    // keys OutputStream
    val keysFile = keyFile(version)
    val keysFS = new FileOutputStream(keysFile);
    val keysB = new DataOutputStream(new BufferedOutputStream(keysFS))

    val keysFileSize = baseKeyOffset + keySizeExtra * all.size
    keysB.writeLong(0L) //header
    keysB.writeLong(0L) //checksum
    keysB.writeLong(keysFileSize) //file size
    keysB.writeInt(all.size) //number of keys
    keysB.writeInt(keySize)


    // values OutputStream
    val valuesFile = valueFile(version)
    val valuesFS = new FileOutputStream(valuesFile);
    val valuesB = new DataOutputStream(new BufferedOutputStream(valuesFS))

    valuesB.writeLong(0L) //header
    valuesB.writeLong(0L) //checksum
    valuesB.writeLong(valueFileSize) //file size

    var valueOffset = baseValueOffset
    for ((key, value) <- all) {
      keysB.write(key.data)
      keysB.writeInt(if (value eq tombstone) -1 else value.data.length)
      keysB.writeLong(if (value eq tombstone) -1 else valueOffset)

      if(!(value eq tombstone)){
        valueOffset += value.data.length
        valuesB.write(value.data)
      }
    }
    assert(valueOffset == valueFileSize)

    keysB.flush()
    keysFS.flush()
    keysFS.getFD.sync() //TODO sync can be optional
    assert(keysFileSize == keysFS.getChannel.position())
    keysB.close()
    keysB.close()

    valuesB.flush()
    valuesFS.flush()
    valuesFS.getFD.sync()
    assert(valueOffset == valuesFS.getChannel.position())
    valuesB.close()
    valuesB.close()

    files.put(version, (mmap(keysFile), mmap(valuesFile)))
  }


  protected def updateSorted(versionId: Long, toUpdate: Iterable[(K, V)]): Unit = {


    // keys OutputStream
    val keysFile = keyFile(versionId)
    val keysFS = new FileOutputStream(keysFile);
    val keysB = new DataOutputStream(new BufferedOutputStream(keysFS))

    keysB.writeLong(0L) //header
    keysB.writeLong(0L) //checksum
    keysB.writeLong(-1L) //file size, will be written latter
    keysB.writeInt(-1) //number of keys, will be written latter
    keysB.writeInt(keySize)

    // values OutputStream
    val valuesFile = valueFile(versionId)
    val valuesFS = new FileOutputStream(valuesFile);
    val valuesB = new DataOutputStream(new BufferedOutputStream(valuesFS))

    valuesB.writeLong(0L) //header
    valuesB.writeLong(0L) //checksum
    valuesB.writeLong(-1) //file size, will be written latter


    var keysCount = 0;
    var valueOffset = baseValueOffset
    for ((key, value) <- toUpdate) {
      keysB.write(key.data)
      keysB.writeInt(if (value eq tombstone) -1 else value.data.length)
      keysB.writeLong(if (value eq tombstone) -1 else valueOffset)
      keysCount+=1

      if(!(value eq tombstone)){
        valueOffset += value.data.length
        valuesB.write(value.data)
      }
    }

    val keysFileSize = baseKeyOffset + keySizeExtra * keysCount
    keysB.flush()
    assert(keysFileSize == keysFS.getChannel.position())
    //seek back to write file size
    keysFS.getChannel.position(fileSizeOffset)
    val keysB2 = new DataOutputStream(keysFS)
    keysB2.writeLong(keysFileSize) //file size
    keysB2.writeInt(keysCount) //number of keys
    keysB2.flush()

    keysFS.flush()
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
    valuesFS.getFD.sync()
    valuesB.close()
    valuesB.close()
  }

  override def lastVersion: Long = files.firstKey()

  /** reverts to older version. Higher (newer) versions are discarded and their versionID can be reused */
  override def rollback(versionID: Long): Unit = {
    val toDelete = files.headMap(versionID, false).keySet().asScala.toBuffer
    for(versionToDelete <- toDelete){
        fileDelete(keyFile(versionToDelete))
        fileDelete(valueFile(versionToDelete))
        files.remove(versionToDelete)
    }
  }

  protected def fileDelete(f:File): Unit ={
    assert(f.exists())
    var deleted = f.delete()
    assert(deleted)
  }

  override def clean(): Unit = {
  }

  override def close(): Unit = {
  }

  override def cleanStop(): Unit = {
  }

}

package io.iohk.iodb

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util.Comparator
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.google.common.collect.Iterators

import scala.collection.JavaConverters._


/**
  * Store which uses Log-Structured-Merge tree
  */
class LSMStore(dir: File, keySize: Int = 32, keepLastN: Int = 10) extends Store {

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



  private val checksumOffset = 8 + 8
  private val fileSizeOffset = 8 + 8
  private val keyCountOffset = 8 + 8 + 8
  private val keySizeOffset = keyCountOffset+4


  private val filePrefix = "store"

  private val fileKeyExt = ".keys"
  private val fileValueExt = ".values"

  private val baseKeyOffset: Long = 8 + 8 + 8 + 4 + 4
  private val baseValueOffset: Long = 8 + 8 + 8

  private val keySizeExtra: Long = keySize + 4 + 8

  private val lock = new ReentrantReadWriteLock()

  /**
    * Set of active files sorted in descending order (newest first).
    * First value is key file, second value is value file.
    * Java collection is used, it supports mutable iterator.
    */
  private val files = new java.util.TreeMap[Long, (ByteBuffer, ByteBuffer)](java.util.Collections.reverseOrder[Long]())

  private var version: Long = 0
  private val tombstone: V = new ByteArrayWrapper(new Array[Byte](0))

  override def lastVersion: Long = version


  {
    val l = filePrefix.size
    val l2 = fileKeyExt.size
    val pattern = (filePrefix + "[0-9]+"+fileKeyExt).r
    //find newest version
    val files2 = dir.listFiles()
      .map(_.getName())
      .filter(pattern.pattern.matcher(_).matches())
      .map(_.substring(l)) //remove prefix
      .map(s=> s.substring(0, s.size-l2).toLong) //remove extension and convert to long

    files2.foreach(version => files.put(version, mmap(version)))

    version = if (files.isEmpty) 0 else files.firstKey
  }

  override def update(versionID: Long, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    lock.writeLock().lock()
    try {
      if (files.containsKey(versionID))
        throw new IllegalArgumentException("Version already exists")

      updateFile(versionID, toRemove, toUpdate)
      val oldVal = files.put(versionID, mmap(versionID))
      assert(oldVal == null)
      this.version = versionID
    } finally {
      lock.writeLock().unlock()
    }
  }

  protected def updateFile(versionId: Long, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
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
    val keysFile = keyFile(versionId)
    val keysFS = new FileOutputStream(keysFile);
    val keysB = new DataOutputStream(new BufferedOutputStream(keysFS))

    keysB.writeLong(0L) //header
    keysB.writeLong(0L) //checksum
    keysB.writeLong(baseKeyOffset + keySizeExtra * all.size) //file size
    keysB.writeInt(all.size) //number of keys
    keysB.writeInt(keySize)


    // values OutputStream
    val valuesFile = valueFile(versionId)
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
    keysFS.getFD.sync()
    keysB.close()
    keysB.close()

    valuesB.flush()
    valuesFS.flush()
    valuesFS.getFD.sync()
    valuesB.close()
    valuesB.close()
  }


  override def get(key: K): V = {
    lock.readLock.lock()
    try {
      for ((keyBuf2, valueBuf2) <- files.values.iterator().asScala) {
        val keyBuf = keyBuf2.duplicate()
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
              return null //tombstone, return nothing

            //load value
            val valueOffset = keyBuf.getLong()
            val valueBuf = valueBuf2.duplicate()
            valueBuf.position(valueOffset.toInt)
            val ret = new Array[Byte](valueSize)
            valueBuf.get(ret)
            return new ByteArrayWrapper(ret)
          }
        }
      }
      return null
    } finally {
      lock.readLock().unlock()
    }
  }


  override def rollback(versionID: Long): Unit = {
    lock.writeLock().lock()
    try {
      val iter = files.entrySet().iterator
      while (iter.hasNext) {
        val v = iter.next()
        version = v.getKey
        if (version <= versionID)
          return //reached previous version, finish iteration
        //move to prev version
        fileDelete(version)
        iter.remove()
      }
    } finally {
      lock.writeLock().unlock()
    }
  }


  override def close(){

  }

  override def clean() {
    lock.writeLock().lock()
    try{

      //collect data from old files
      if(files.size()<=keepLastN+1)
        return

      val oldFiles= files.asScala.keys.drop(keepLastN)

      // iterator of iterators over all files
      val iters:java.lang.Iterable[java.util.Iterator[(K, Long, V)]] =
        oldFiles.map { version =>
          val iter = fileIter(version)
          iter.map{e=>
            (e._1, version, e._2)
          }.asJava
        }.asJavaCollection


      // compares result of iterators,
      // Second tuple val is Version, is descending so we get only newest version
      object comparator extends Comparator[(K, Long, V)]{
        override def compare(o1: (K, Long, V), o2: (K, Long, V)): Int = {
          val c = o1._1.compareTo(o2._1)
          if(c!=0) return c
          return -o1._2.compareTo(o2._2)
        }
      }

      val result = new scala.collection.mutable.ArrayBuffer[(K,V)]()

      //merge multiple iterators, result iterator is sorted union of all iters
      val iter = Iterators.mergeSorted[(K, Long, V)](iters, comparator)
      //iterator over sorted data, most recent version of key is first, followed by older versions
      var prevKey:K = null;
      while(iter.hasNext){
        val e = iter.next()
        val key = e._1
        if(key!=prevKey){ //skip older versions of keys
          val value = e._3
          if(value!=null){ //null value means that key was deleted
            //got newest version if the new key
            result+=((key,value))
          }
          prevKey = key
        }
      }

      //FIXME this section has no crash recovery, old file might get deleted before compacted file is finished

      //delete old files
      oldFiles.foreach{ v:Long=>
        files.remove(v)
        fileDelete(v)
      }
      //save into new file
      val lastVersion = oldFiles.head
      updateFile(lastVersion, toRemove = List.empty,  toUpdate = result)

      files.put(lastVersion, mmap(lastVersion))

    }finally{
      lock.writeLock.unlock()
    }
  }

  protected def fileIter(version: Long): Iterator[(ByteArrayWrapper, ByteArrayWrapper)] = {
    var (keyBuf, valueBuf) = files.get(version)
    keyBuf = keyBuf.duplicate()
    keyBuf.position(baseKeyOffset.toInt)
    valueBuf = valueBuf.duplicate()

    val count = keyBuf.getInt(keyCountOffset)
    val iter = (0 until count).map { i =>
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
    }
    return iter.iterator
  }


  override def cleanStop(): Unit = {

  }

  protected def keyFile(version:Long) = new File(dir, filePrefix+version+fileKeyExt)
  protected def valueFile(version:Long) = new File(dir, filePrefix+version+fileValueExt)

  protected def mmap(version: Long): (ByteBuffer, ByteBuffer) = {
    return (mmap(keyFile(version)), mmap(valueFile(version)))
  }

  protected def mmap(file: File): ByteBuffer = {
    val c = FileChannel.open(file.toPath, StandardOpenOption.READ)
    val ret = c.map(MapMode.READ_ONLY, 0, file.length())
    c.close()
    return ret
  }

  protected def fileDelete(version:Long){
    var f = keyFile(version)
    assert(f.exists())
    var deleted = f.delete()
    assert(deleted)

    f = valueFile(version)
    assert(f.exists())
    deleted = f.delete()
    assert(deleted)
  }

  protected def writeLong(c: FileChannel, value:Long, offset:Long = -1L){
    if(offset != -1L)
      c.position(offset)
    val b = ByteBuffer.allocate(8)
    b.putLong(0, value)
    Utils.writeFully(c,b)
  }


  protected def writeInt(c: FileChannel, value:Int, offset:Long = -1L){
    if(offset != -1L)
      c.position(offset)
    val b = ByteBuffer.allocate(4)
    b.putInt(0, value)
    Utils.writeFully(c,b)
  }
}

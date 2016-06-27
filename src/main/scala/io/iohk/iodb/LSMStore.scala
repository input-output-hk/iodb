package io.iohk.iodb

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._


/**
  * Store which uses Log-Structured-Merge tree
  */
class LSMStore(dir: File, keySize: Int = 32, keepLastN: Int = 10) extends Store {

  /*
  Format:

   - 8 bytes header
   - 8 bytes checksum
   - 8 bytes total size
   - 4 bytes number of keys
   - 4 bytes key size (all keys have the equal size)


   */

  private val keyCountOffset = 8 + 8 + 8

  private val filePrefix = "store"

  private val baseOffset: Long = 8 + 8 + 8 + 4 + 4
  private val keySizeExtra: Long = keySize + 4 + 8

  private val lock = new ReentrantReadWriteLock()

  /**
    * Set of active files sorted in descending order (newest first).
    * Java collection is used, it supports mutable iterator.
    */
  private val files = new java.util.TreeMap[Long, ByteBuffer](java.util.Collections.reverseOrder[Long]())

  private var version: Long = 0
  private val tombstone: V = new ByteArrayWrapper(new Array[Byte](0))

  override def lastVersion: Long = version


  {
    val l = filePrefix.size
    val pattern = (filePrefix + "[0-9]+").r
    //find newest version
    val files2 = dir.listFiles()
      .map(_.getName())
      .filter(pattern.pattern.matcher(_).matches())
      .map(_.substring(l).toLong)

    files2.foreach(version => files.put(version, mmap(version)))

    version = if (files.isEmpty) 0 else files.firstKey
  }

  override def update(versionID: Long, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    lock.writeLock().lock()
    try {
      if (files.containsKey(versionID))
        throw new IllegalArgumentException("Version already exists")

      val all = new java.util.TreeMap[K, V].asScala

      var fileSize: Long = baseOffset


      for ((key, value) <- toUpdate ++ toRemove.map {
        (_, tombstone)
      }) {
        if (key.data.length != keySize)
          throw new IllegalArgumentException("Wrong key size")

        val old = all.put(key, value)
        if (!old.isEmpty)
          throw new IllegalArgumentException("Duplicate key")

        fileSize += keySize + 4 + 8 + value.data.length
      }


      val f = new File(dir, filePrefix + versionID)
      assert(!f.exists())
      val fout = new FileOutputStream(f);
      val out = new DataOutputStream(new BufferedOutputStream(fout))

      out.writeLong(0L) //header
      out.writeLong(0L) //checksum
      out.writeLong(fileSize) //file size
      out.writeInt(all.size) //number of keys
      out.writeInt(keySize)

      var valueOffset = baseOffset + keySizeExtra * all.size
      for ((key, value) <- all) {
        out.write(key.data)
        val valueSize = if (value eq tombstone) -1 else value.data.length
        out.writeInt(valueSize)
        out.writeLong(valueOffset)
        valueOffset += value.data.length
      }
      assert(valueOffset == fileSize)

      for (value <- all.values) {
        out.write(value.data)
      }

      out.flush()
      fout.flush()
      fout.getFD.sync()
      out.close()
      fout.close()
      val oldVal = files.put(versionID, mmap(versionID))
      assert(oldVal == null)
      this.version = versionID
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def get(key: K): V = {
    lock.readLock.lock()
    try {
      for (c <- files.values.iterator().asScala) {
        val keyCount: Long = c.getInt(keyCountOffset)
        val c2 = c.duplicate()

        var lo: Long = 0
        var hi: Long = keyCount - 1

        val key2 = new Array[Byte](keySize)
        while (lo <= hi) {
          //split interval
          val mid = (lo + hi) / 2
          val keyOffset = baseOffset + mid * keySizeExtra
          //load key

          c2.position(keyOffset.toInt)
          c2.get(key2)
          //compare keys and split intervals if not equal
          val comp = Utils.BYTE_ARRAY_COMPARATOR.compare(key2, key.data)
          if (comp < 0) lo = mid + 1
          else if (comp > 0) hi = mid - 1
          else {
            //key found, load value
            val valueSize = c2.getInt()
            if (valueSize == -1)
              return null
            val valueOffset = c2.getLong()

            c2.position(valueOffset.toInt)
            val ret = new Array[Byte](valueSize)
            c2.get(ret)
            return new ByteArrayWrapper(ret)
          }

        }

      }
      return null
    } finally {
      lock.readLock().unlock()
    }
  }

  /** get value using RandomAccessFile */
  private def getRaf(key: K): V = {
    lock.readLock.lock()
    try {
      for (versionId <- files.keySet.iterator().asScala) {
        val f = new File(dir, filePrefix + versionId)
        val raf = new RandomAccessFile(f, "r")
        raf.seek(keyCountOffset)
        val keyCount: Long = raf.readInt()

        var lo: Long = 0
        var hi: Long = keyCount - 1

        val key2 = new Array[Byte](keySize)
        while (lo <= hi) {
          //split interval
          val mid = (lo + hi) / 2
          val keyOffset = baseOffset + mid * keySizeExtra
          //load key
          raf.seek(keyOffset)
          raf.readFully(key2)
          //compare keys and split intervals if not equal
          val comp = Utils.BYTE_ARRAY_COMPARATOR.compare(key2, key.data)
          if (comp < 0) lo = mid + 1
          else if (comp > 0) hi = mid - 1
          else {
            //key found, load value
            val valueSize = raf.readInt()
            if (valueSize == -1)
              return null
            val valueOffset = raf.readLong()

            raf.seek(valueOffset)
            val ret = new Array[Byte](valueSize)
            raf.readFully(ret)
            raf.close()
            return new ByteArrayWrapper(ret)
          }

        }
        raf.close()
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
        new File(dir, filePrefix + v).delete()
        iter.remove()
      }
    } finally {
      lock.writeLock().unlock()
    }
  }


  override def close(): Unit = {

  }

  override def clean() {

  }

  private def fileIter(version: Long): Iterator[(ByteArrayWrapper, ByteArrayWrapper)] = {
    val buf = files.get(version).duplicate()
    val count = buf.getInt(24)
    val iter = (0 until count).map { i =>
      val keyOffset = baseOffset + i * keySizeExtra
      val key = new Array[Byte](keySize)
      buf.position(keyOffset.toInt)
      buf.get(key)

      //load value
      val valueSize = buf.getInt()
      val value =
        if (valueSize == -1) null
        else {
          val valueOffset = buf.getLong
          buf.position(valueOffset.toInt)
          val value = new Array[Byte](valueSize)
          buf.get(value)
          new ByteArrayWrapper(value)
        }
      (new ByteArrayWrapper(key), value)
    }
    return iter.iterator
  }


  override def cleanStop(): Unit = {

  }

  def mmap(version: Long): ByteBuffer = {
    val f = new File(dir, filePrefix + version)
    val c = FileChannel.open(f.toPath, StandardOpenOption.READ)
    val ret = c.map(MapMode.READ_ONLY, 0, f.length())
    c.close()
    return ret
  }

}

package io.iohk.iodb

import java.io.File
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption

import io.iohk.iodb.Store._
import io.iohk.iodb.Utils.keyCountOffset

/**
  * Different ways to access files (RandomAccessFile, memory-mapped, direct mmap with Unsafe)
  */
sealed abstract class FileAccess {


  def getValue(fileHandle: Any, key: K, keySize: Int, baseKeyOffset: Long): V


  /**
    * gets file size of given file
    *
    * @param fileHandle for opened file
    * @return file size
    */
  def fileSize(fileHandle: Any): Long


  /**
    * Reads data from given offset
    *
    * @param fileHandle
    * @param offset
    * @param data
    */

  def readData(fileHandle: Any, offset: Int, data: Array[Byte]): Unit

  /**
    * Read all key-value pairs from given log file
    *
    * @param fileHandle
    * @param baseKeyOffset offset where keys are starting
    * @param keySize       size of key
    * @return iterator over key-value pairs
    */
  def readKeyValues(fileHandle: Any, baseKeyOffset: Long, keySize: Int): Iterator[(K, V)]

  /**
    * Opens new file
    *
    * @param fileName name of file
    * @return handle for opened file in given file access method
    */
  def open(fileName: String): Any

  /**
    * Close file and release all resources associated  with file
    *
    * @param file handle for opened file
    */
  def close(file: Any)


}

object FileAccess {

  /** use memory mapped files */
  object MMAP extends FileAccess {

    override def open(fileName: String): MappedByteBuffer = mmap(fileName)


    override def close(file: Any): Unit = {
      Utils.unmap(buf(file))
    }

    /**
      * Reads data from given offset
      *
      * @param fileHandle
      * @param offset
      * @param data
      */
    override def readData(fileHandle: Any, offset: Int, data: Array[Byte]): Unit = {
      val buf2 = buf(fileHandle).duplicate()
      buf2.position(LogStore.headerSizeWithoutVersionID)
      buf2.get(data)
    }

    /**
      * Read all key-value pairs from given log file
      *
      * @param fileHandle
      * @return iterator over key-value pairs
      */
    override def readKeyValues(fileHandle: Any, baseKeyOffset: Long, keySize: Int): Iterator[(K, V)] = {
      val buf = FileAccess.buf(fileHandle).duplicate()
      val valueBuf = buf.duplicate()
      buf.position(baseKeyOffset.toInt)

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

    /**
      * gets file size of given file
      *
      * @param fileHandle for opened file
      * @return file size
      */
    override def fileSize(fileHandle: Any): Long = buf(fileHandle).limit()

    override def getValue(fileHandle: Any, key: K, keySize: Int, baseKeyOffset: Long): V = {

      val buf = FileAccess.buf(fileHandle).duplicate()

      def loadValue(): V = {
        //key found, load value
        val valueSize = buf.getInt()
        if (valueSize == -1)
          return LogStore.tombstone //tombstone, return nothing

        //load value
        val valueOffset = buf.getLong()
        buf.position(valueOffset.toInt)
        val ret = new Array[Byte](valueSize)
        buf.get(ret)
        ByteArrayWrapper(ret)
      }

      val keySizeExtra = keySize + 4 + 8
      //    if (useUnsafe) {
      //      val r = Utils.unsafeBinarySearch(logFile.buf, key.data, logFile.baseKeyOffset.toInt)
      //      if (r < 0)
      //        return null
      //      val keyOffset = logFile.baseKeyOffset + r * keySizeExtra
      //      //load key
      //      buf.position(keyOffset.toInt + keySize)
      //      return loadValue
      //    }

      val key2 = new Array[Byte](keySize)
      val keyCount: Long = buf.getInt(keyCountOffset)
      var lo: Long = 0
      var hi: Long = keyCount - 1

      while (lo <= hi) {

        //split interval
        val mid = (lo + hi) / 2
        val keyOffset = baseKeyOffset + mid * keySizeExtra
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
  }

  /**
    * Memory maps file into read-only ByteBuffer. File must be smaller than 2GB due to addressing limit.
    *
    * @param fileName to be mapped
    * @return ByteByffer of memory mapped file
    */
  protected def mmap(fileName: String): MappedByteBuffer = {
    val file = new File(fileName)
    val c = FileChannel.open(file.toPath, StandardOpenOption.READ)
    val ret = c.map(MapMode.READ_ONLY, 0, file.length())
    c.close()
    ret
  }

  protected def buf(fileHandle: Any) = fileHandle.asInstanceOf[MappedByteBuffer]

}
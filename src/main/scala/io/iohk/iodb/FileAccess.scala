package io.iohk.iodb

import java.io.File
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.nio.{ByteBuffer, MappedByteBuffer}

import io.iohk.iodb.Store._

/**
  * Different ways to access files (RandomAccessFile, memory-mapped, direct mmap with Unsafe)
  */
sealed abstract class FileAccess {

  def getValue(fileHandle: Any, key: K, keySize: Int, updateOffset: Long): Option[V]


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

  def readData(fileHandle: Any, offset: Long, data: Array[Byte]): Unit

  /**
    * Read all key-value pairs from given log file
    *
    * @param fileHandle
    * @param offset  offset where keys are starting
    * @param keySize size of key
    * @return iterator over key-value pairs
    */
  def readKeyValues(fileHandle: Any, offset: Long, keySize: Int): Iterator[(K, V)]

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

  /** If file size changes, the file handle must change as well.
    * Usuall case is when file expands and the `ByteBuffer` expands
    */
  def expandFileSize(file: Any): Any = file

  def readLong(file: Any, offset: Long): Long = {
    val b = ByteBuffer.allocate(8)
    readData(file, offset, b.array())
    return b.getLong(0)
  }

  def readInt(file: Any, offset: Long): Int = {
    val b = ByteBuffer.allocate(4)
    readData(file, offset, b.array())
    return b.getInt(0)
  }

  def readByte(file: Any, offset: Long): Byte = {
    val b = ByteBuffer.allocate(1)
    readData(file, offset, b.array())
    return b.get(0)
  }
}

object FileAccess {

  abstract class ByteBufferFileAccess extends FileAccess {

    /** cast handle to MappedByteBuffer */
    protected[iodb] def castBuf(fileHandle: Any): MappedByteBuffer =
      fileHandle.asInstanceOf[(MappedByteBuffer, File)]._1

    /** cast handle to File */
    protected[iodb] def castFile(fileHandle: Any): File =
      fileHandle.asInstanceOf[(MappedByteBuffer, File)]._2

    protected def checkBufferSize(file: Any): Unit = {
      val bufSize = castBuf(file).limit
      val fileSize = castFile(file).length()
      assert(bufSize == fileSize, "File size has changed")
    }

    override def open(fileName: String): (MappedByteBuffer, File) = (mmap(fileName), new File(fileName))


    /** If file size changes, the file handle must change as well.
      * Usuall case is when file expands and the `ByteBuffer` expands
      */
    override def expandFileSize(fileHandle: Any): Any = {
      val buf = castBuf(fileHandle)
      val file = castFile(fileHandle)
      if (buf.limit() == file.length())
        return fileHandle

      //file size has changed, remap
      Utils.unmap(buf)
      return (mmap(file.getPath), file)
    }

    override def close(file: Any): Unit = {
      Utils.unmap(castBuf(file))
    }

    /**
      * gets file size of given file
      *
      * @param fileHandle for opened file
      * @return file size
      */
    override def fileSize(fileHandle: Any): Long = castFile(fileHandle).length()

    //
    /**
      * Reads data from given offset
      *
      * @param fileHandle
      * @param offset
      * @param data
      */
    override def readData(fileHandle: Any, offset: Long, data: Array[Byte]): Unit = {
      checkBufferSize(fileHandle)
      val buf2 = castBuf(fileHandle).duplicate()
      buf2.position(offset.toInt)
      buf2.get(data)
    }

    /**
      * Read all key-value pairs from given log file
      *
      * @param fileHandle
      * @return iterator over key-value pairs
      */
    override def readKeyValues(fileHandle: Any, offset: Long, keySize: Int): Iterator[(K, V)] = {
      checkBufferSize(fileHandle)
      val buf = castBuf(fileHandle).duplicate()

      val updateSize = buf.getInt(offset.toInt + 8)
      val keyCount = buf.getInt(offset.toInt + 8 + 4)
      assert(keyCount * keySize >= 0 && keyCount * keySize < updateSize)

      val baseKeyOffset = offset.toInt + LSMStore.updateHeaderSize

      return (0 until keyCount).map { i =>
        val keyOffset = baseKeyOffset + i * keySize
        buf.position(keyOffset)
        val key = new Array[Byte](keySize)
        buf.get(key)

        //read pointers to value
        buf.position(baseKeyOffset + keyCount * keySize + i * 8)
        val valueSize = buf.getInt()
        val value =
          if (valueSize == -1) Store.tombstone
          else {
            val valueOffset = offset + buf.getInt()
            buf.position(valueOffset.toInt)
            val value = new Array[Byte](valueSize)
            buf.get(value)
            ByteArrayWrapper(value)
          }
        (ByteArrayWrapper(key), value)
      }.iterator
    }

  }


  /** use memory mapped files, fast but can cause problem on Windows.  */
  object MMAP extends ByteBufferFileAccess {

    override def getValue(fileHandle: Any, key: K, keySize: Int, updateOffset: Long): Option[V] = {
      assert(updateOffset >= 0, "negative updateOffset: " + updateOffset)
      checkBufferSize(fileHandle)
      val buf = castBuf(fileHandle).duplicate()

      val keyCount = buf.getInt(updateOffset.toInt + 8 + 4)
      val baseKeyOffset = updateOffset + LSMStore.updateHeaderSize

      val key2 = new Array[Byte](keySize)
      var lo: Int = 0
      var hi: Int = keyCount - 1

      while (lo <= hi) {
        //split interval
        val mid = (lo + hi) / 2
        val keyOffset = baseKeyOffset + mid * keySize
        //load key
        buf.position(keyOffset.toInt)
        buf.get(key2)
        //compare keys and split intervals if not equal
        val comp = Utils.BYTE_ARRAY_COMPARATOR.compare(key2, key.data)
        if (comp < 0) lo = mid + 1
        else if (comp > 0) hi = mid - 1
        else {
          //key found, load value
          val valuePointersOffset = baseKeyOffset + keyCount * keySize + mid * 8
          val valueSize = buf.getInt(valuePointersOffset.toInt)
          if (valueSize == -1)
            return None //tombstone, return nothing

          //load value
          val valueOffset = buf.getInt(valuePointersOffset.toInt + 4)
          buf.position(updateOffset.toInt + valueOffset)
          val ret = new Array[Byte](valueSize)
          buf.get(ret)
          return Some(ByteArrayWrapper(ret))
        }
      }
      return null
    }
  }

  /** Use `sun.misc.Unsafe` with direct memory access. Very fast, but can cause JVM  and has problems on 32bit systems and Windows. */
  object UNSAFE extends ByteBufferFileAccess {
    override def getValue(fileHandle: Any, key: K, keySize: Int, updateOffset: Long): Option[V] = {
      checkBufferSize(fileHandle)
      val buf = castBuf(fileHandle).duplicate()

      val keyCount = buf.getInt(updateOffset.toInt + 8 + 4)
      val baseKeyOffset = updateOffset + LSMStore.updateHeaderSize

      val r = Utils.unsafeBinarySearch(buf, key.data, baseKeyOffset.toInt, keyCount)
      if (r < 0)
        return null

      //key found, load value
      val valuePointersOffset = baseKeyOffset + keyCount * keySize + r * 8
      val valueSize = buf.getInt(valuePointersOffset.toInt)
      if (valueSize == -1)
        return None //tombstone, return nothing

      //load value
      val valueOffset = buf.getInt(valuePointersOffset.toInt + 4)
      buf.position(updateOffset.toInt + valueOffset)
      val ret = new Array[Byte](valueSize)
      buf.get(ret)
      return Some(ByteArrayWrapper(ret))
    }

  }

  /**
    * Use `FileChannel` to access files. Slower, but safer. Keeps many file handles open,
    * and might cause crash if 'maximal number of open files per process' is exceed.
    */
  object FILE_CHANNEL extends FileAccess {

    protected def cast(fileHandle: Any) = fileHandle.asInstanceOf[FileChannel]

    override def getValue(fileHandle: Any, key: K, keySize: Int, updateOffset: Long): Option[V] = {
      assert(updateOffset >= 0, "negative updateOffset: " + updateOffset)
      val c = cast(fileHandle)
      val tempBuf = ByteBuffer.allocate(8)

      val keyCount: Long = readInt(c, updateOffset + LogStore.updateKeyCountOffset, tempBuf)

      val baseKeyOffset = updateOffset + LSMStore.updateHeaderSize

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
          val valueSize = readInt(c, valuePointersOffset, tempBuf)
          if (valueSize == -1)
            return None
          //tombstone, return nothing
          val valueOffset = readInt(c, valuePointersOffset + 4, tempBuf)
          //load value
          return Some(readData(c, updateOffset + valueOffset, valueSize))
        }
      }
      null
    }

    protected def readData(c: FileChannel, offset: Long, size: Int): ByteArrayWrapper = {
      val ret = new Array[Byte](size)
      val ret2 = ByteBuffer.wrap(ret)
      Utils.readFully(c, offset, ret2)
      return ByteArrayWrapper(ret)
    }

    protected def readLong(c: FileChannel, offset: Long, buf: ByteBuffer = ByteBuffer.allocate(8)): Long = {
      buf.clear();
      buf.limit(8)
      Utils.readFully(c, offset, buf)
      return buf.getLong(0)
    }


    protected def readInt(c: FileChannel, offset: Long, buf: ByteBuffer = ByteBuffer.allocate(4)): Int = {
      buf.position(0);
      buf.limit(4)
      Utils.readFully(c, offset, buf)
      return buf.getInt(0)
    }

    override def readData(fileHandle: Any, offset: Long, data: Array[Byte]): Unit = {
      val c = cast(fileHandle)
      val b = ByteBuffer.wrap(data)
      c.position(offset.toInt)
      Utils.readFully(c, offset, b)
    }

    override def readKeyValues(fileHandle: Any, offset: Long, keySize: Int): Iterator[(K, V)] = {
      assert(offset >= 0)
      val c = cast(fileHandle)
      val tempBuf = ByteBuffer.allocate(8)

      //get size
      val updateSize = readInt(c, offset, tempBuf)
      val keyCount = readInt(c, offset + LogStore.updateKeyCountOffset, tempBuf)
      assert(keyCount * keySize >= 0 && keyCount * keySize < updateSize)

      val baseKeyOffset = offset + LSMStore.updateHeaderSize

      return (0 until keyCount).map { i =>
        val keyOffset = baseKeyOffset + i * keySize
        val key = readData(c, keyOffset, keySize)

        val pointersOffsets = baseKeyOffset + keyCount * keySize + i * 8
        val valueSize = readInt(c, pointersOffsets, tempBuf)
        val value =
          if (valueSize == -1) Store.tombstone
          else {
            val valueOffset = readInt(c, pointersOffsets + 4, tempBuf)
            readData(c, offset + valueOffset, valueSize)
          }
        (key, value)
      }.iterator
    }

    override def open(fileName: String): Any = {
      FileChannel.open(new File(fileName).toPath, StandardOpenOption.READ)
    }

    override def fileSize(fileHandle: Any): Long = cast(fileHandle).size()

    override def close(file: Any): Unit = cast(file).close()

  }

  /**
    * Use `FileChannel` to access files, no file handles are kept open.
    * Slower and safer.
    */

  object SAFE extends FileAccess {

    protected def cast(fileHandle: Any) = fileHandle.asInstanceOf[File]

    protected def open2(fileHandle: Any) = FileChannel.open(cast(fileHandle).toPath, StandardOpenOption.READ)

    override def getValue(fileHandle: Any, key: K, keySize: Int, updateOffset: Long): Option[V] = {
      val c = open2(fileHandle)
      try {
        return FILE_CHANNEL.getValue(fileHandle = c, key = key,
          keySize = keySize, updateOffset = updateOffset)
      } finally {
        c.close()
      }
    }

    override def readData(fileHandle: Any, offset: Long, data: Array[Byte]): Unit = {
      val c = open2(fileHandle)
      try {
        FILE_CHANNEL.readData(fileHandle = c, offset = offset, data = data)
      } finally {
        c.close()
      }
    }

    override def readKeyValues(fileHandle: Any, offset: Long, keySize: Int): Iterator[(K, V)] = {
      val c = open2(fileHandle)
      try {
        return FILE_CHANNEL.readKeyValues(fileHandle = c, offset = offset, keySize = keySize).toBuffer.iterator
      } finally {
        c.close()
      }
    }

    override def fileSize(fileHandle: Any): Long = cast(fileHandle).length()

    override def open(fileName: String): Any = new File(fileName)

    override def close(file: Any): Unit = {
      //nothing to do, java.io.File consumes no system resources
    }
  }

  /**
    * Memory maps file into read-only ByteBuffer. File must be smaller than 2GB due to addressing limit.
    *
    * @param fileName to be mapped
    * @return ByteBuffer of memory mapped file
    */
  protected def mmap(fileName: String): MappedByteBuffer = {
    val file = new File(fileName)
    val c = FileChannel.open(file.toPath, StandardOpenOption.READ)
    try {
      return c.map(MapMode.READ_ONLY, 0, c.size())
    } finally {
      c.close()
    }
  }


}
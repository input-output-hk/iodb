package io.iohk.iodb

import java.io._
import java.util.concurrent.locks.ReentrantReadWriteLock

import collection.JavaConverters._


/**
  * Store which uses Log-Structured-Merge tree
  */
class LSMStore(dir:File, keySize:Int=32, keepLastN:Int=10) extends Store {

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
  private val files = new java.util.TreeSet[Long](java.util.Collections.reverseOrder[Long]())

  private var version: Long = 0
  private val tombstone: V = new ByteArrayWrapper(new Array[Byte](0))

  override def lastVersion: Long = version


  {
    val l = filePrefix.size
    val pattern = (filePrefix+"[0-9]+").r
    //find newest version
    val files2 = dir.listFiles()
          .map(_.getName())
          .filter(pattern.pattern.matcher(_).matches())
          .map(_.substring(l).toLong)

    files2.foreach(files.add(_))

    version = if(files.isEmpty) 0 else files.first
  }

  override def update(versionID: Long, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    lock.writeLock().lock()
    try {
      if(files.contains(versionID))
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
      val notExisted = files.add(versionID)
      assert(notExisted)
      this.version = versionID
    }finally{
      lock.writeLock().unlock()
    }
  }

  override def get(key: K): V = {
    lock.readLock.lock()
    try{
      for (versionId <- files.descendingIterator().asScala) {
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
          val comp = ByteArrayComparator.INSTANCE.compare(key2, key.data)
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
    }finally{
      lock.readLock().unlock()
    }
  }

  override def rollback(versionID: Long): Unit ={
    lock.writeLock().lock()
    try{
      val iter = files.iterator()
      while(iter.hasNext){
        val v = iter.next()
        version = v
        if(v<=versionID)
          return //reached previous version, finish iteration
        //move to prev version
        new File(dir, "store"+v).delete()
        iter.remove()
      }
    }finally{
      lock.writeLock().unlock()
    }
  }


  override def close(): Unit ={

  }

  override def clean(){

  }

  override def cleanStop(): Unit = {

  }


}

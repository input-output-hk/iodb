package io.iohk.iodb

import java.io._
import collection.JavaConverters._


/**
  * Store which uses Log-Structured-Merge tree
  */
class LSMStore(dir:File, keySize:Int=32) extends Store{

  /*
  Format:

   - 8 bytes header
   - 8 bytes checksum
   - 8 bytes total size
   - 4 bytes number of keys
   - 4 bytes key size (all keys have the equal size)


   */

  private val keyCountOffset = 8+8+8

  private val baseOffset:Long = 8+8+8+4+4
  private val keySizeExtra:Long = keySize + 4 + 8



  private var version:Long = 0
  private val tombstone:V = new ByteArrayWrapper(new Array[Byte](0))

  override def lastVersion: Long = ???

  override def update(versionID: Long, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {

    val all = new java.util.TreeMap[K,V].asScala

    var fileSize:Long = baseOffset


    for((key,value) <- toUpdate ++ toRemove.map{(_, tombstone)}){
      if(key.data.length!=keySize)
        throw new IllegalArgumentException("Wrong key size")

      val old = all.put(key,value)
      if(!old.isEmpty)
        throw new IllegalArgumentException("Duplicate key")

      fileSize += keySize + 4 + 8 + value.data.length
    }


    val f = new File(dir,"store")
    val fout = new FileOutputStream(f);
    val out = new DataOutputStream(new BufferedOutputStream(fout))

    out.writeLong(0L)   //header
    out.writeLong(0L)   //checksum
    out.writeLong(fileSize)   //file size
    out.writeInt(all.size)  //number of keys
    out.writeInt(keySize)

    var valueOffset = baseOffset + keySizeExtra * all.size
    for((key,value) <- all){
      out.write(key.data)
      val valueSize = if(value eq tombstone) -1 else value.data.length
      out.writeInt(valueSize)
      out.writeLong(valueOffset)
      valueOffset+=valueSize
    }
    assert(valueOffset==fileSize)

    for(value <- all.values){
      out.write(value.data)
    }

    out.flush()
    fout.flush()
    fout.getFD.sync()
    out.close()
    fout.close()
  }

  override def get(key: K): V = {
    val f = new File(dir,"store")
    val raf = new RandomAccessFile(f, "r")
    raf.seek(keyCountOffset)
    val keyCount:Long = raf.readInt()

    //TODO binary search
    var lo: Long = 0
    var hi: Long = keyCount-1

    val key2 = new Array[Byte](keySize)
    while (lo <= hi) {
      //split interval
      val mid = (lo + hi) / 2
      val keyOffset = baseOffset + mid*keySizeExtra
      //load key
      raf.seek(keyOffset)
      raf.readFully(key2)
      //compare keys and split intervals if not equal
      val comp = ByteArrayComparator.INSTANCE.compare(key2, key.data)
      if (comp<0) lo = mid + 1
      else if (comp>0) hi = mid - 1
      else {
        //key found, load value
        val valueSize = raf.readInt()
        if(valueSize == -1)
          return null
        val valueOffset = raf.readLong()

        raf.seek(valueOffset)
        val ret = new Array[Byte](valueSize)
        raf.readFully(ret)
        return new ByteArrayWrapper(ret)
      }

    }
    return null
  }

  override def get(keys: Iterable[K], consumer: (K, V) => Unit): Unit = ???

  override def rollback(versionID: Long): Unit = ???

  override def clean(): Unit = ???

  override def close(): Unit = ???

  override def cleanStop(): Unit = ???
}

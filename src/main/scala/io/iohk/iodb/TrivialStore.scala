package io.iohk.iodb

import java.io._
import java.util

/**
  * Naive store implementation, it does not have any index and always traverses file to find value.
  * used for testing.
  *
  */
class TrivialStore(
                    val dir: File,
                    val keySize: Int = 32,
                    val keepLastN: Int = 10
                  ) extends Store {


  protected var _lastVersion: Long = 0L

  private val filePrefix = "storeTrivial"

  protected var data = new util.HashMap[K, V]()


  /**
    * Set of active files sorted in descending order (newest first).
    * Java collection is used, it supports mutable iterator.
    */
  private val files = new java.util.TreeSet[Long](java.util.Collections.reverseOrder[Long]())



    {
      val l = filePrefix.size
      val pattern = (filePrefix + "[0-9]+").r
      //find newest version
      val files2 = dir.listFiles()
        .map(_.getName())
        .filter(pattern.pattern.matcher(_).matches())
        .map(_.substring(l).toLong)

      files2.foreach(files.add(_))

      if (files.isEmpty) {
        _lastVersion = 0
      }else{
        _lastVersion = files.first
        //load data from first file
        val in = new ObjectInputStream(new FileInputStream(this.lastFile()))
        data = in.readObject().asInstanceOf[util.HashMap[K, V]]
        in.close()
      }
  }

  protected def lastFile() = new File(dir, filePrefix + lastVersion)

  override def get(key: K): V = {
    return data.get(key)
  }

  override def lastVersion(): Long = _lastVersion

  override def update(versionID: Long, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    if (_lastVersion >= versionID) {
      throw new IllegalArgumentException("VersionID not incremented")
    }

    //check nulls before proceeding with any operations
    for(a <- toRemove){
      if(a==null)throw new NullPointerException()
    }
    for(a <- toUpdate){
      if(a==null || a._1==null || a._2==null )
        throw new NullPointerException()
      if(a._1.data.length!=keySize)
        throw new IllegalArgumentException("key has wrong size, expected "+keySize+", got "+a._1.data.length)
    }

    _lastVersion = versionID
    val notExist = files.add(_lastVersion)
    assert(notExist)

    for (key <- toRemove) {
      val oldVal = data.remove(key)
      if (oldVal == null) {
        throw new AssertionError("removed key was not found")
      }
    }

    for ((key, value) <- toUpdate) {
      val oldVal = data.put(key, value)
      if (oldVal != null) {
        throw new AssertionError("updated existing key")
      }
    }


    //save map
    val fout = new FileOutputStream(lastFile());
    val oi = new ObjectOutputStream(fout);
    oi.writeObject(data)
    oi.flush()
    fout.getFD.sync()
    oi.close()
  }


  override def rollback(versionID: Long): Unit ={
    if (lastVersion() < versionID)
      throw new IllegalArgumentException("Can not rollback to newer version")

    val iter = files.iterator()
      while(iter.hasNext){
        val v = iter.next()
        _lastVersion = v
        if(v<=versionID) {
          val in = new ObjectInputStream(new FileInputStream(lastFile()))
          data = in.readObject().asInstanceOf[util.HashMap[K, V]]
          in.close()

          return //reached previous version, finish iteration
        }
        //move to prev version
        new File(dir, filePrefix+v).delete()
        iter.remove()
      }
    val in = new ObjectInputStream(new FileInputStream(lastFile()))
    data = in.readObject().asInstanceOf[util.HashMap[K, V]]
    in.close()

  }


  override def clean() {
    //keep last N versions
    dir.listFiles()
        .filter(_.getName.matches("[0-9]+"))
        .sortBy(_.getName.toInt)
        .dropRight(keepLastN) //do not delete last N versions
        .foreach(_.delete())
  }

  override def cleanStop() {

  }

  override def close() {

  }
}

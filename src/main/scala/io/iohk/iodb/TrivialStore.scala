package io.iohk.iodb

import java.io._
import java.util

/**
  * Naive store implementation, it does not have any index and always traverses file to find value.
  * used for testing.
  *
  */
class TrivialStore(
                    val dir:File,
                    val keySize:Int = 32
                  ) extends Store {


  protected var _lastVersion:Long = 0L

  protected var data = new util.TreeMap[K,V](ByteArrayComparator.INSTANCE)


  {
    //find newest version
    val files = dir.listFiles()
    if(files.length>0) {
      val lastFile = files.sortBy(_.getName.toInt).last
      _lastVersion = lastFile.getName.toInt
      val in = new ObjectInputStream(new FileInputStream(this.lastFile()))
      data = in.readObject().asInstanceOf[util.TreeMap[K,V]]
      in.close()
    }
  }
  protected def lastFile() = new File(dir, "" + lastVersion)

  override def get(key: K): V = {
    return data.get(key)
  }

  override def lastVersion(): Long = _lastVersion

  override def update(versionID: Long, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit ={
    if(_lastVersion>=versionID){
      throw new IllegalArgumentException("VersionID not incremented")
    }
    _lastVersion = versionID

    for(key <- toRemove){
      val oldVal = data.remove(key)
      if(oldVal==null) {
        throw new AssertionError("removed key was not found")
      }
    }

    for((key,value) <- toUpdate) {
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

  override def get(keys: Iterable[K], consumer: (K, V) => Unit): Unit ={
    for(key <- keys){
      val value = get(key)
      consumer(key,value)
    }
  }

  override def rollback(versionID: Long): Unit ={
      if(lastVersion()<versionID)
        throw new IllegalArgumentException("Can not rollback to newer version")

      _lastVersion = versionID
      val in = new ObjectInputStream(new FileInputStream(lastFile()))
      data = in.readObject().asInstanceOf[util.TreeMap[K,V]]
      in.close()

      //delete newer files
      dir.listFiles().foreach { f =>
          if(f.getName.matches("[0-9]+") && f.getName.toInt>versionID)
            f.delete()
      }
  }

  override def clean(){
    //TODO
  }

  override def cleanStop(){

  }

  override def close(){

  }
}

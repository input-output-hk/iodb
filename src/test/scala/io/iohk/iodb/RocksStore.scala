package io.iohk.iodb

import java.io.File
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.rocksdb._

/**
  * Uses RocksDB as backend
  */
class RocksStore(val dir:File) extends Store{

  {
    RocksDB.loadLibrary
  }

  protected val db:RocksDB = {
    val options: Options = new Options().setCreateIfMissing(true)
    RocksDB.open(options, dir.getPath)
  }

  //TODO snapshots?
  protected var snapshots = new java.util.TreeMap[Long, Snapshot](java.util.Collections.reverseOrder[Long]())

  //TODO thread safe
  private val lock = new ReentrantReadWriteLock()

  private var version: Long = 0


  /** returns value associated with key */
  override def get(key: K): V = {
    val ret = db.get(key.data)
    if(ret==null) null else new ByteArrayWrapper(ret)
  }

  /** returns versionID from last update, used when Scorex starts */
  override def lastVersion: Long = version

  /** update records and move to new version */
  override def update(versionID: Long, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    for(key <- toRemove){
      db.remove(key.data)
    }

    for((key,value) <- toUpdate){
      db.put(key.data, value.data)
    }
    db.flush(new FlushOptions().setWaitForFlush(true))
  }

  /** reverts to older version. Higher (newer) versions are discarded and their versionID can be reused */
  override def rollback(versionID: Long): Unit = ???

  override def clean(version:Long): Unit = {

  }

  override def close(): Unit ={
    db.close
  }

  /** pause cleaning operation */
  override def cleanStop(): Unit = {

  }
}

package io.iohk.iodb.bench

import java.io.File
import java.util.concurrent.locks.ReentrantReadWriteLock

import io.iohk.iodb.{ByteArrayWrapper, Store}
import org.rocksdb._

/**
  * Uses RocksDB as backend
  */
class RocksStore(val dir: File) extends Store {

  import Store._
  {
    RocksDB.loadLibrary()
  }

  protected val db: RocksDB = {
    val options: Options = new Options().setCreateIfMissing(true)
    RocksDB.open(options, dir.getPath)
  }

  //TODO snapshots?
  protected var snapshots = new java.util.TreeMap[Long, Snapshot](java.util.Collections.reverseOrder[Long]())

  //TODO thread safe
  private val lock = new ReentrantReadWriteLock()

  //TODO: versioning
  private var version: VersionID = null


  /** returns value associated with key */
  override def get(key: K): Option[V] = {
    val ret = db.get(key.data)
    if (ret == null) None else Some(ByteArrayWrapper(ret))
  }

  /** returns versionID from last update, used when Scorex starts */
  override def lastVersionID: VersionID = version

  /** update records and move to new version */
  override def update(versionID: VersionID, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    for (key <- toRemove) {
      db.remove(key.data)
    }

    for ((key, value) <- toUpdate) {
      db.put(key.data, value.data)
    }
    db.flush(new FlushOptions().setWaitForFlush(true))
    version = versionID
  }


  override def close(): Unit = {
    db.close()
  }

  /** pause cleaning operation */
  override def cleanStop(): Unit = {

  }

  override def clean(count: Int): Unit = ???

  override def rollback(versionID: VersionID): Unit = ???
}

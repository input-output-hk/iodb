package io.iohk.iodb.bench

import java.io._

import io.iohk.iodb.Store.{K, V, VersionID}
import io.iohk.iodb.{ByteArrayWrapper, Store}
import org.iq80.leveldb._
import org.iq80.leveldb.impl._

/**
  * Uses LevelDB backend
  */
class LevelDBStore(val dir: File, val storeName: String = "leveldb") extends Store {

  private val db: DB = {
    val op = new Options()
    op.createIfMissing(true)
    Iq80DBFactory.factory.open(new File(dir, storeName), op)
  }

  private var lastVersion: Option[VersionID] = None

  override def get(key: K): Option[V] = {
    val b = db.get(key.data)
    return if (b == null) None else Some(ByteArrayWrapper(b))
  }

  override def getAll(consumer: (K, V) => Unit): Unit = {
    val iterator = db.iterator();
    iterator.seekToFirst()
    try {
      while (iterator.hasNext) {
        val n = iterator.next()
        consumer(ByteArrayWrapper(n.getKey), ByteArrayWrapper(n.getValue))
      }
    } finally {
      iterator.close();
    }

  }

  override def clean(count: Int): Unit = ???

  override def cleanStop(): Unit = ???

  override def lastVersionID: Option[VersionID] = lastVersionID

  override def update(versionID: VersionID,
                      toRemove: Iterable[K],
                      toUpdate: Iterable[(K, V)]): Unit = {

    val batch = db.createWriteBatch();
    try {
      toRemove.foreach(b => batch.delete(b.data))
      for ((k, v) <- toUpdate) {
        batch.put(k.data, v.data)
      }
      db.write(batch);
    } finally {
      // Make sure you close the batch to avoid resource leaks.
      batch.close();
    }

  }

  override def rollback(versionID: VersionID): Unit = ???

  override def close(): Unit = {
    db.close()
  }
}

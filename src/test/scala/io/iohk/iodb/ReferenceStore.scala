package io.iohk.iodb

import io.iohk.iodb.Store.{K, V, VersionID}

import scala.collection.mutable

/**
  * Trivial on-heap implementation of Store, used for unit tests.
  */
class ReferenceStore extends Store {

  protected var current: Map[K, V] = Map()
  protected var history = mutable.LinkedHashMap[VersionID, Map[K, V]]()
  protected var curVersiondId: Option[VersionID] = None

  override def get(key: K): Option[V] = current.get(key)

  override def clean(count: Int): Unit = {
    //remove all but last N versions from history
    history = history.takeRight(count)
  }

  override def cleanStop(): Unit = {}

  override def lastVersionID: Option[VersionID] = curVersiondId

  override def update(versionID: VersionID, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    assert(history.get(versionID) == None)
    current = current ++ (toUpdate) -- (toRemove)
    history.put(versionID, current)
    curVersiondId = Some(versionID)
  }

  override def rollback(versionID: VersionID): Unit = {
    current = history(versionID)
    curVersiondId = Some(versionID)
    //drop non used versions
    var found = false
    for (ver <- history.keys) {
      if (found) {
        //remove all keys after current version
        history.remove(ver)
      }
      found = found || ver == versionID
    }
  }

  override def close(): Unit = {}

  override def getAll(consumer: (K, V) => Unit) = {
    for ((k, v) <- current) {
      consumer(k, v)
    }
  }

  override def rollbackVersions(): Iterable[VersionID] = history.keys
}

package io.iohk.iodb

/**
  * Disk storage for Scorex
  */
trait Store {

  type K = ByteArrayWrapper
  type V = ByteArrayWrapper

  /** returns value associated with key */
  def get(key: K): V

  /** gets values associated with keys, returns map with result */
  def get(keys: Iterable[K]): Iterable[(K, V)] = {
    val ret = scala.collection.mutable.ArrayBuffer.empty[(K, V)]
    get(keys, (key: K, value: V) =>
      ret += ((key, value))
    )
    ret
  }

  /** gets values associated with keys, consumer is called for each result */
  def get(keys: Iterable[K], consumer: (K, V) => Unit): Unit = {
    for (key <- keys) {
      val value = get(key)
      consumer(key, value)
    }
  }

  /** start background cleanup/ compact operation, all versions older than parameter will be removed */
  def clean(version:Long)

  /** pause cleaning operation */
  def cleanStop(): Unit //TODO: Try[Unit] ?

  /** returns versionID from last update, used when Scorex starts */
  def lastVersion: Long

  /** update records and move to new version */
  def update(versionID: Long, toRemove: Iterable[K], toUpdate: Iterable[(K, V)])

  /** reverts to older version. Higher (newer) versions are discarded and their versionID can be reused */
  def rollback(versionID: Long)

  def close(): Unit //TODO: Try[Unit] ?
}
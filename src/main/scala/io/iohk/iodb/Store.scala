package io.iohk.iodb

import scala.collection.mutable

object Store {

  /** type of key */
  type K = ByteArrayWrapper
  /** type of value */
  type V = ByteArrayWrapper

  /** type used for versionID */
  type VersionID = ByteArrayWrapper

  val tombstone = new ByteArrayWrapper(0)
}

/**
  * Interface for a key-value versioned database.
  * It has been created with a blockchain core needs in mind.
  */
trait Store {

  import Store._

  /**
    * Finds key and returns value associated with the key.
    * If key is not found, it returns null.
    *
    * It uses lattest (most recent) version available in store.
    *
    * @param key to lookup
    * @return value associated with key or null
    */
  def get(key: K): Option[V]

  /** Returns value associated with the key, or defualt value from user
    */
  def getOrElse(key: K, default: => V): V = get(key).getOrElse(default)

  /** returns value associated with the key or throws `NoSuchElementException` */
  def apply(key: K): V = getOrElse(key, {
    throw new NoSuchElementException()
  })

  /**
    * Batch get.
    *
    * Finds all keys from given iterable.
    * Result is returned in an iterable of key-value pairs.
    * If key is not found, null value is included in result pair.
    *
    * It uses lattest (most recent) version available in store
    *
    * @param keys keys to loopup
    * @return iterable over key-value pairs found in store
    */
  def get(keys: Iterable[K]): Iterable[(K, Option[V])] = {
    val ret = scala.collection.mutable.ArrayBuffer.empty[(K, Option[V])]
    get(keys, (key: K, value: Option[V]) =>
      ret += ((key, value))
    )
    ret
  }

  /**
    * Batch get with callback for result value.
    *
    * Finds all keys from given iterable.
    * Results are passed to callable consumer.
    *
    * It uses lattest (most recent) version available in store
    *
    * @param keys     keys to lookup
    * @param consumer callback method to consume results
    */
  def get(keys: Iterable[K], consumer: (K, Option[V]) => Unit): Unit = {
    for (key <- keys) {
      val value = get(key)
      consumer(key, value)
    }
  }

  /** Get content of entire store. Result is not sorted. */
  def getAll(): Iterator[(K, V)] = {
    val ret = new mutable.ArrayBuffer[(K, V)]()
    getAll { (k: K, v: V) =>
      ret += ((k, v))
    }
    return ret.iterator
  }

  /**
    * Get content of entire store and pass it to consumer.
    * There might be too many entries to fit on heap.
    * Iterators also cause problems for locking.
    * So the consumer is preferred way to fetch all entries.
    *
    * @param consumer
    */
  def getAll(consumer: (K, V) => Unit)


  /**
    * Starts or resumes  background compaction.
    * Compaction performs cleanup and runs in background process.
    * It removes older version and compacts index to consume less space.
    *
    * @param count how many past versions to keep
    *
    */
  def clean(count: Int)

  /**
    * Pauses background compaction process.
    * This can be used if machine needs all available CPU power for other tasks.
    */
  def cleanStop(): Unit //TODO: Try[Unit] ?

  /**
    * Returns current versionID used by Store.
    * It is last version store was update to with `update()` method.
    *
    * If store is empty, the last version does not exist yet and store returns `None`
    *
    * VersionID is persisted between restarts.
    */
  def lastVersionID: Option[VersionID]

  /**
    * Batch update records.
    *
    * Each update increments versionID. New versionID is passed as an argument.
    *
    * Update might remove some key-value pairs, or can insert new key-value pairs.
    * Iterable of keys to be deleted, and iterable of key-value pairs to be updated is passed as an argument.
    *
    * @param versionID new versionID associated with this update
    * @param toRemove  iterable over keys which will be deleted in this update
    * @param toUpdate  iterable over key-value pairs which will be inserted in this update
    */

  def update(versionID: VersionID, toRemove: Iterable[K], toUpdate: Iterable[(K, V)])

  def update(version: Long, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    update(ByteArrayWrapper.fromLong(version), toRemove, toUpdate)
  }

  /**
    * Reverts to an older versionID.
    * All key-value pairs are reverted to this older version (updates between two versionIDs are removed).
    *
    * Higher (newer) versionIDs are discarded and their versionID can be reused
    */
  def rollback(versionID: VersionID)


  /**
    * Closes store. All resources associated with this store are closed and released (files, background threads...).
    * Any get/update operations invoked on closed store will throw an exception.
    */
  def close(): Unit //TODO: Try[Unit] ?
}
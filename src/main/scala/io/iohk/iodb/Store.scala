package io.iohk.iodb

import java.util

/**
  * Disk storage for Scorex
  */
trait Store{

  type K = Array[Byte]
  type V = Array[Byte]

  /** returns value associated with key */
  def get(key:K):V

  /** gets values associated with keys, consumer is called for each result */
  def get(keys:Iterable[K], consumer: (K,V)=>Unit)

  /** gets values associated with keys, returns map with result */
  def get(keys:Iterable[K]): Iterable[(K, V)] = {
    val ret = scala.collection.mutable.ArrayBuffer.empty[(K, V)]
    get(keys, (key: K, value: V) =>
      ret += ((key,value))
    )
    return ret;
  }

  /** start background cleanup/ compact operation. Only last N versions will be preserved */
  def clean()

  /** pause cleaning operation */
  def cleanStop()

  /**  returns versionID from last update, used when Scorex starts */
  def lastVersion:Long

  /** update records and move to new version */
  def update(versionID: Long, toRemove:Iterable[K],   toUpdate:Iterable[(K,V)] )

  /** reverts to older version. Higher (newer) versions are discarted and their versionID can be reused*/
  def rollback(versionID : Long)

  def close()
}

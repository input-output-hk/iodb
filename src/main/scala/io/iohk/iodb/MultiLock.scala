package io.iohk.iodb

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.LockSupport

/**
  * Set of concurrent locks. It can lock on many files and other values.
  */
protected[iodb] class MultiLock[E] {

  /** currently locked entres, key is entry, value is Thread ID which locked  */
  protected val locks = new ConcurrentHashMap[E, Any]()


  /** locks given entry, block until locked */
  def lock(entry:E): Unit = {
    val th = Thread.currentThread().getId
    while(locks.putIfAbsent(entry,th) != null){
      LockSupport.parkNanos(100)
    }
  }

  /** unlock given entry. Throws `IllegalAccessError` if current thread does not hold the lock */
  def unlock(entry:E): Unit = {
    val th = Thread.currentThread().getId
    if(!locks.remove(entry, th)){
      throw new IllegalAccessError("Current thread does not hold lock on entry.")
    }
  }

}

package io.iohk.iodb

import java.io._
import java.nio.ByteBuffer
import java.nio.file.{Files, StandardOpenOption}
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream
import io.iohk.iodb.Store.{K, V, VersionID}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Keeps all data in-memory. Also uses log file to provide durability and rollbacks
  */
class QuickStore(val dir: File, filePrefix: String = "quickStore") extends Store {

  protected val lock = new ReentrantReadWriteLock()
  protected val keyvals = new mutable.HashMap[K, V]()

  protected val path = new File(dir, filePrefix + "-1").toPath

  protected var versionID = Some(Store.tombstone)

  {
    if (!path.toFile.exists())
      path.toFile.createNewFile()

    //replay all updates
    deserializeAllUpdates { u =>
      u.changes.foreach { c =>
        if (c.newValue eq Store.tombstone) {
          keyvals.remove(c.key)
        } else {
          keyvals.update(c.key, c.newValue)
        }
      }
    }
  }

  override def get(key: K): Option[V] = {
    lock.readLock().lock()
    try {
      return keyvals.get(key)
    } finally {
      lock.readLock().unlock()
    }
  }

  override def getAll(consumer: (K, V) => Unit): Unit = {
    lock.readLock().lock()
    try {
      for ((key, value) <- keyvals) {
        consumer(key, value)
      }
    } finally {
      lock.readLock().unlock()
    }
  }

  override def update(versionID: VersionID, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    val changes = new ArrayBuffer[QuickChange]()
    for ((key, value) <- toUpdate) {
      changes += new QuickChange(key = key,
        oldValue = keyvals.getOrElse(key, Store.tombstone),
        newValue = value)
    }

    for ((key) <- toRemove) {
      changes += new QuickChange(key = key,
        oldValue = keyvals.getOrElse(key, Store.tombstone),
        newValue = Store.tombstone)
    }

    lock.writeLock().lock()
    try {

      val binaryUpdate = serializeUpdate(new QuickUpdate(versionID = versionID, prevVersionID = this.versionID.get, changes = changes))

      this.versionID = Some(versionID)
      val fout = Files.newOutputStream(path,
        StandardOpenOption.APPEND, StandardOpenOption.WRITE,
        StandardOpenOption.DSYNC)
      try {
        fout.write(binaryUpdate)
      } finally {
        fout.flush()
        fout.close()
      }


      for ((key, value) <- toUpdate) {
        keyvals.put(key, value)
      }
      for ((key) <- toRemove) {
        keyvals.remove(key)
      }
    } finally {
      lock.writeLock().unlock()
    }
  }

  protected[iodb] def serializeUpdate(u: QuickUpdate): Array[Byte] = {
    val out = new ByteOutputStream()
    val out2 = new DataOutputStream(out)
    //skip place for update size and checksum
    out2.writeLong(0L) //checksum
    out2.writeInt(0) //update size

    def write(b: ByteArrayWrapper): Unit = {
      if (b eq Store.tombstone) {
        out2.writeInt(-1)
      } else {
        out2.writeInt(b.data.size)
        out2.write(b.data)
      }
    }

    write(u.versionID)
    write(u.prevVersionID)
    out2.writeInt(u.changes.size)

    u.changes.foreach { c =>
      write(c.key)
      write(c.oldValue)
      write(c.newValue)
    }
    val b = out.getBytes
    val out3 = ByteBuffer.wrap(b)
    out3.putInt(8, b.size)
    out3.putLong(0, Utils.checksum(b))
    return b
  }

  protected[iodb] def deserializeUpdate(in: DataInputStream): QuickUpdate = {

    val checksum = in.readLong()
    val size = in.readInt()

    //verify checksum, must read byte[] to do that
    val b = new Array[Byte](size)
    in.readFully(b, 12, size - 12)
    Utils.putInt(b, 8, size)

    val calculatedChecksum = Utils.checksum(b)
    if (checksum != calculatedChecksum)
      throw new DataCorruptionException("wrong checksum")

    val in2 = new DataInputStream(new ByteArrayInputStream(b))
    in2.readLong()
    in2.readInt()

    def read(): ByteArrayWrapper = {
      val size = in2.readInt()
      if (size == -1)
        return Store.tombstone
      val r = new ByteArrayWrapper(size)
      in2.readFully(r.data)
      return r
    }

    val versionID = read()
    val prevVersionID = read()

    val keyCount = in2.readInt()
    val changes = (0 until keyCount).map(i => new QuickChange(read(), read(), read())).toBuffer
    return new QuickUpdate(versionID = versionID, prevVersionID = prevVersionID, changes = changes)
  }

  protected[iodb] def deserializeAllUpdates(consumer: (QuickUpdate) => Unit): Unit = {
    val fin = Files.newInputStream(path, StandardOpenOption.READ)
    try {
      val din = new DataInputStream(fin)
      while (din.available() > 0) {
        val u = deserializeUpdate(din)
        consumer(u)
      }
    } finally {
      fin.close()
    }
  }

  override def lastVersionID: Option[VersionID] = {
    lock.readLock().lock()
    try {
      versionID
    } finally {
      lock.readLock().unlock()
    }
  }

  override def rollback(versionID: VersionID): Unit = {
    lock.writeLock().lock()
    try {
      val updatesMap = mutable.HashMap[VersionID, QuickUpdate]()
      var lastUpdate: QuickUpdate = null
      deserializeAllUpdates { u =>
        updatesMap.put(u.versionID, u)
        lastUpdate = u
      }

      if (!updatesMap.contains(versionID) || lastUpdate == null)
        throw new IllegalArgumentException("VersionID not found")

      //create linked list of updates
      val updates = new ArrayBuffer[QuickUpdate]()
      var counter = 0
      while (lastUpdate.versionID != versionID) {
        lastUpdate = updatesMap(lastUpdate.prevVersionID)
        //cyclic ref protection
        counter += 1
        if (counter > 1e7)
          throw new DataCorruptionException("rollback over too many versions, most likely cyclic ref")
      }

      this.versionID = Some(versionID)

      //got linked list of updates, now replay in reverse order (insert old values)
      for (u <- updates; change <- u.changes) {
        if (change.oldValue eq Store.tombstone)
          keyvals.remove(change.key)
        else
          keyvals.put(change.key, change.oldValue)
      }
    } finally {
      lock.writeLock().unlock()
    }
  }

  override def close(): Unit = {}

  override def clean(count: Int): Unit = {}

  override def cleanStop(): Unit = {}

  override def rollbackVersions(): Iterable[VersionID] = {
    lock.readLock().lock()
    try {
      val v = new ArrayBuffer[VersionID]()
      deserializeAllUpdates { u =>
        v += u.versionID
      }
      return v
    } finally {
      lock.readLock().unlock()
    }
  }

}

case class QuickUpdate(
                        versionID: VersionID,
                        prevVersionID: VersionID,
                        changes: Iterable[QuickChange])

case class QuickChange(
                        key: K,
                        oldValue: V,
                        newValue: V)
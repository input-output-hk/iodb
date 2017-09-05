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
class QuickStore(
                  val dir: File,
                  val filePrefix: String = "quickStore",
                  val keepVersions: Int = 0
                ) extends Store {

  protected val lock = new ReentrantReadWriteLock()
  protected val keyvals = new mutable.HashMap[K, V]()

  protected val path = new File(dir, filePrefix + "-1").toPath

  protected var versionID: VersionID = Store.tombstone

  {
    if (!path.toFile.exists())
      path.toFile.createNewFile()

    //get all updates from file
    val updates = new mutable.HashMap[VersionID, QuickUpdate]()
    var lastUpdate: QuickUpdate = null
    deserializeAllUpdates { u =>
      updates.put(u.versionID, u)
      lastUpdate = u
    }

    //construct sequence of updates
    val seq = new mutable.ArrayBuffer[QuickUpdate]
    var counter = 0L
    while (lastUpdate != null) {
      //cyclic ref protection
      counter += 1
      if (counter > 1e9)
        throw new DataCorruptionException("too many versions, most likely cyclic ref")

      seq += lastUpdate
      lastUpdate =
        if (lastUpdate.prevVersionID eq Store.tombstone) null
        else updates(lastUpdate.prevVersionID)
    }

    //replay updates to get current map
    replayChanges(seq.reverse) { (c: QuickChange, u: QuickUpdate) =>
      if (c.newValue eq Store.tombstone) {
        keyvals.remove(c.key)
      } else {
        keyvals.update(c.key, c.newValue)
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

      val binaryUpdate = serializeUpdate(versionID = versionID, prevVersionID = this.versionID, changes = changes)

      this.versionID = versionID
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

  protected[iodb] def serializeUpdate(versionID: VersionID, prevVersionID: VersionID, changes: Iterable[QuickChange]): Array[Byte] = {
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

    write(versionID)
    write(prevVersionID)
    out2.writeInt(changes.size)

    changes.foreach { c =>
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


  protected def readByteArray(in: DataInput): ByteArrayWrapper = {
    val size = in.readInt()
    if (size == -1)
      return Store.tombstone
    val r = new ByteArrayWrapper(size)
    in.readFully(r.data)
    return r
  }

  protected[iodb] def deserializeUpdate(in: DataInputStream, offset: Long, skipChecksum: Boolean = false): QuickUpdate = {

    val checksum = in.readLong()
    val length = in.readInt()

    //verify checksum, must read byte[] to do that
    val b = new Array[Byte](length)
    in.readFully(b, 12, length - 12)

    if (!skipChecksum) {
      Utils.putInt(b, 8, length)

      val calculatedChecksum = Utils.checksum(b)
      if (checksum != calculatedChecksum)
        throw new DataCorruptionException("wrong checksum")
    }

    val in2 = new DataInputStream(new ByteArrayInputStream(b))
    in2.readLong()
    in2.readInt()

    val versionID = readByteArray(in2)
    val prevVersionID = readByteArray(in2)

    val keyCount = in2.readInt()
    //    val changes =
    //      if(skipData) Nil
    //      else (0 until keyCount).map(i => new QuickChange(read(), read(), read())).toBuffer
    return new QuickUpdate(offset = offset, length = length, versionID = versionID, prevVersionID = prevVersionID, changeCount = keyCount)
  }


  protected[iodb] def deserializeAllUpdates(consumer: (QuickUpdate) => Unit): Unit = {
    val fin = Files.newInputStream(path, StandardOpenOption.READ)
    var offset = 0L
    try {
      val din = new DataInputStream(fin)
      while (din.available() > 0) {
        val u = deserializeUpdate(in = din, offset = offset)
        consumer(u)
        offset += u.length
      }
    } finally {
      fin.close()
    }
  }

  protected def deserializeChange(in: DataInput) =
    new QuickChange(
      key = readByteArray(in),
      oldValue = readByteArray(in),
      newValue = readByteArray(in))


  protected def replayChanges(updates: Iterable[QuickUpdate])(consumer: (QuickChange, QuickUpdate) => Unit): Unit = {
    val fin = new FileInputStream(path.toFile)
    try {
      for (update <- updates; if (update.changeCount > 0)) {
        //seek to offset where data are starting
        fin.getChannel.position(update.offset + 4 + 8 + 4 + 4 + 4 + update.versionID.size + update.prevVersionID.size)

        //create buffered stream, it can not be reused, `fin` will seek and buffer would become invalid
        val din = new DataInputStream(new BufferedInputStream(fin))
        for (i <- 0L until update.changeCount) {
          val change = deserializeChange(din)
          consumer(change, update)
        }
      }
    } finally {
      fin.close()
    }
  }

  override def lastVersionID: Option[VersionID] = {
    lock.readLock().lock()
    try {
      val versionID2 = versionID
      return if ((versionID2 eq null) || (versionID2 eq Store.tombstone)) None
      else Some(versionID2)
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
      var counter = 0L
      while (lastUpdate.versionID != versionID) {
        lastUpdate = updatesMap(lastUpdate.prevVersionID)
        //cyclic ref protection
        counter += 1
        if (counter > 1e7)
          throw new DataCorruptionException("rollback over too many versions, most likely cyclic ref")
      }

      this.versionID = versionID

      //replay in reverse order (insert old values)
      replayChanges(updates) { (c: QuickChange, u: QuickUpdate) =>
        if (c.oldValue eq Store.tombstone)
          keyvals.remove(c.key)
        else
          keyvals.put(c.key, c.oldValue)
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


protected case class QuickUpdate(
                                  offset: Long,
                                  length: Long, //number of bytes consumed by this update
                                  versionID: VersionID,
                                  prevVersionID: VersionID,
                                  changeCount: Long)

protected case class QuickChange(
                        key: K,
                        oldValue: V,
                        newValue: V)
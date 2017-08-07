package io.iohk.iodb

import java.io.FileOutputStream

import io.iohk.iodb.Store._
import io.iohk.iodb.TestUtils._
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

class LogStoreTest extends StoreTest {

  override def open(keySize: Int) = new LogStore(dir = dir, keySize = keySize)

  @Test def binarySearch() {
    val store = new LogStore(dir = dir, keySize = 32)

    //random testing
    val r = new Random()
    val data = (0 until 1000).map { i =>
      val key = randomA(store.keySize)
      val valSize = 10 + r.nextInt(100)
      val value = randomA(valSize)
      (key, value)
    }.sortBy(_._1)

    //serialize update entry
    val b = store.serializeUpdate(Store.tombstone, data, true, prevFileNumber = 0, prevFileOffset = 0)

    //write to file
    val f = tempFile()
    val fout = new FileOutputStream(f)
    val foffset = 10000
    fout.write(new Array[Byte](foffset))
    fout.write(b)
    fout.close()

    //try to read all values
    val fa = store.fileOpen(f.getPath)
    for ((key, value) <- data) {
      val value2 = store.fileGetValue(fa, key, store.keySize, foffset)
      assertEquals(value2, Some(value))
    }

    //try non existent
    val nonExistentKey = randomA(32)
    assertEquals(null, store.fileGetValue(fa, nonExistentKey, store.keySize, foffset))
    store.verify()
    store.close()
  }

  @Test def get2(): Unit = {
    val store = new LogStore(dir = dir, keySize = 8)

    val updated = mutable.HashMap[K, V]()
    val removed = mutable.HashSet[K]()
    for (i <- 0 until 10) {
      //generate random data
      var toUpdate = (0 until 10).map(a => (randomA(8), randomA(40)))
      var toRemove: List[K] = if (updated.isEmpty) Nil else updated.keys.take(2).toList
      toRemove.foreach(updated.remove(_))

      //modify
      store.update(fromLong(i), toUpdate = toUpdate, toRemove = toRemove)

      removed ++= toRemove
      updated ++= toUpdate

      removed.foreach { k =>
        assertEquals(store.get(k), None)
      }
      for ((k, v) <- updated) {
        assertEquals(store.get(k), Some(v))
      }
    }

    //try to iterate over all items in store
    val updated2 = mutable.HashMap[K, V]()
    val removed2 = mutable.HashSet[K]()

    store.loadKeyValues(store.loadUpdateOffsets(stopAtMerge = true), store.fileHandles.asScala.toMap, false).foreach { case (k, v) =>
      if (v eq tombstone)
        removed2.add(k)
      else
        updated2.put(k, v)
    }

    //and compare result
    assertEquals(removed, removed2)
    assertEquals(updated, updated2)
    store.verify()
    store.close()
  }

  @Test def fileAccess_getAll(): Unit = {
    val store = new LogStore(dir = dir, keySize = 32)
    for (i <- 0 until 10) {
      //generate random data
      val toUpdate = (0 until 10).map(a => (randomA(32), randomA(40))).toMap
      val toRemove = (0 until 10).map(a => randomA(32)).toSet
      store.update(fromLong(i), toUpdate = toUpdate, toRemove = toRemove)

      store.get(tombstone) //initialize fAccess handle

      //try to get all key/vals from last update
      val keyVals = store.fileReadKeyValues(store.fileHandles.firstEntry().getValue, store.validPos.get.offset, keySize = 32).toBuffer
      val toUpdate2 = keyVals.filterNot(_._2 eq tombstone).toMap
      val toRemove2 = keyVals.filter(_._2 eq tombstone).map(_._1).toSet
      assertEquals(toUpdate, toUpdate2)
      assertEquals(toRemove, toRemove2)
    }
    store.verify()
    store.close()
  }

  @Test def startNewFile(): Unit = {
    val store = new LogStore(dir = dir, keySize = 8)
    for (i <- 1L until 10) {
      store.update(fromLong(i), toUpdate = makeKeyVal(i, i), toRemove = Nil)
      assert(dir.listFiles().filter(_.length() > 0).size == i)
      store.startNewFile()
    }
    store.close()
  }

  @Test def offset_allias(): Unit = {
    var store = new LogStore(dir = dir, keySize = 8)

    def update(i: Long) = store.update(fromLong(i), toUpdate = makeKeyVal(i, i), toRemove = Nil)

    update(1)
    assert(store.eof.fileNum == 1)
    store.startNewFile()
    update(2)
    store.startNewFile()
    update(3)
    store.startNewFile()
    update(4)

    //skip over #2
    store.appendFileAlias(3, 0, 2, 0)

    def check() {
      assert(Map(FilePos(3, 0) -> FilePos(2, 0)).asJava == store.offsetAliases)
      assert(List(fromLong(1), fromLong(2), fromLong(4)) == store.rollbackVersions())
      assert(Some(fromLong(1)) == store.get(fromLong(1)))
      assert(Some(fromLong(2)) == store.get(fromLong(2)))
      assert(None == store.get(fromLong(3)))
      assert(Some(fromLong(4)) == store.get(fromLong(4)))
    }

    val aliases = store.offsetAliases
    check()
    //reopen
    store.close()
    store = new LogStore(dir = dir, keySize = 8)
    assert(aliases == store.offsetAliases)
    check()
  }


  @Test def get_stops_at_merge(): Unit = {
    var store = new LogStore(dir = dir, keySize = 8)

    def update(i: Long) = store.update(fromLong(i), toUpdate = makeKeyVal(i, i), toRemove = Nil)

    update(1L)
    update(2L)
    //insert compacted entry
    val eof = store.eof
    val pos = store.validPos.get()
    val compactedData = store.serializeUpdate(fromLong(3L), data = makeKeyVal(3L, 3L),
      isMerged = true, prevFileNumber = pos.fileNum, prevFileOffset = pos.offset)
    store.append(compactedData)

    //and update positions
    store.setValidPos(eof)
    //    store.eof = store.eof.copy(offset = store.eof.offset + compactedData.size)
    //add extra entry after compacted entry
    update(4L)

    //older entries should be ignored, latest entry is merged
    def check() {
      assert(None == store.get(fromLong(1L)))
      assert(None == store.get(fromLong(2L)))
      assert(Some(fromLong(3L)) == store.get(fromLong(3L)))
      assert(Some(fromLong(4L)) == store.get(fromLong(4L)))
    }

    check()
    store.close()
    store = new LogStore(dir = dir, keySize = 8)
    check()
    store.close()
  }

  @Test def readerIncrement(): Unit = {
    val store = new LogStore(dir = dir, keySize = 8)
    Utils.fileReaderIncrement(store.fileSemaphore, 22L)
    assert(store.fileSemaphore.get(22L) == 1L)
    Utils.fileReaderIncrement(store.fileSemaphore, 22L)
    assert(store.fileSemaphore.get(22L) == 2L)
    Utils.fileReaderDecrement(store.fileSemaphore, 22L)
    assert(store.fileSemaphore.get(22L) == 1L)
    Utils.fileReaderDecrement(store.fileSemaphore, 22L)
    assert(store.fileSemaphore.get(22L) == null)

    store.close()
  }


  @Test def loadOffsets_stops_at_merge(): Unit = {
    val store = new LogStore(dir = dir, keySize = 8)

    for (i <- 1 to 10) {
      val b = fromLong(i)
      store.update(versionID = b, toRemove = Nil, toUpdate = List((b, b)))
    }

    store.compact()
    assert(1 == store.loadUpdateOffsets(stopAtMerge = true).size)

    for (i <- 1 to 10) {
      val b = fromLong(i + 20)
      store.update(versionID = b, toRemove = Nil, toUpdate = List((b, b)))
      assert(i + 1 == store.loadUpdateOffsets(stopAtMerge = true).size)
    }
  }

}
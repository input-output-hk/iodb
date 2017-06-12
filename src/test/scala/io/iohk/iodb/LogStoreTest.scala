package io.iohk.iodb

import java.io.FileOutputStream

import io.iohk.iodb.Store._
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable
import scala.util.Random

class LogStoreTest {

  @Test def binarySearch() {
    val dir = TestUtils.tempDir()
    val store = new LogStore(dir = dir, keySize = 32)

    //random testing
    val r = new Random()
    val data = (0 until 1000).map { i =>
      val key = TestUtils.randomA(store.keySize)
      val valSize = 10 + r.nextInt(100)
      val value = TestUtils.randomA(valSize)
      (key, value)
    }.sortBy(_._1)

    //serialize update entry
    val b = store.serializeUpdate(Store.tombstone, data, true, prevFileNumber = 0, prevFileOffset = 0)

    //write to file
    val f = TestUtils.tempFile()
    val fout = new FileOutputStream(f)
    val foffset = 10000
    fout.write(new Array[Byte](foffset))
    fout.write(b)
    fout.close()

    //try to read all values
    val fa = FileAccess.SAFE.open(f.getPath)
    for ((key, value) <- data) {
      val value2 = FileAccess.SAFE.getValue(fa, key, store.keySize, foffset)
      assertEquals(value2, Some(value))
    }

    //try non existent
    val nonExistentKey = TestUtils.randomA(32)
    assertEquals(null, FileAccess.SAFE.getValue(fa, nonExistentKey, store.keySize, foffset))
    store.close()
    TestUtils.deleteRecur(dir)
  }

  @Test def get_getAll(): Unit = {
    val dir = TestUtils.tempDir()
    val store = new LogStore(dir = dir, keySize = 8)

    val updated = mutable.HashMap[K, V]()
    val removed = mutable.HashSet[K]()
    for (i <- 0 until 10) {
      //generate random data
      var toUpdate = (0 until 10).map(a => (TestUtils.randomA(8), TestUtils.randomA(40)))
      var toRemove: List[K] = if (updated.isEmpty) Nil else updated.keys.take(2).toList
      toRemove.foreach(updated.remove(_))

      //modify
      store.update(TestUtils.fromLong(i), toUpdate = toUpdate, toRemove = toRemove)

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

    store.keyValues(false).foreach { case (k, v) =>
      if (v eq tombstone)
        removed2.add(k)
      else
        updated2.put(k, v)
    }

    //and compare result
    assertEquals(removed, removed2)
    assertEquals(updated, updated2)

    store.close()
    TestUtils.deleteRecur(dir)
  }

  @Test def fileAccess_getAll(): Unit = {
    val dir = TestUtils.tempDir()
    val store = new LogStore(dir = dir, keySize = 32)
    for (i <- 0 until 10) {
      //generate random data
      val toUpdate = (0 until 10).map(a => (TestUtils.randomA(32), TestUtils.randomA(40))).toMap
      val toRemove = (0 until 10).map(a => TestUtils.randomA(32)).toSet
      store.update(TestUtils.fromLong(i), toUpdate = toUpdate, toRemove = toRemove)

      store.get(tombstone) //initialize fAccess handle

      //try to get all key/vals from last update
      val keyVals = store.fileAccess.readKeyValues(store.fAccess, store.journalLastEntryOffset, keySize = 32).toBuffer
      val toUpdate2 = keyVals.filterNot(_._2 eq tombstone).toMap
      val toRemove2 = keyVals.filter(_._2 eq tombstone).map(_._1).toSet
      assertEquals(toUpdate, toUpdate2)
      assertEquals(toRemove, toRemove2)
    }
    store.close()
    TestUtils.deleteRecur(dir)
  }
}
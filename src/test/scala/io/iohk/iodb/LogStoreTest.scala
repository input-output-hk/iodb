package io.iohk.iodb

import java.io.FileOutputStream

import io.iohk.iodb.Store._
import io.iohk.iodb.TestUtils._
import org.junit.Assert._
import org.junit.Test

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
    val fa = FileAccess.SAFE.open(f.getPath)
    for ((key, value) <- data) {
      val value2 = FileAccess.SAFE.getValue(fa, key, store.keySize, foffset)
      assertEquals(value2, Some(value))
    }

    //try non existent
    val nonExistentKey = randomA(32)
    assertEquals(null, FileAccess.SAFE.getValue(fa, nonExistentKey, store.keySize, foffset))
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

    store.keyValues(store.loadUpdateOffsets(), false).foreach { case (k, v) =>
      if (v eq tombstone)
        removed2.add(k)
      else
        updated2.put(k, v)
    }

    //and compare result
    assertEquals(removed, removed2)
    assertEquals(updated, updated2)

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
      val keyVals = store.fileAccess.readKeyValues(store.fileHandle, store.validPos.get.offset, keySize = 32).toBuffer
      val toUpdate2 = keyVals.filterNot(_._2 eq tombstone).toMap
      val toRemove2 = keyVals.filter(_._2 eq tombstone).map(_._1).toSet
      assertEquals(toUpdate, toUpdate2)
      assertEquals(toRemove, toRemove2)
    }
    store.close()
  }

}
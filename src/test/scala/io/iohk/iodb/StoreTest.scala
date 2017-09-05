package io.iohk.iodb

import java.security.MessageDigest
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import io.iohk.iodb.Store.{K, V}
import io.iohk.iodb.TestUtils._
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable
import scala.util.Random

abstract class StoreTest extends TestWithTempDir {

  def open(keySize: Int = 32): Store


  def testReopen(): Unit = {
    var store = open(keySize = 8)
    store.update(fromLong(1L), toUpdate = (1L to 100L).map(i => (fromLong(i), fromLong(i))), toRemove = Nil)
    store.update(fromLong(2L), toUpdate = Nil, toRemove = (90L to 100L).map(fromLong))

    def check(): Unit = {
      (1L to 89L).foreach(i => assert(Some(fromLong(i)) == store.get(fromLong(i))))
      store.verify()
    }

    check()
    store.close()
    store = open(keySize = 8)

    check()

    store.close()
  }

  @Test def none_last_versionID(): Unit = {
    var s = open()
    assert(s.lastVersionID == None)
    s.close()
    s = open()
    assert(s.lastVersionID == None)
    s.close()
  }

  @Test def get() {
    val store = open(keySize = 32)

    //random testing
    val r = new Random()
    val data = (0 until 1000).map { i =>
      val key = randomA(32)
      val valSize = 10 + r.nextInt(100)
      val value = randomA(valSize)
      (key, value)
    }.sortBy(_._1)

    //put
    store.update(versionID = fromLong(1L), toUpdate = data, toRemove = Nil)

    //try to read all values
    for ((key, value) <- data) {
      assertEquals(Some(value), store.get(key))
    }
    //try non existent
    val nonExistentKey = randomA(32)
    assertEquals(None, store.get(nonExistentKey))
    store.verify()
    store.close()
  }

  @Test def get_getAll(): Unit = {
    val store = open(keySize = 8)

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

    for ((key, value) <- store.getAll()) {
      updated2.put(key, value)
    }
    assertEquals(updated, updated2)
    store.verify()
    store.close()
  }


  def makeKeyVal(key: Long, value: Long) = List((fromLong(key), fromLong(value)))

  @Test def getVersions(): Unit = {
    val store = open(keySize = 8)

    val versions = (0L until 100).map(fromLong).toBuffer
    val updates = makeKeyVal(1, 1)

    for (version <- versions) {
      store.update(versionID = version, toUpdate = updates, toRemove = Nil)
      assertEquals(Some(fromLong(1L)), store.get(fromLong(1L)))
      assertEquals(Some(version), store.lastVersionID)
    }
    val versions2 = store.rollbackVersions().toBuffer
    assertEquals(versions, versions2)
    store.verify()
    store.close()
  }

  @Test def ropen(): Unit = {
    var store = open(keySize = 8)

    store.update(versionID = fromLong(1), toUpdate = makeKeyVal(1, 1), toRemove = Nil)
    assertEquals(Some(fromLong(1)), store.lastVersionID)
    assertEquals(Some(fromLong(1)), store.get(fromLong(1)))

    store.verify()
    store.close()
    store = open(keySize = 8)
    assertEquals(Some(fromLong(1)), store.lastVersionID)
    assertEquals(Some(fromLong(1)), store.get(fromLong(1)))
    store.verify()
  }

  @Test def rollback(): Unit = {
    var store = open(keySize = 8)

    store.update(versionID = fromLong(1), toUpdate = makeKeyVal(1, 1), toRemove = Nil)
    store.update(versionID = fromLong(2), toUpdate = makeKeyVal(1, 2), toRemove = Nil)
    assertEquals(Some(fromLong(2)), store.lastVersionID)
    assertEquals(Some(fromLong(2)), store.get(fromLong(1)))
    store.rollback(fromLong(1))
    assertEquals(Some(fromLong(1)), store.lastVersionID)
    assertEquals(Some(fromLong(1)), store.get(fromLong(1)))

    store.verify()
    //reopen, rollback should be preserved
    store.close()
    store = open(keySize = 8)
    assertEquals(Some(fromLong(1)), store.lastVersionID)
    assertEquals(Some(fromLong(1)), store.get(fromLong(1)))
    store.verify()
  }

  @Test def longRunningUpdates(): Unit = {
    val cycles = 100 + 100000 * TestUtils.longTest()
    val store = open(keySize = 8)
    for (c <- 0L until cycles) {
      val toUpdate = (0 until 100).map { k => (fromLong(k), fromLong(1)) }
      store.update(versionID = fromLong(c), toUpdate = toUpdate, toRemove = Nil)
      if (c % 2000 == 0) {
        store.clean(1000)
      }
      assert(TestUtils.dirSize(dir) < 1e8)
    }
    store.verify()
    store.close()
  }

  @Test def concurrent_updates(): Unit = {
    if (TestUtils.longTest() <= 0)
      return

    val store = open(keySize = 8)
    val threadCount = 8
    val versionID = new AtomicLong(0)
    val key = new AtomicLong(0)
    val count: Long = 1e6.toLong

    val exec = new ForkExecutor(1)
    for (i <- 0 until threadCount) {
      exec.execute {
        var newVersion = versionID.incrementAndGet()
        while (newVersion <= count) {
          val newKey = key.incrementAndGet()
          store.update(fromLong(newVersion), toUpdate = List((fromLong(newKey), fromLong(newKey))), toRemove = Nil)

          newVersion = versionID.incrementAndGet()
        }
      }
    }

    //wait for tasks to finish
    exec.finish()

    //ensure all keys are present
    val keys = (1L to count).map(fromLong(_)).toSet
    val keys2 = store.getAll().map(_._1).toSet
    assert(keys == keys2)

    val versions = store.rollbackVersions().toSet
    assert(keys == versions)
    store.verify()
    store.close()
  }

  @Test def concurrent_key_update(): Unit = {
    val threadCount = 1
    val duration = 100 * 1000; // in milliseconds

    val finishedTaskCounter = new AtomicInteger(0)

    val exec = new ForkExecutor(duration)
    val store: Store = open(keySize = 8)

    for (i <- (1 to threadCount)) {
      val key = TestUtils.fromLong(i)
      exec.execute {
        var counter = 1L
        val r = new Random()
        while (exec.keepRunning) {
          val versionID2 = TestUtils.fromLong(r.nextLong())
          val value = TestUtils.fromLong(counter)
          store.update(versionID2, Nil, List((key, value)))
          for (a <- (1 until 100)) { //try more times, higher chance to catch merge/distribute in progress
            assert(store.get(key) == Some(value))
          }

          counter += 1
        }
      }
      finishedTaskCounter.incrementAndGet()
    }
    //start cleanup thread
    exec.execute {
      while (exec.keepRunning) {
        store.clean(1)
        if (store.isInstanceOf[ShardedStore]) {
          store.asInstanceOf[ShardedStore].distribute()
        }
      }
      finishedTaskCounter.incrementAndGet()
    }

    exec.finish()
    store.close()

    assert(threadCount + 1 == finishedTaskCounter.get())
  }


  @Test def `quick store's lastVersionId should be None right after creation` {
    assert(open().lastVersionID == None)
  }

  @Test def `empty update rollback versions test quick` {
    emptyUpdateRollbackVersions(blockStorage = open())
  }


  def emptyUpdateRollbackVersions(blockStorage: Store): Unit = {
    assert(blockStorage.rollbackVersions().size == 0)

    val version1 = ByteArrayWrapper("version1".getBytes)
    blockStorage.update(version1, Seq(), Seq())
    assert(blockStorage.rollbackVersions().size == 1)

    val version2 = ByteArrayWrapper("version2".getBytes)
    blockStorage.update(version2, Seq(), Seq())
    assert(blockStorage.rollbackVersions().size == 2)
  }

  case class BlockChanges(id: ByteArrayWrapper,
                          toRemove: Seq[ByteArrayWrapper],
                          toInsert: Seq[(ByteArrayWrapper, ByteArrayWrapper)])

  def hash(b: Array[Byte]): Array[Byte] = MessageDigest.getInstance("SHA-256").digest(b)

  def randomBytes(): ByteArrayWrapper = ByteArrayWrapper(hash(Random.nextString(16).getBytes))

  def generateBytes(howMany: Int): Seq[(ByteArrayWrapper, ByteArrayWrapper)] = {
    (0 until howMany).map(i => (randomBytes(), randomBytes()))
  }


}

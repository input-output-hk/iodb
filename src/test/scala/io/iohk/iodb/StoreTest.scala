package io.iohk.iodb

import java.security.MessageDigest
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import io.iohk.iodb.Store.{K, V}
import io.iohk.iodb.TestUtils._
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable
import scala.util.Random
import org.scalatest._
import Matchers._
import io.iohk.iodb.smoke.RandomRollbackTest

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


  @Test def getMulti(): Unit = {
    val store = open(keySize = 8)
    for (i <- 1 to 1000) {
      val b = fromLong(i)
      store.update(versionID = b, toRemove = Nil, toUpdate = List((b, b)))
    }

    for (i <- 1 to 1000) {
      val b = fromLong(i)
      store.get(b) shouldBe Some(b)
    }
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
    updated.size shouldBe  updated2.size
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
    }
    //start cleanup thread
    exec.execute {
      while (exec.keepRunning) {
        store.clean(1)
      }
    }

    exec.finish()
    store.close()
  }


  @Test def `quick store's lastVersionId should be None right after creation` {
    assert(open().lastVersionID == None)
  }

  @Test def `empty update rollback versions test` {
    emptyUpdateRollbackVersions(blockStorage = open())
  }

  @Test def `consistent data after rollbacks test` {
    dataAfterRollbackTest(blockStorage = open())
  }



  def dataAfterRollbackTest(blockStorage: Store): Unit = {

    val data1 = generateBytes(20)
    val data2 = generateBytes(20)
    val data3 = generateBytes(20)

    val block1 = BlockChanges(data1.head._1, Seq(), data1)
    val block2 = BlockChanges(data2.head._1, Seq(), data2)
    val block3 = BlockChanges(data3.head._1, Seq(), data3)

    blockStorage.update(block1.id, block1.toRemove, block1.toInsert)
    blockStorage.update(block2.id, block2.toRemove, block2.toInsert)
    blockStorage.update(block3.id, block3.toRemove, block3.toInsert)

    assert(blockStorage.lastVersionID == Some(block3.id))

    def checkBlockExists(block: BlockChanges): Unit = block.toInsert.foreach{ case (k , v) =>
      val valueOpt = blockStorage.get(k)
      assert(valueOpt.isDefined)
      assert(valueOpt.contains(v))
    }

    def checkBlockNotExists(block: BlockChanges): Unit = block.toInsert.foreach{ case (k , _) =>
      assert(blockStorage.get(k) == None)
    }

    checkBlockExists(block1)
    checkBlockExists(block2)
    checkBlockExists(block3)

    blockStorage.rollback(block2.id)

    checkBlockExists(block1)
    checkBlockExists(block2)
    checkBlockNotExists(block3)

    blockStorage.update(block3.id, block3.toRemove, block3.toInsert)

    checkBlockExists(block1)
    checkBlockExists(block2)
    checkBlockExists(block3)

    blockStorage.rollback(block1.id)

    checkBlockExists(block1)
    checkBlockNotExists(block2)
    checkBlockNotExists(block3)

    blockStorage.update(block2.id, block2.toRemove, block2.toInsert)
    blockStorage.update(block3.id, block3.toRemove, block3.toInsert)

    checkBlockExists(block1)
    checkBlockExists(block2)
    checkBlockExists(block3)
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


  @Test def doubleRollbackTest: Unit ={
    val s = open()
    val data = generateBytes(100)
    val block1 = BlockChanges(data.head._1, Seq(), data.take(50))
    val block2 = BlockChanges(data(51)._1, data.map(_._1).take(20), data.slice(51, 61))
    val block3 = BlockChanges(data(61)._1, data.map(_._1).slice(20, 30), data.slice(61, 71))
    s.update(block1.id, block1.toRemove, block1.toInsert)
    s.update(block2.id, block2.toRemove, block2.toInsert)
    s.get(block2.id) shouldBe Some(data(51)._2)
    s.rollbackVersions() shouldBe List(block1.id, block2.id)
    s.lastVersionID.get shouldBe block2.id
    s.rollback(block1.id)
    s.get(block1.id) shouldBe Some(data.head._2)
    s.get(block2.id) shouldBe None

    s.lastVersionID.get shouldBe block1.id
    s.rollbackVersions() shouldBe List(block1.id)

    s.update(block3.id, block3.toRemove, block3.toInsert)
    s.get(block3.id) shouldBe Some(data(61)._2)
    s.lastVersionID.get shouldBe block3.id
    s.rollbackVersions() shouldBe List(block1.id, block3.id)

    s.rollback(block1.id)
    s.get(block1.id) shouldBe Some(data.head._2)
    s.get(block2.id) shouldBe None
    s.get(block3.id) shouldBe None
    s.lastVersionID.get shouldBe block1.id
    s.rollbackVersions() shouldBe List(block1.id)

  }

  @Test def test_random_rollback(): Unit ={
    RandomRollbackTest.test(open())
  }

}

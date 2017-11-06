package io.iohk.iodb

import io.iohk.iodb.Store._
import org.junit.Test
import org.scalatest.Matchers._

import scala.collection.JavaConverters._

class ShardedStoreTest extends StoreTest {

  override def open(keySize: Int) = new ShardedStore(dir = dir, keySize = keySize)

  @Test def distribute_flushes_log(): Unit = {
    val store = open(keySize = 8)

    for (i <- (0L until 10L)) {
      val b = TestUtils.fromLong(i)
      store.update(versionID = b, toRemove = Nil, toUpdate = List((b, b)))
    }
    assert(store.journal.loadUpdateOffsets(stopAtMerge = true, stopAtDistribute = false).size > 1)
    store.taskDistribute()
    assert(store.journal.loadUpdateOffsets(stopAtMerge = true, stopAtDistribute = true).size == 1)
    store.close()
  }

  @Test def get_after_dist(): Unit = {
    val store = open(keySize = 8)

    def check(shift: Int, inc: Int): Unit = {
      for (i <- (0L until 10L)) {
        val b = TestUtils.fromLong(i + shift)
        assert(Some(TestUtils.fromLong(i + inc)) == store.get(b))
      }

    }

    def update(shift: Int, inc: Int): Unit = {
      for (i <- (0L until 10L)) {
        val b = TestUtils.fromLong(i + shift)
        val value = TestUtils.fromLong(i + inc)
        store.update(versionID = b, toRemove = Nil, toUpdate = List((b, value)))
      }
    }

    update(0, 0)
    check(0, 0)
    store.taskDistribute()
    check(0, 0)
    update(100, 1)
    check(0, 0)
    check(100, 1)
    update(0, 10)
    check(0, 10)
    store.close()
  }


  @Test def shard_count(): Unit = {
    val s = new ShardedStore(dir = dir, shardCount = 10, keySize = 8, executor = null)
    s.shards.size() shouldBe 10
    s.shards.asScala.values.toSet.size shouldBe 10

    //insert random data
    val data: Seq[(K, V)] = (1 until 10000).map(i => (TestUtils.randomA(8), TestUtils.randomA(3))).toBuffer
    val sorted = data.sortBy(_._1)
    s.update(TestUtils.fromLong(1), toUpdate = data, toRemove = Nil)
    s.getAll().toBuffer.size shouldBe sorted.size
    s.getAll().toBuffer shouldBe sorted

    s.taskDistribute()

    s.getAll().toBuffer.size shouldBe sorted.size
    s.getAll().toBuffer shouldBe sorted

    //test all shards are not empty
    for (shard <- s.shards.values().asScala) {
      assert(shard.getAll().toBuffer.size > 0)
    }
    s.close()
  }
}

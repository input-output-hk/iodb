package io.iohk.iodb

import org.junit.Test

class ShardedStoreTest extends StoreTest {

  override def open(keySize: Int) = new ShardedStore(dir = dir, keySize = keySize)

  @Test def distribute_flushes_log(): Unit = {
    val store = open(keySize = 8)

    for (i <- (0L until 10L)) {
      val b = TestUtils.fromLong(i)
      store.update(versionID = b, toRemove = Nil, toUpdate = List((b, b)))
    }
    assert(store.journal.loadUpdateOffsets(true).size > 1)
    store.distribute()
    assert(store.journal.loadUpdateOffsets(true).size == 1)
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
    store.distribute()
    check(0, 0)
    update(100, 1)
    check(0, 0)
    check(100, 1)
    update(0, 10)
    check(0, 10)
    store.close()
  }

}

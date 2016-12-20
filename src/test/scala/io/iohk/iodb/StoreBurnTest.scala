package io.iohk.iodb

import org.junit._

/**
  * Tests store under continuous usage (disk leaks, data corruption etc..
  */
abstract class StoreBurnTest extends TestWithTempDir {

  def makeStore(): Store


  /** tests for disk leaks under continous usage */
  @Test def continous_insert() {
    val store = makeStore()
    val endTime = TestUtils.endTimestamp()
    var keys = Seq.empty[ByteArrayWrapper]

    var version = 1
    while (System.currentTimeMillis() < endTime) {
      keys.foreach { it =>
        if (it != store.get(it))
          store.get(it)
        assert(it == store(it))
      }

      val newKeys = (0 until 10000).map(i => TestUtils.randomA())
      val toUpdate = newKeys.map(a => (a, a))
      store.update(TestUtils.fromLong(version), keys, toUpdate)
      assert(store.lastVersionID.get == TestUtils.fromLong(version))
      version += 1
      if (version % 5 == 0)
        store.clean(version-10)
      keys = newKeys

      //check for disk leaks
      assert(storeSize < 200 * 1024 * 1024)
    }
    store.close()
  }

  @Test def continous_rollback() {
    val store = makeStore()
    val endTime = TestUtils.endTimestamp()

    var keys = Seq.empty[ByteArrayWrapper]
    var rollbackKeys = Seq.empty[ByteArrayWrapper]

    var version = 1

    while (System.currentTimeMillis() < endTime) {
      keys.foreach { it =>
        assert(it == store(it))
      }

      val newKeys = (0 until 10000).map(i => TestUtils.randomA())
      val toUpdate = newKeys.map(a => (a, a))
      val versionID = TestUtils.fromLong(version)
      store.update(versionID, keys, toUpdate)
      assert(store.lastVersionID.get == versionID)

      if (version == 50) {
        rollbackKeys = newKeys
      }

      version += 1

      keys = newKeys
      if (version > 100) {
        store.rollback(TestUtils.fromLong(50))
        version = 51
        keys = rollbackKeys
      }

      //check for disk leaks
      assert(storeSize < 200 * 1024 * 1024)
    }
    store.close()
  }

  @Test def reopen() {
    var store = makeStore()
    val endTime = TestUtils.endTimestamp()

    var keys = Seq.empty[ByteArrayWrapper]

    var version = 1
    while (System.currentTimeMillis() < endTime) {
      keys.foreach { it =>
        assert(it == store(it))
      }

      val newKeys = (0 until 10000).map(i => TestUtils.randomA())
      val toUpdate = newKeys.map(a => (a, a))
      val versionID = TestUtils.fromLong(version)
      store.update(versionID, keys, toUpdate)
      assert(store.lastVersionID.get == versionID)
      version += 1
      if (version % 5 == 0)
        store.clean(version-10)

      keys = newKeys
      //check for disk leaks
      assert(storeSize < 200 * 1024 * 1024)

      store.close()
      store = makeStore()
    }
    store.close()
  }

  @Test def reopen_rollback() {
    var store = makeStore()
    val endTime = TestUtils.endTimestamp()

    var keys = Seq.empty[ByteArrayWrapper]
    var rollbackKeys = Seq.empty[ByteArrayWrapper]

    var version = 1

    while (System.currentTimeMillis() < endTime) {
      keys.foreach { it =>
        assert(it == store(it))
      }

      val newKeys = (0 until 10000).map(i => TestUtils.randomA())
      val toUpdate = newKeys.map(a => (a, a))
      val versionID = TestUtils.fromLong(version)
      store.update(versionID, keys, toUpdate)
      assert(store.lastVersionID.get == versionID)

      if (version == 50) {
        rollbackKeys = newKeys
      }

      version += 1

      keys = newKeys
      if (version > 100) {
        store.rollback(TestUtils.fromLong(50))
        version = 51
        keys = rollbackKeys
      }

      //check for disk leaks
      assert(storeSize < 200 * 1024 * 1024)

      store.close()
      store = makeStore()
    }
    store.close()
  }

  @Test def rollback_to_skipped_version(): Unit = {
    val store = makeStore()

    store.update(TestUtils.fromLong(1), Seq.empty, Seq.empty)
    store.update(TestUtils.fromLong(2), Seq.empty, Seq.empty)

    store.update(TestUtils.fromLong(5), Seq.empty, Seq.empty)
    store.update(TestUtils.fromLong(6), Seq.empty, Seq.empty)

    store.rollback(TestUtils.fromLong(2)) //was 4, but that no longer works with non sequential version IDs
    assert(store.lastVersionID.get == TestUtils.fromLong(2))
    store.close()
  }

}



class LSMStoreBurnTest extends StoreBurnTest {
  def makeStore(): Store = {
    new LSMStore(dir = dir, keySize = 32)
  }
}
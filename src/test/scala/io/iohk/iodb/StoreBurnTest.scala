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
        assert(it === store.get(it))
      }

      val newKeys = (0 until 10000).map(i => TestUtils.randomA())
      val toUpdate = newKeys.map(a => (a, a))
      store.update(version, keys, toUpdate)
      assert(store.lastVersion === version)
      version += 1
      if (version % 5 == 0)
        store.clean(version-10)
      keys = newKeys

      //check for disk leaks
      assert(storeSize < 200 * 1024 * 1024)
    }
  }

  @Test def continous_rollback() {
    val store = makeStore()
    val endTime = TestUtils.endTimestamp()

    var keys = Seq.empty[ByteArrayWrapper]
    var rollbackKeys = Seq.empty[ByteArrayWrapper]

    var version = 1

    while (System.currentTimeMillis() < endTime) {
      keys.foreach { it =>
        assert(it === store.get(it))
      }

      val newKeys = (0 until 10000).map(i => TestUtils.randomA())
      val toUpdate = newKeys.map(a => (a, a))
      store.update(version, keys, toUpdate)
      assert(store.lastVersion === version)

      if (version == 50) {
        rollbackKeys = newKeys
      }

      version += 1

      keys = newKeys
      if (version > 100) {
        store.rollback(50)
        version = 51
        keys = rollbackKeys
      }

      //check for disk leaks
      assert(storeSize < 200 * 1024 * 1024)
    }
  }

  @Test def reopen() {
    var store = makeStore()
    val endTime = TestUtils.endTimestamp()

    var keys = Seq.empty[ByteArrayWrapper]

    var version = 1
    while (System.currentTimeMillis() < endTime) {
      keys.foreach { it =>
        assert(it === store.get(it))
      }

      val newKeys = (0 until 10000).map(i => TestUtils.randomA())
      val toUpdate = newKeys.map(a => (a, a))
      store.update(version, keys, toUpdate)
      assert(store.lastVersion === version)
      version += 1
      if (version % 5 == 0)
        store.clean(version-10)

      keys = newKeys
      //check for disk leaks
      assert(storeSize < 200 * 1024 * 1024)

      store.close()
      store = makeStore()
    }

  }

  @Test def reopen_rollback() {
    var store = makeStore()
    val endTime = TestUtils.endTimestamp()

    var keys = Seq.empty[ByteArrayWrapper]
    var rollbackKeys = Seq.empty[ByteArrayWrapper]

    var version = 1

    while (System.currentTimeMillis() < endTime) {
      keys.foreach { it =>
        assert(it === store.get(it))
      }

      val newKeys = (0 until 10000).map(i => TestUtils.randomA())
      val toUpdate = newKeys.map(a => (a, a))
      store.update(version, keys, toUpdate)
      assert(store.lastVersion === version)

      if (version == 50) {
        rollbackKeys = newKeys
      }

      version += 1

      keys = newKeys
      if (version > 100) {
        store.rollback(50)
        version = 51
        keys = rollbackKeys
      }

      //check for disk leaks
      assert(storeSize < 200 * 1024 * 1024)

      store.close()
      store = makeStore()
    }
  }

  @Test def rollback_to_skipped_version(): Unit = {
    val store = makeStore()


    store.update(1, Seq.empty, Seq.empty)
    store.update(2, Seq.empty, Seq.empty)

    store.update(5, Seq.empty, Seq.empty)
    store.update(6, Seq.empty, Seq.empty)

    store.rollback(4)
    assert(store.lastVersion == 2)
  }

}


class TrivialStoreBurnTest extends StoreBurnTest {
  def makeStore(): Store = {
    new TrivialStore(dir = dir, keySize = 32)
  }
}

class LogStoreBurnTest extends StoreBurnTest {
  def makeStore(): Store = {
    new LogStore(dir = dir, filePrefix="store", keySize = 32)
  }
}


class LSMStoreBurnTest extends StoreBurnTest {
  def makeStore(): Store = {
    new LSMStore(dir = dir, keySize = 32)
  }
}
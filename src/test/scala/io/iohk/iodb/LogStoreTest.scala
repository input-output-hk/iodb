package io.iohk.iodb

import java.util

import org.junit.Test

import scala.collection.mutable

class LogStoreTest extends TestWithTempDir {

  @Test def binary_search(): Unit = {
    val store = new LogStore(dir = dir, filePrefix = "store")

    val s = new mutable.TreeSet[ByteArrayWrapper]()
    for (i <- 0 until 1000) {
      s.add(TestUtils.randomA(size = 32))
    }
    store.update(1, Seq.empty, s.map { a => (a, a) })

    for (a <- s) {
      val a2 = store.get(a)
      assert(a === a2)
    }
  }

  @Test def clean_empty(): Unit = {
    val store = new LogStore(dir = dir, filePrefix = "store")
    val ff = store.getFiles()
    store.clean(100)
    assert(ff === store.getFiles())
  }


  @Test def clean_versions(): Unit = {
    val filePrefix = "store"
    var store = new LogStore(dir = dir, filePrefix = filePrefix, keySize = 8)

    for (i <- 1L until 100) {
      val b = TestUtils.fromLong(i)
      store.update(i, Nil, List((b, b)))
    }
    def last = store.getFiles().lastEntry().getValue

    store.clean(20)
    assert(store.getFiles().size === 80)
    assert(last.isMerged)
    assert(last.version === 20)

    def checkExists(version: Long) = {
      for (i <- 1L until 100) {
        val b = TestUtils.fromLong(i)
        assert(b == store.get(b))

        assert((i == version) == LogStore.keyFile(i, dir = dir, filePrefix = filePrefix, isMerged = true).exists())
        assert((i == version) == LogStore.valueFile(i, dir = dir, filePrefix = filePrefix, isMerged = true).exists())
        assert((i > version) == LogStore.keyFile(i, dir = dir, filePrefix = filePrefix).exists())
        assert((i > version) == LogStore.valueFile(i, dir = dir, filePrefix = filePrefix).exists())
      }

    }
    checkExists(20)

    store.clean(40)
    assert(store.getFiles().size === 60)
    assert(last.isMerged)
    assert(last.version === 40)
    checkExists(40)

    //reopen
    val oldFiles = new util.TreeMap(store.getFiles())
    store.close()
    store = new LogStore(dir = dir, filePrefix = "store", keySize = 8)

    assert(oldFiles === store.getFiles())

    checkExists(40)
  }

  @Test def reopen(): Unit = {
    var store = new LogStore(dir = dir, filePrefix = "store", keySize = 8)

    val c = 100
    for (version <- (1 until c)) {
      val toUpdate = (version * c until version * c + c).map(k => (TestUtils.fromLong(k), TestUtils.fromLong(k)))
      store.update(version, Nil, toUpdate)

      //produce merged file every 10 updates
      if (version % 10 == 0)
        store.merge(version, store.keyValues(version))
    }

    val files = store.getFiles()
    store.close()
    store = new LogStore(dir = dir, filePrefix = "store", keySize = 8)

    assert(files == store.getFiles())
    store.close()
  }
}
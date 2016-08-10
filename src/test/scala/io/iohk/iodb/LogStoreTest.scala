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
    var store = new LogStore(dir = dir, filePrefix = "store", keySize = 8)

    for (i <- 0L until 100) {
      val b = TestUtils.fromLong(i)
      store.update(i, Nil, List((b, b)))
    }
    def last = store.getFiles().lastEntry().getValue

    store.clean(20)
    assert(store.getFiles().size === 80)
    assert(last.isMerged)
    assert(last.version === 20)

    def checkExists(version: Long) = {
      for (i <- 0L until 100) {
        val b = TestUtils.fromLong(i)
        assert(b === store.get(b))

        assert((i == version) === store.keyFile(i, isMerged = true).exists())
        assert((i == version) === store.valueFile(i, isMerged = true).exists())
        assert((i > version) === store.keyFile(i).exists())
        assert((i > version) === store.valueFile(i).exists())
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

    //use .toString because LogFile has reference to LogStore, that makes equality different
    assert(oldFiles.toString === store.getFiles().toString)

    checkExists(40)
  }
}
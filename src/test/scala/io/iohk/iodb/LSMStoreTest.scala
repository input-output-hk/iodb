package io.iohk.iodb

import org.junit.Test

import scala.collection.mutable

class LSMStoreTest extends TestWithTempDir {

  @Test def binary_search(): Unit = {
    val store = new LSMStore(dir = dir)

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
}

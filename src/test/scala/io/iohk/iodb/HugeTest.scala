package io.iohk.iodb

import org.junit.Test
import org.scalatest.Assertions

/**
  * Tests huge  store
  */
class HugeTest extends Assertions with TestWithTempDir {

  val giga: Long = 1024 * 1024 * 1024

  @Test def compaction() {
    if (TestUtils.longTest() < 4) return
    val spaceReq = 5 * giga

    assert(dir.getFreeSpace > spaceReq, "not enough free space")

    val store = new LSMStore(dir = dir)

    //fill with updates
    var version = 1L
    while (storeSize < spaceReq) {
      val d = (0 until 1000000).map(a => (TestUtils.randomA(), TestUtils.randomA()))
      store.update(TestUtils.fromLong(version), List.empty, d)
      version += 1
    }

    store.clean(10)
    assert(storeSize < spaceReq / 10)
    store.close()
  }
}

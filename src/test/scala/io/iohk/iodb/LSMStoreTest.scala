package io.iohk.iodb

import org.junit.Test

class LSMStoreTest extends TestWithTempDir {

  @Test def testShard(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8, splitSize = 1024)
    val toUpdate = (1L to 100000L).map(TestUtils.fromLong).map(k => (k, k))

    store.update(versionID = 1L, toRemove = Nil, toUpdate = toUpdate)
    store.taskShardLogForce()
    store.taskShardMerge()

    assert(store.getShards.size() > 1)
  }

}
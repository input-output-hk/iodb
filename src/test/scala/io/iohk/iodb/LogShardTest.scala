package io.iohk.iodb

import org.junit.Test

class LogShardTest extends TestWithTempDir {

  def sharded(): LSMStore = {
    val sharded = new LSMStore(dir = dir, keySize = 8)

    for (i <- 10L until 100 by 10) {
      val key = TestUtils.fromLong(i)
      val endKey = if (i == 90) null else TestUtils.fromLong(i + 10)
      sharded.shardAdd(key, endKey, versionID = key, version = 0L)
    }
    sharded
  }

  @Test def shardCount(): Unit = {
    val s = sharded()
    assert(s.getShards.size == 10)
    sharded().close()
  }

  @Test def readFromShard(): Unit = {
    for (i <- 1L until 110) {
      val s = sharded()
      val key = TestUtils.fromLong(i)
      s.update(TestUtils.fromLong(i), Nil, List((key, key)))
      s.taskShardLogForce()
      assert(s.getFromShard(key) === Some(key))
      s.close()
    }
  }
}

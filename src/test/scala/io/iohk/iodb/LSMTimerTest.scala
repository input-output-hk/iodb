package io.iohk.iodb

import org.junit.Test

class LSMTimerTest extends TestWithTempDir {

  @Test(timeout = 10000L)
  def shard(): Unit ={
    val s = new LSMStore(dir=dir, keySize = 8)

    for(i <- 0 until 100){
      val key = TestUtils.fromLong(i)
      s.update(i, Nil, List((key, key)))
    }
    val key = TestUtils.fromLong(10)

    //wait until one of the keys is available in shard
    while(s.getFromShard(key)==null)
      Thread.sleep(10)
  }


}

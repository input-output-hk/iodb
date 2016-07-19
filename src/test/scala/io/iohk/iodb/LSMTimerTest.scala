package io.iohk.iodb

import org.junit.{Ignore, Test}

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
    while(s.getFromShardBuffer(key)==null)
      Thread.sleep(10)
  }


  @Test(timeout = 20000L) @Ignore
  def shard_and_merge(): Unit ={
    val s = new LSMStore(dir=dir, keySize = 8)

    for(i <- 0 until 100){
      val key = TestUtils.fromLong(i)
      s.update(i, Nil, List((key, key)))
    }
    val key = TestUtils.fromLong(10)

    //wait until one of the keys is available in shard
    while(s.getFromShardBuffer(key)==null)
      Thread.sleep(10)

    //now key should be moved to index
    while(s.getFromShardIndex(key)==null)
      Thread.sleep(10)

    //and is no longer in buffer
    assert(s.getFromShardBuffer(key)===null)
  }




}

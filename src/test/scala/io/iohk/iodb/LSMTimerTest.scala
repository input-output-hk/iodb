package io.iohk.iodb

import org.junit.Test

class LSMTimerTest extends TestWithTempDir {

  @Test(timeout = 30000L)
  def shard(): Unit ={
    val s = new LSMStore(dir=dir, keySize = 8)

    for (i <- 1 until 100) {
      val key = TestUtils.fromLong(i)
      s.update(TestUtils.fromLong(i), Nil, List((key, key)))
    }
    val key = TestUtils.fromLong(10)

    //wait until one of the keys is available in shard
    while(s.getFromShard(key)==null)
      Thread.sleep(10)

    s.close()
  }


  @Test(timeout = 20000L)
  def shard_and_merge(): Unit ={
    val s = new LSMStore(dir=dir, keySize = 8)

    var version = 1L
    //wait until merge file was created
    while(
      !dir.listFiles().exists(_.getName.contains(".merged"))
    ){
      //add new version
      val key = TestUtils.fromLong(version)
      s.update(versionID = key, Nil, toUpdate = List((key, key)))
      version += 1
      Thread.sleep(1)
    }
    s.close()
  }
}
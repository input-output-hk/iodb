package io.iohk.iodb

import java.util

import org.junit.{Ignore, Test}

class LSMTimerTest extends TestWithTempDir {

  @Test(timeout = 20000L)
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


  @Test(timeout = 20000L)
  def shard_and_merge(): Unit ={
    val s = new LSMStore(dir=dir, keySize = 8)

    var version = 1L
    //wait until merge file was created
    while(
      dir.listFiles().filter {_.getName.contains(".merged")}.isEmpty
    ){
      //add new version
      val key = TestUtils.fromLong(version)
      s.update(version, Nil, toUpdate = List((key,key)))
      version += 1
      Thread.sleep(1)
    }



  }




}

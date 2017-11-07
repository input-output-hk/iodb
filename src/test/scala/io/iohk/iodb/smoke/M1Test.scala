package io.iohk.iodb.smoke

import java.io.{BufferedOutputStream, FileOutputStream}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption

import io.iohk.iodb.{ByteArrayWrapper, ShardedStore, Store, TestUtils}
import io.iohk.iodb.Store._
import TestUtils._

import scala.util.Random
import org.scalatest.Matchers._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/** tests inserts, updates, deletes and rollbacks */
object M1Test {

  val defaultKeyCount = 1000000
  val defaultDuration = 12L

  val defaultShardCount = 16

  val batchSize = 1e5.toLong


  def main(args: Array[String]): Unit = {


    val keyCount = if(args.length>=1) args(0).toInt else defaultKeyCount
    val duration = if(args.length>=2) args(1).toLong else defaultDuration
    val shardCount = if(args.length>=3) args(2).toInt else defaultShardCount

    val dir = TestUtils.tempDir()

    def dirSize() = dir.listFiles().map(_.length()).sum

    def dirSizeGiga(): String = (dirSize().toDouble / 1e9) + " GB"


    val store = new ShardedStore(dir = dir, keySize = 8, shardCount = shardCount)

    try {
      println("KeyCount: " + keyCount)
      println("Shard count: " + shardCount)
      println("Dir: " + dir.getPath)

      //insert initial data
      for (i <- 0L until keyCount by batchSize) {
        val value = valueFromSeed(1)
        val keyVals = (i until Math.min(keyCount, i + batchSize)).map(k => (fromLong(k), value))

        store.update(versionID = fromLong(-i), toUpdate = keyVals, toRemove = Nil)
      }

      val initRollbackVersionsSize = store.rollbackVersions().size

      println("Store populated, dir size: " + dirSizeGiga())

      //initialize reference file with given size:
      // Reference file is used to calculate content of store.
      // Each byte in file represents one key (byte offset is key value).
      // Byte value at given offset represents seed value associated with given key, where 0 is deleted key
      val refFile = createRefFile(keyCount, 1)

      val endTime = System.currentTimeMillis() + duration * 3600 * 1000

      var history = new mutable.ArrayBuffer[Long]()

      val r = new Random()
      while (System.currentTimeMillis() < endTime) {
        if (history.size > 5 && r.nextInt(20) == 0) {
          //rollback
          var cutOffset = r.nextInt(history.size - 2)
          var versionID = fromLong(history(cutOffset))
          history = history.take(cutOffset + 1)
          store.rollback(versionID)
          println("rollback")

          //replay actions on reference file to match content of store
          replayRefFile(refFile, history, keyCount)
        } else {
          val vlong = Math.abs(r.nextLong)
          var version = fromLong(vlong)
          //update some keys and delete some existing keys

          history.append(vlong)
          val (toUpdate, toRemove) = alterFile(vlong, refFile, keyCount)

          store.update(version, toUpdate = toUpdate, toRemove = toRemove)
        }

        if(r.nextInt(100)==0) {

          //verify store
          store.rollbackVersions().toBuffer.drop(initRollbackVersionsSize) shouldBe history.map(fromLong(_))
          //iterate over all keys in ref  file, ensure that store content is identical
          for (offset <- 0 until keyCount) {
            val seed = refFile.get(offset)
            val value = if (seed == 0) None else Some(valueFromSeed(seed))
            val key = fromLong(offset)
            store.get(key) shouldBe value
          }

          println("Ver: " + history.size + " - disk size: " + dirSizeGiga())
        }
      }

    }finally{
        deleteRecur(dir)
        store.close()
    }

  }


  def valueFromSeed(seed:Byte):ByteArrayWrapper = {
    assert(seed!=0)
    val random = new Random(seed)
    val size = random.nextInt(100)
    randomA(size=size, random)
  }


  /*
    */
  def createRefFile(keyCount:Int, valueSeed:Byte):ByteBuffer = {
    assert(keyCount<=Integer.MAX_VALUE, "keyCount must be < 2G")
    val f = tempFile()
    val fout = new FileOutputStream(f)
    val bout = new BufferedOutputStream(fout)

    //fill with data
    for(i <- 0L until keyCount){
      bout.write(valueSeed)
    }
    bout.flush()
    fout.close()

    //memory map
    val channel = FileChannel.open(f.toPath, StandardOpenOption.READ, StandardOpenOption.WRITE)
    val buf = channel.map(MapMode.READ_WRITE, 0, keyCount)
    channel.close()

    f.delete()
    f.deleteOnExit()

    return buf
  }


  def alterFile(v:Long, ref:ByteBuffer, keyCount:Int): (Iterable[(K,V)], Iterable[K]) ={
    val r = new Random(v)
    val updateSize = r.nextInt(2000)
    val removeSize = r.nextInt(1000)

    val keys = (0 until updateSize).map(i=>r.nextInt(keyCount)).toSet
    val keyVals = keys.toBuffer.sorted.map { k =>
      var seed = r.nextInt().toByte
      if(seed==0) seed=1
      ref.put(k, seed) //update reference file
      (fromLong(k), valueFromSeed(seed))
    }

    //find some keys for deletion
    val keysToRemove = (0 until removeSize).map(i=>r.nextInt(keyCount))
      .filter(ref.get(_)!=0)
      .filter(!keys.contains(_))
      .toSet.toBuffer.sorted.map { k =>
      ref.put(k, 0) //update reference file
      fromLong(k)
    }

    (keyVals, keysToRemove)
  }

  def replayRefFile(refFile: ByteBuffer, history: ArrayBuffer[FileNum], keyCount:Int) = {
    //first fill with 1
    for(offset <- 0 until keyCount){
      refFile.put(offset, 1)
    }
    //now replay all actions
    for(version<-history){
      alterFile(version, refFile, keyCount)
    }
  }


}

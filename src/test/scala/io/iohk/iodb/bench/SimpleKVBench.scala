package io.iohk.iodb.bench

import java.io.File
import java.util.Random

import io.iohk.iodb._
import org.junit.Test

class SimpleKVBench{
  @Test def run(): Unit ={
    SimpleKVBench.main(Array("1000", "100"))
  }

}

case class BenchResult(storage: String, insertTime: Long, getTime: Long, storeSizeMb: Long)

/**
  * Benchmark for IODB
  */
object SimpleKVBench extends Benchmark{

  var defaultUpdates = 1000
  var defaultKeyCount = 100

  def main(args: Array[String]) {
    val updates = if (args.length > 0) args(0).toInt else defaultUpdates
    val keyCount = if (args.length > 0) args(0).toInt else defaultKeyCount
    var dir = TestUtils.tempDir()
    dir.mkdirs()
    val lb = bench(
      store = new ShardedStore(dir, keySize = KeySize, shardCount = 10),
      dir = dir,
      updates = updates,
      keyCount = keyCount)
    TestUtils.deleteRecur(dir)
    printlnResult(lb)

    dir = TestUtils.tempDir()
    dir.mkdirs()
    val rb = bench(
      store = new RocksStore(dir),
      dir = dir,
      updates = updates,
      keyCount = keyCount)
    printlnResult(rb)
    TestUtils.deleteRecur(dir)

    //    dir = TestUtils.tempDir()
    //    val lvb = bench(
    //      store = new LevelDBStore(dir),
    //      dir = dir,
    //      updates = updates,
    //      keyCount = keyCount)
    //    printlnResult(lvb)
    //    TestUtils.deleteRecur(dir)

    printf("Commit count: %,d \n", updates)
    printf("Keys per update: %,d \n", keyCount)

  }

  def bench(store: Store, dir: File, updates: Int, keyCount: Int): BenchResult = {
    val r = new Random(1)
    var version = 0
    //insert random values
    val insertTime = TestUtils.runningTimeUnit {
      for (i <- 0 until updates) {
        val toInsert = (0 until keyCount).map { a =>
          val k = randomKey(r)
          (k, k)
        }
        version += 1
        store.update(version, List.empty, toInsert)
      }
    }

    Thread.sleep(10000)

    val getTime = TestUtils.runningTimeUnit {
        val r = new Random(1)
        for (i <- 0 until updates) {
          val toGet = (0 until keyCount).map { j =>
            randomKey(r)
          }

          version += 1

          toGet.foreach { k =>
            assert(null != store.get(k))
          }
        }
    }

    val br = BenchResult(store.getClass.toString, insertTime, getTime, TestUtils.dirSize(dir) / (1024 * 1024))

    store.close()
    TestUtils.deleteRecur(dir)
    br
  }

  def randomKey(r: Random): ByteArrayWrapper = {
    val key = new Array[Byte](KeySize)
    r.nextBytes(key)
    ByteArrayWrapper(key)
  }

  def printlnResult(res: BenchResult): Unit = {
    println("Store: " + res.storage)
    printf("Insert time: %,d \n", res.insertTime / 1000)
    printf("Get time: %,d \n", res.getTime / 1000)
    printf("Store size: %,d MB \n", res.storeSizeMb)
  }
}

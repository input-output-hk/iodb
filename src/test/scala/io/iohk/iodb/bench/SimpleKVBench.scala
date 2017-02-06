package io.iohk.iodb.bench

import java.io.File
import java.util.Random

import io.iohk.iodb._

case class BenchResult(storage: String, insertTime: Long, getTime: Long, storeSizeMb: Long)

/**
  * Benchmark for IODB
  */
object SimpleKVBench extends Benchmark{

  var updates = 1000
  var keyCount = 100

  def main(args: Array[String]) {
    if (args.length > 0) {
      updates = args(0).toInt
      keyCount = args(1).toInt
    }
    var dir = TestUtils.tempDir()
    val lb = bench(new LSMStore(dir, keySize = KeySize
    ), dir)
    TestUtils.deleteRecur(dir)
    printlnResult(lb)

    dir = TestUtils.tempDir()
    val rb = bench(new RocksStore(dir), dir)
    printlnResult(rb)
    TestUtils.deleteRecur(dir)

    printf("Commit count: %,d \n", updates)
    printf("Keys per update: %,d \n", keyCount)

    if (lb.getTime < rb.getTime && lb.insertTime < rb.insertTime) {
      println("IODB won!")
    }
  }

  def bench(store: Store, dir: File): BenchResult = {
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

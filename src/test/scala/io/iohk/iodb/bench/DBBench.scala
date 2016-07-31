package io.iohk.iodb.bench

import java.io.File
import java.util.Random

import io.iohk.iodb._

case class BenchResult(storage: String, insertTime: Long, getTime: Long, storeSizeMb: Long)

/**
  * Benchmark for IODB
  */
object DBBench {

  val updates = 10000
  val keyCount = 100
  val keySize = 32
  val valueSize = 128

  def main(args: Array[String]) {
    var dir = TestUtils.tempDir()
    val lb = bench(new LSMStore(dir, keySize = keySize, keepSingleVersion = true), dir)
    printlnResult(lb)

    dir = TestUtils.tempDir()
    val rb = bench(new RocksStore(dir), dir)
    printlnResult(rb)

    if (lb.getTime < rb.getTime && lb.insertTime < rb.insertTime) {
      println("IODB won!")
    }
  }

  def bench(store: Store, dir: File): BenchResult = {
    val r = new Random(1)
    var version = 0
    //insert random values
    val insertTime = TestUtils.stopwatch { () =>
      for (i <- 0 until updates) {
        val toInsert = (0 until keyCount).map { i =>
          val key = new Array[Byte](keySize)
          r.nextBytes(key)
          val value = new Array[Byte](valueSize)
          r.nextBytes(value)
          (ByteArrayWrapper(key), ByteArrayWrapper(value))
        }

        version += 1
        store.update(version, List.empty, toInsert)
      }
    }


    val getTime = TestUtils.stopwatch { () =>
      val r = new Random(1)
      for (i <- 0 until updates) {
        val toGet = (0 until keyCount).map { i =>
          val key = new Array[Byte](keySize)
          r.nextBytes(key)
          val value = new Array[Byte](valueSize)
          r.nextBytes(value)
          ByteArrayWrapper(key)
        }

        version += 1

        toGet.foreach(store.get)
      }
    }

    val br = BenchResult(store.getClass.toString, insertTime, getTime, TestUtils.dirSize(dir) / (1024 * 1024))

    store.close()
    TestUtils.deleteRecur(dir)
    br
  }

  def printlnResult(res: BenchResult): Unit = {
    println(s"Store: ${res.storage}")
    println(s"Insert time:  ${res.insertTime}")
    println(s"Get time: ${res.getTime}")
    println(s"Store size: ${res.storeSizeMb} MB")
  }
}

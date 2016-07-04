package io.iohk.iodb

import java.io.File
import java.util.Random

/**
  * Benchmark for IODB
  */
object DBBench {

  val updates = 1000
  val keyCount = 100
  val keySize = 32
  val valueSize = 128

  def main(args: Array[String]) {
    var dir = TestUtils.tempDir()
    bench(new LSMStore(dir, keySize = keySize), dir)

    dir = TestUtils.tempDir()
    bench(new RocksStore(dir), dir)
  }

  def bench(store: Store, dir:File): Unit = {
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
          (new ByteArrayWrapper(key), new ByteArrayWrapper(value))
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
          new ByteArrayWrapper(key)
        }

        version += 1

        toGet.foreach(store.get(_))
      }
    }

    println("Store: "+store.getClass)
    println("Insert time:  " + insertTime)
    println("Get time:     " + getTime)
    println("Store size: "+TestUtils.dirSize(dir)/(1024*1024)+" MB")

    store.close()
    TestUtils.deleteRecur(dir)
  }
}

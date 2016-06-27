package io.iohk.iodb

import java.util.Random

/**
  * Benchmark for IODB
  */
object DBBench {

  val updates = 100
  val keyCount = 100
  val keySize = 32
  val valueSize = 128

  def main(args: Array[String]) {

    val dir = TestUtils.tempDir()
    val store = new LSMStore(dir, keySize = keySize)

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

    println("Insert time:  " + insertTime)
    println("Get time:     " + getTime)

    store.close()
    TestUtils.deleteRecur(dir)
  }

}

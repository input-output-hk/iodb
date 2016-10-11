package io.iohk.iodb.bench

import java.io.File
import io.iohk.iodb.{ByteArrayWrapper, LSMStore, Store, TestUtils}

/**
  * Performance benchmark utility simulating initial blockchain processing
  */
object InitialProcessing extends Benchmark {
  val Updates = 10000L //should be 1M finally

  val Inputs = 1900
  //average number of inputs per block
  val Outputs = 2100 //average number of outputs per block

  def bench(store: Store, dir: File): Unit = {
    val time = TestUtils.runningTime {
      (1L to Updates).foldLeft(Seq[ByteArrayWrapper]()) { case (cache, version) =>
        processBlock(version, store, Inputs, Outputs, cache).get.take(Inputs * 100)
      }
    }
    println(s"Store: $store")
    println(s"Test time: $time")
    store.close()
    TestUtils.deleteRecur(dir)
  }

  def main(args: Array[String]): Unit = {
    var dir = TestUtils.tempDir()
    bench(new LSMStore(dir, keySize = KeySize, keepSingleVersion = true), dir)

    dir = TestUtils.tempDir()
    bench(new RocksStore(dir), dir)
  }
}
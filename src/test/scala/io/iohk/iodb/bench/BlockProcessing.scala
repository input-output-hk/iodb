package io.iohk.iodb.bench

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import io.iohk.iodb.{ByteArrayWrapper, LSMStore, Store, TestUtils}

object BlockProcessing extends Benchmark {

  val InitialSize = 500000 //000

  val Outputs = 1000 //0
  val Blocks = 50

  val keysCache = Seq[ByteArrayWrapper]()

  var version = new AtomicLong(1)

  def bench(store: Store, dir: File): Unit = {
    (1 to 1000).foreach { v =>
      val toInsert = (1 to InitialSize / 1000).map(_ => randomKV())
      if (v % Blocks == 0) keysCache ++ toInsert.map(_._1)
      store.update(version.incrementAndGet(), Seq.empty, toInsert)
    }

    println("Initial data is loaded into the store")

    val (_, ts ) = (1L to Blocks).foldLeft((Seq[ByteArrayWrapper](), Seq[Long]())) {case ((cache, times), v) =>
      val (time, newCache) = TestUtils.runningTime(processBlock(version.incrementAndGet, store, Outputs, Outputs, cache).get)
      (newCache, times ++ Seq(time))
    }

    val avgTime = ts.sum / Blocks

    println(s"Store: $store")
    println(s"Avg block processing time: $avgTime")
    store.close()
    TestUtils.deleteRecur(dir)
  }

  def main(args: Array[String]): Unit = {
    var dir = TestUtils.tempDir()
    bench(new LSMStore(dir, keySize = keySize, keepSingleVersion = true), dir)

    dir = TestUtils.tempDir()
    bench(new RocksStore(dir), dir)
  }
}
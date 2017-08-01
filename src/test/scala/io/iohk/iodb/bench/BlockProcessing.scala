package io.iohk.iodb.bench

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import io.iohk.iodb.{ByteArrayWrapper, ShardedStore, Store, TestUtils}

object BlockProcessing extends Benchmark with App {

  val InitialSize = 5000000

  val InputsPerBlock = 5500
  val OutputsPerBlock = 6000
  val Blocks = 2000

  val keysCache = Seq[ByteArrayWrapper]()

  var version = new AtomicLong(1)

  def bench(store: Store, dir: File): Unit = {
    (1 to 1000).foreach { v =>
      val toInsert = (1 to InitialSize / 1000).map(_ => randomKV())
      if (v % Blocks == 0) keysCache ++ toInsert.map(_._1)
      store.update(version.incrementAndGet(), Seq.empty, toInsert)
    }

    println("Initial data is loaded into the store")

    val (_, ts) = (1L to Blocks).foldLeft((Seq[ByteArrayWrapper](), Seq[Long]())) { case ((cache, times), v) =>
      val (time, newCache) = TestUtils.runningTime(processBlock(version.incrementAndGet, store, InputsPerBlock, OutputsPerBlock, cache).get)
      println(s"Block processing time for block# $v: " + time)
      (newCache, times ++ Seq(time))
    }

    val totalTime = ts.sum

    println(s"Store: $store")
    println(s"Total processing time: $totalTime")
    println(s"Avg block processing time: ${totalTime / Blocks.toFloat}")
    store.close()
    TestUtils.deleteRecur(dir)
  }


  var dir = TestUtils.tempDir()
  bench(new ShardedStore(dir, keySize = KeySize), dir)

  println("===========================")

  dir = TestUtils.tempDir()
  bench(new RocksStore(dir), dir)
}
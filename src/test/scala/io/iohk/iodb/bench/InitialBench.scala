package io.iohk.iodb.bench

import java.io.File

import io.iohk.iodb.{ByteArrayWrapper, LSMStore, Store, TestUtils}

import scala.util.{Random, Try}


object InitialBench {
  type Key = Array[Byte]

  val keySize = 32
  val valueSize = 256

  val updates = 1000L //should be 1M finally

  val inputs = 1900
  //average number of inputs per block
  val outputs = 2100 //average number of outputs per block

  def update(version: Long, store: Store, keysCache: Seq[ByteArrayWrapper]): Try[Seq[ByteArrayWrapper]] = {
    val cacheSize = keysCache.size

    val (toRemove, cacheRem) = if (cacheSize > inputs) {
      val sliceStart = Random.nextInt(cacheSize - inputs)
      keysCache.slice(sliceStart, sliceStart + inputs) ->
        (keysCache.take(sliceStart) ++ keysCache.drop(sliceStart + inputs))
    } else (keysCache, Seq())

    val toAppend = (1 to outputs).map { _ =>
      val key = new Array[Byte](keySize)
      Random.nextBytes(key)
      val value = new Array[Byte](valueSize)
      Random.nextBytes(value)
      ByteArrayWrapper(key) -> ByteArrayWrapper(value)
    }

    Try(store.update(version, toRemove, toAppend)).map { _ =>
      cacheRem ++ toAppend.map(_._1)
    }
  }

  def bench(store: Store, dir: File): Unit = {
    val time = TestUtils.runningTime { () =>
      (1L to updates).foldLeft(Seq[ByteArrayWrapper]()) { case (cache, version) =>
        update(version, store, cache).get
      }
    }
    println(s"Store: $store")
    println(s"Test time: $time")
    TestUtils.deleteRecur(dir)
  }

  def main(args: Array[String]): Unit = {
    var dir = TestUtils.tempDir()
    bench(new LSMStore(dir, keySize = keySize, keepSingleVersion = true), dir)

    dir = TestUtils.tempDir()
    bench(new RocksStore(dir), dir)
  }
}

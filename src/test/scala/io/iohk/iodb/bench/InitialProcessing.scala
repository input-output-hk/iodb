package io.iohk.iodb.bench

import java.io.File

import ch.qos.logback.classic.LoggerContext
import io.iohk.iodb.{ByteArrayWrapper, LSMStore, Store, TestUtils}
import org.slf4j.LoggerFactory

/**
  * Performance benchmark utility simulating initial blockchain processing
  */
object InitialProcessing extends Benchmark {
  val Milestones = Seq(1000, 5000, 10000, 50000, 100000, 250000, 500000, 750000, 1000000)

  val Inputs = 5500
  //average number of inputs per block
  val Outputs = 6000 //average number of outputs per block

  def bench(store: Store, dir: File): Unit = {
    println(s"Store: $store")

    Milestones.foldLeft((0, 0L, Seq[ByteArrayWrapper]())) {
      case ((prevMilestone, prevTime, prevCache), milestone) =>
        val (time, newCache) = TestUtils.runningTime {
          (prevMilestone + 1 to milestone).foldLeft(prevCache) { case (cache, version) =>
            processBlock(version, store, Inputs, Outputs, cache).get.take(Inputs * 100)
          }
        }
        val newTime = prevTime + time
        println(s"Time  to get to $milestone: $time")
        (milestone, newTime, newCache)
    }

    store.close()
    TestUtils.deleteRecur(dir)
  }

  def main(args: Array[String]): Unit = {
    //switching off logging
    val context = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    context.stop()

    var dir = TestUtils.tempDir()
    bench(new LSMStore(dir, keySize = KeySize), dir)

    System.gc()
    Thread.sleep(15000)
    println("======================================")

    dir = TestUtils.tempDir()
    bench(new RocksStore(dir), dir)
  }
}
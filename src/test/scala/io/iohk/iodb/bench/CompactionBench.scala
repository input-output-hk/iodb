package io.iohk.iodb.bench

import java.util.concurrent.atomic.AtomicLong

import io.iohk.iodb.TestUtils._
import io.iohk.iodb._

/**
  * Benchmark to test how background compaction is blocking updates.
  * We should get decent number of updates, if the compaction does not block writer threads.
  */
object CompactionBench {

  def main(args: Array[String]): Unit = {
    val time: Long = 60
    val endTime = System.currentTimeMillis() + time * 1000

    //start background thread with updates, while compaction runs on foreground
    val dir = TestUtils.tempDir()
    val store = new LogStore(dir = dir, keySize = 8, keepVersions = 0)
    val updateCounter = new AtomicLong(1)
    var compactionCounter = 0L

    val updateThread = new Thread(runnable {
      while (System.currentTimeMillis() < endTime) {
        val value = updateCounter.incrementAndGet()
        val toUpdate = List((fromLong(1L), fromLong(value)))
        store.update(versionID = fromLong(1L), toUpdate = toUpdate, toRemove = Nil)
      }
    })
    updateThread.setDaemon(false)
    updateThread.start()


    while (System.currentTimeMillis() < endTime) {
      store.compact()
      store.clean(0)
      compactionCounter += 1
    }

    //wait until Update Thread finishes
    while (updateThread.isAlive) {
      Thread.sleep(1)
    }


    println("Runtime: " + (time / 1000) + " seconds")
    println("Update count: " + updateCounter.get())
    println("Compaction count: " + compactionCounter)

    store.close()
    deleteRecur(dir)
  }

}

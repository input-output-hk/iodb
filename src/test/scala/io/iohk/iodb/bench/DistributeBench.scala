package io.iohk.iodb.bench

import io.iohk.iodb.TestUtils._
import io.iohk.iodb._

import scala.collection.JavaConverters._

/**
  * Tests how fast is Distribute Task
  */
object DistributeBench extends Benchmark {

  def main(args: Array[String]): Unit = {

    val dir = tempDir()
    val store = new LSMStore(dir = dir, keySize = 8,
      maxJournalUpdates = Integer.MAX_VALUE, //distribute task will be triggered manually
      maxShardUnmergedCount = 1000,
      executor = null, taskSchedulerDisabled = true
    )

    val v1 = fromLong(1L)
    val v2 = fromLong(2L)
    val v3 = fromLong(3L)

    //add some data to create shards
    for (v <- List(v1, v2)) {
      store.update(versionID = v, toRemove = Nil,
        toUpdate = (0l until 1e6.toLong).map { i => (fromLong(i), v) }
      )
      store.taskDistribute()
      store.taskShardMerge(store.shards.keySet().asScala.head)
    }
    println("Shard count: " + store.shards.size)
    assert(store.shards.size > 1)


    for (i <- 4 until 100) {
      val v = fromLong(i)
      store.update(versionID = v, toRemove = Nil,
        toUpdate = (0l until 1e6.toLong).map { i => (fromLong(i), v) }
      )
      val time = System.currentTimeMillis()
      store.taskDistribute()
      System.out.println(System.currentTimeMillis() - time)

    }
    store.close()
    deleteRecur(dir)
  }

}

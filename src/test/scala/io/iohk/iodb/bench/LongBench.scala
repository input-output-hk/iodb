package io.iohk.iodb.bench

import java.io.File
import java.util.concurrent.TimeUnit

import io.iohk.iodb.{ByteArrayWrapper, QuickStore, Store, TestUtils}
import org.junit.Assert._

import scala.util.Random

/**
  * Long running benchmark to test store scalability under continuous use.
  */
object LongBench {

  case class Config(
                     keyCount: Long = 1e7.toLong,
                     batchSize: Int = 10000,
                     keySize: Int = 32,
                     minValueSize: Int = 32,
                     maxValueSize: Int = 128,
                     randomSeed: Int = new Random().nextInt(),
                     duration: Long = TimeUnit.HOURS.toMillis(1),
                     maxStoreDirSize: Double = 1e9 * 100) {
    assert(keyCount % batchSize == 0)
  }

  def randomPair(r: Random, config: Config): (ByteArrayWrapper, ByteArrayWrapper) = {
    val valueSize = config.minValueSize + r.nextInt(config.maxValueSize - config.minValueSize)
    (TestUtils.randomA(size = config.keySize, random = r), TestUtils.randomA(size = valueSize, random = r))
  }

  def checkDirSize(dir: File, config: Config): Unit = {
    val dirSize = TestUtils.dirSize(dir)
    if (dirSize > config.maxStoreDirSize) {
      throw new Error("Store too big, size is " + (dirSize / 1e9) + " GB")
    }
  }

  def printVal(name: String, value: Long): Unit = {
    printf(name + " %,d \n", value)
  }

  def main(args: Array[String]): Unit = {
    println("max heap " + Runtime.getRuntime.maxMemory() / 1e9.toLong + " GB")
    val dir = TestUtils.tempDir()
    val dirClean = new File(dir, "clean")
    dirClean.mkdirs()

    val storeFab = { f: File => new QuickStore(f) }

    val config = new Config()

    printf("Key Count %,d \n", config.keyCount)

    //fill store with N keysunti l
    var time = System.currentTimeMillis()
    var store: Store = storeFab(dirClean)
    var r = new Random(config.randomSeed)
    for (i <- 0L until config.keyCount by config.batchSize) {
      val toUpdate = (i until i + config.batchSize).map { i => randomPair(r, config) }.toBuffer
      store.update(
        versionID = TestUtils.fromLong(i),
        toRemove = Nil,
        toUpdate = toUpdate
      )
      checkDirSize(dirClean, config)
    }
    store.close()

    time = System.currentTimeMillis() - time
    printVal("Insert", time)
    printVal("Size", TestUtils.dirSize(dirClean))


    // now we have store with data in `dirClean`
    // run readonly benchmark
    store = storeFab(dirClean)

    val readSpeed = readBench(store, config)
    printVal("Read: ", readSpeed)


    val reupdateSpeed = reupdateBench(store, config, dirClean)
    printVal("Reupdate: ", reupdateSpeed)

    TestUtils.deleteRecur(dir)

  }

  def readBench(store: Store, config: Config): Long = {
    val startTime = System.currentTimeMillis()
    var counter = 0L
    while (true) {
      val r = new Random(config.randomSeed)

      for (i <- 0L until config.keyCount) {
        counter += 1

        val curTime = System.currentTimeMillis()
        if (curTime > startTime + config.duration)
          return 1000 * counter / (curTime - startTime)

        val (key, value) = randomPair(r, config)
        assertEquals(Some(value), store.get(key))
      }
    }
    return -1L
  }


  def reupdateBench(store: Store, config: Config, dir: File): Long = {
    val startTime = System.currentTimeMillis()
    var counter = 0L
    while (true) {
      var r = new Random(config.randomSeed)
      for (i <- 0L until config.keyCount by config.batchSize) {
        val toUpdate = (i until i + config.batchSize).map { i => randomPair(r, config) }.toBuffer
        store.update(
          versionID = TestUtils.fromLong(i),
          toRemove = Nil,
          toUpdate = toUpdate
        )
        checkDirSize(dir, config)
        val curTime = System.currentTimeMillis()

        counter += 1
        if (curTime > startTime + config.duration)
          return 1000 * counter / (curTime - startTime)

      }

    }
    return -1L
  }


  def updateBench(store: Store, config: Config, dir: File): Long = {
    val startTime = System.currentTimeMillis()
    var counter = 0L
    while (true) {
      var r = new Random(config.randomSeed + 1)
      for (i <- 0L until config.keyCount by config.batchSize) {
        val toUpdate = (i until i + config.batchSize).map { i => randomPair(r, config) }.toBuffer
        store.update(
          versionID = TestUtils.fromLong(i),
          toRemove = Nil,
          toUpdate = toUpdate
        )
        checkDirSize(dir, config)
        val curTime = System.currentTimeMillis()

        counter += 1
        if (curTime > startTime + config.duration)
          return 1000 * counter / (curTime - startTime)

      }

    }
    return -1L
  }
}

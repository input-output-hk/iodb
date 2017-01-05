package io.iohk.iodb.bench

import io.iohk.iodb.{FileAccess, LogStore, TestUtils}

/**
  * Performance of different file access methods (memory mapped file versus file channel)
  */
object FileAccessBench extends Benchmark {

  def main(args: Array[String]): Unit = {
    val methods = List(FileAccess.MMAP, FileAccess.UNSAFE, FileAccess.FILE_CHANNEL, FileAccess.SAFE)

    for (method <- methods) {
      val keyVal = (1 until 10000000).map(TestUtils.fromLong(_)).map(i => (i, i))
      val dir = TestUtils.tempDir()
      val log = new LogStore(dir = dir, filePrefix = "test", keySize = 8)(fileAccess = method)
      log.update(version = 1, versionID = TestUtils.fromLong(1), toRemove = Nil, toUpdate = keyVal)

      val time = System.currentTimeMillis()

      for ((key, value) <- keyVal) {
        log.get(key, versionId = 1)
      }

      val time2 = System.currentTimeMillis() - time
      println(method.getClass.getSimpleName + " - " + time2 / 1000)

      TestUtils.deleteRecur(dir)
    }
  }
}

package io.iohk.iodb.bench

import io.iohk.iodb.{FileAccess, LSMStore, TestUtils}

/**
  * Finds optimal performance parameters by trying various settings
  */
object ParamSearch {


  val updates = 1000
  val keyCount = 1000
  val keySize = 32


  val fileAccess2 = List(FileAccess.SAFE, FileAccess.UNSAFE)

  val maxJournalEntryCount2 = List(1e4, 1e5, 1e6).map(_.toInt)

  val maxShardUnmergedCount2 = List(1, 3, 6)

  val splitSize2 = List(1024 * 100, 1024 * 1000, 1024 * 12000, 1024 * 1024 * 64)

  val maxFileSize2 = splitSize2.map(_ * 10)


  def main(args: Array[String]): Unit = {

    for (fileAccess <- fileAccess2;
         maxJournalEntryCount <- maxJournalEntryCount2;
         maxShardUnmergedCount <- maxShardUnmergedCount2;
         splitSize <- splitSize2;
         maxFileSize <- maxFileSize2
    ) try {
      val dir = TestUtils.tempDir()
      val store = new LSMStore(
        dir = dir,
        keySize = 32,
        maxJournalEntryCount = maxJournalEntryCount,
        maxShardUnmergedCount = maxShardUnmergedCount,
        splitSize = splitSize,
        maxFileSize = maxFileSize
      )

      val result = SimpleKVBench.bench(store = store, dir = dir, updates = updates, keyCount = keyCount)
      store.close()
      TestUtils.deleteRecur(dir)

      println("================")
      println("maxJournalEntryCount: " + maxJournalEntryCount)
      println("maxShardUnmergedCount: " + maxShardUnmergedCount)
      println("splitSize: " + splitSize)
      println("maxFileSize: " + maxFileSize)
      println("size: " + result.storeSizeMb)
      println("insert time: " + result.insertTime)
      println("get time: " + result.getTime)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }

}

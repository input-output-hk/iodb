package io.iohk.iodb.unit

import java.io.File

import io.iohk.iodb.TestUtils.fromLong
import io.iohk.iodb._
import org.junit.Test

import scala.collection.JavaConverters._


class LSMStoreTest extends TestWithTempDir {

  val v1 = TestUtils.fromLong(1L)
  val v2 = TestUtils.fromLong(2L)
  val v3 = TestUtils.fromLong(3L)

  @Test def testShard(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8, splitSize = 1024)
    val toUpdate = (1L to 100000L).map(fromLong).map(k => (k, k))

    store.update(versionID = v1, toRemove = Nil, toUpdate = toUpdate)
    store.taskShardLogForce()
    store.taskShardMerge()

    assert(store.getShards.size() > 1)
    store.close()
  }

  @Test def rollback(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8)
    val key = fromLong(100)

    store.update(v1, Nil, (key, fromLong(1)) :: Nil)
    store.update(v2, Nil, (key, fromLong(2)) :: Nil)
    store.update(v3, Nil, (key, fromLong(3)) :: Nil)
    assert(store.mainLog.files.size == 3)
    val lastFiles = store.mainLog.files.firstEntry().getValue

    store.rollback(v2)

    assert(store.mainLog.lastVersionID.get == v2)
    assert(store.shards.firstKey() <= 2)

    assert(store.mainLog.files.firstKey() == 2L)

    assert(!lastFiles.logFile.exists())

    assert(store.mainLog.files.size == 2)
    assert(store.get(key) == Some(fromLong(2)))
    store.close()
  }


  @Test def rollback_shard_merge(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8, minMergeCount = 1, shardEveryVersions = 1)
    val key = fromLong(100)

    store.update(v1, Nil, (key, fromLong(1)) :: Nil)
    store.update(v2, Nil, (key, fromLong(2)) :: Nil)
    store.update(v3, Nil, (key, fromLong(3)) :: Nil)
    //force shard redistribution
    store.taskShardLogForce()

    assert(store.mainLog.files.size == 3)
    assert(store.mainLog.lastVersionID.get == v3)
    assert(store.lastShardedLogVersion == 3)

    val lastFiles = store.mainLog.files.firstEntry().getValue

    store.rollback(v2)

    assert(store.mainLog.lastVersionID.get == v2)
    assert(store.shards.firstKey() <= 2)

    assert(store.mainLog.files.firstKey() == 2L)

    assert(!lastFiles.logFile.exists())

    assert(store.mainLog.files.size == 2)
    assert(store(key) == fromLong(2))

    store.close()
  }

  def allShardFiles(store: LSMStore): Set[File] =
    store.getShards.asScala.values
      .flatMap(_.files.values().asScala)
      .map(f => f.logFile)
      .toSet

  @Test def rollback_shard_split(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8,
      minMergeCount = 1,
      shardEveryVersions = 1,
      splitSize = 1024)
    val key = fromLong(100)

    store.update(v1, Nil, (key, fromLong(1)) :: Nil)
    store.update(v2, Nil, (key, fromLong(2)) :: Nil)

    //all files associated with shards at given version
    val shardFiles2 = allShardFiles(store)

    //add enough items to trigger shard split
    store.update(v3, Nil,
      (1000 to 9000).map(i => (fromLong(i), fromLong(i)))
    )

    //move data from main log to shards
    store.taskShardLogForce()
    //split large shard into smaller shards
    store.taskShardMerge()

    val shardFiles3 = allShardFiles(store)

    assert(store.mainLog.files.size == 3)
    assert(store.mainLog.lastVersionID.get == v3)
    assert(store.lastShardedLogVersion == 3)
    assert(store.shards.size == 2)
    assert(store.shards.lastKey() == 3)
    assert(store.shards.lastEntry().getValue.size() > 1)

    val lastFiles = store.mainLog.files.firstEntry().getValue

    store.rollback(v2)

    assert(store.mainLog.lastVersionID.get == v2)
    assert(store.shards.lastKey() <= 2)

    assert(store.mainLog.files.firstKey() == 2L)

    assert(!lastFiles.logFile.exists())

    assert(store.mainLog.files.size == 2)
    assert(store(key) == fromLong(2))

    //ensure shard layout was restored
    assert(store.shards.size() == 1)

    // /check no old files were deleted
    shardFiles2.foreach { f =>
      assert(f.exists())
    }
    //ensure all shard files were deleted
    shardFiles3.foreach { f =>
      assert(f.exists() == shardFiles2.contains(f))
    }

    store.close()
  }

  @Test def reopen() {
    var store = new LSMStore(dir = dir, keySize = 8,
      minMergeCount = 1,
      minMergeSize = 1024,
      shardEveryVersions = 1,
      splitSize = 1024
    )

    val commitCount = 100
    val keyCount = 1000

    for (ver <- 1 until commitCount) {
      val toUpdate = (0 until keyCount).map(i => (fromLong(i), fromLong(ver * i)))
      store.update(versionID = TestUtils.fromLong(ver), toRemove = Nil, toUpdate = toUpdate)
    }

    store.close()
    val oldShards = store.shards
    val oldLastShardedLogVersion = store.lastShardedLogVersion

    store = new LSMStore(dir = dir, keySize = 8,
      minMergeCount = 1,
      shardEveryVersions = 1,
      splitSize = 1024)

    for (i <- 1 until keyCount) {
      val value = store(fromLong(i))
      assert(value == fromLong((commitCount - 1) * i))
    }
    store.close()
    assert(oldLastShardedLogVersion == store.lastShardedLogVersion)
    assert(store.shards == oldShards)
  }

  @Test def loadSaveShardInfo(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8)

    def test(s: ShardInfo): Unit = {
      val f = new File(dir, Math.random().toString)
      s.save(f)
      val s2 = store.loadShardInfo(f)
      assert(s == s2)
      f.delete()
    }

    test(new ShardInfo(startKey = fromLong(11), endKey = fromLong(22),
      startVersionID = TestUtils.fromLong(111L), startVersion = 111L, keySize = 8))
    test(new ShardInfo(startKey = fromLong(11), endKey = null,
      startVersionID = TestUtils.fromLong(111L), startVersion = 111L, keySize = 8))
    store.close()
  }

  @Test def shardInfoCreated(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8,
      splitSize = 1024,
      minMergeSize = 1024,
      minMergeCount = 1,
      shardEveryVersions = 1)

    def files = TestUtils.listFiles(dir, TestUtils.shardInfoFileExt)
    assert(files.size == 1)

    val initShardInfoFile = files(0)
    assert(store.loadShardInfo(initShardInfoFile) ==
      new ShardInfo(startKey = new Store.K(8), endKey = null, startVersionID = new ByteArrayWrapper(0), startVersion = 0L, keySize = 8))


    //fill with data, force split
    val toUpdate = (1 until 10000).map(k => (fromLong(k), fromLong(k)))
    for (version <- (1 until 100)) {
      store.update(versionID = TestUtils.fromLong(version), toRemove = Nil, toUpdate = toUpdate)
    }
    store.taskShardLogForce()
    store.taskShardMerge()
    assert(files.size > 1)
    assert(files.contains(initShardInfoFile))

    assert(store.getShards.size() > 1)

    val newInfos = files
      .filter(_ != initShardInfoFile)
      .map(store.loadShardInfo(_))
      .toSeq
      .sortBy(_.startKey)

    assert(newInfos.size == newInfos.toSet.size)

    newInfos.foldRight[Store.K](null) { (info: ShardInfo, prevEndkey: Store.K) =>
      assert(info.endKey == prevEndkey)
      assert(info.isLastShard == (prevEndkey == null))
      assert(prevEndkey == null || info.startKey.compareTo(info.endKey) < 0)

      info.startKey
    }

    assert(newInfos(0).startKey == new Store.K(8))
    store.close()
  }

  @Test def getVersionIDEmpty(): Unit = {
    val store = new LSMStore(dir = dir)
    assert(None == store.lastVersionID)
  }

}
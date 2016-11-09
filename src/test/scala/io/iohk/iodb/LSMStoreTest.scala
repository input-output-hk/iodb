package io.iohk.iodb

import java.io.File

import io.iohk.iodb.TestUtils.fromLong
import org.junit.Test

import scala.collection.JavaConverters._


class LSMStoreTest extends TestWithTempDir {

  @Test def testShard(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8, splitSize = 1024)
    val toUpdate = (1L to 100000L).map(fromLong).map(k => (k, k))

    store.update(versionID = 1L, toRemove = Nil, toUpdate = toUpdate)
    store.taskShardLogForce()
    store.taskShardMerge()

    assert(store.getShards.size() > 1)
  }

  @Test def rollback(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8)
    val key = fromLong(100)

    store.update(1L, Nil, (key, fromLong(1)) :: Nil)
    store.update(2L, Nil, (key, fromLong(2)) :: Nil)
    store.update(3L, Nil, (key, fromLong(3)) :: Nil)
    assert(store.mainLog.files.size == 3)
    val lastFiles = store.mainLog.files.firstEntry().getValue

    store.rollback(2L)

    assert(store.mainLog.lastVersion == 2)
    assert(store.shards.firstKey() <= 2)

    assert(store.mainLog.files.firstKey() == 2L)

    assert(!lastFiles.keyFile.exists())
    assert(!lastFiles.valueFile.exists())

    assert(store.mainLog.files.size == 2)
    assert(store.get(key) == fromLong(2))
  }


  @Test def rollback_shard_merge(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8, minMergeCount = 1, shardEveryVersions = 1)
    val key = fromLong(100)

    store.update(1L, Nil, (key, fromLong(1)) :: Nil)
    store.update(2L, Nil, (key, fromLong(2)) :: Nil)
    store.update(3L, Nil, (key, fromLong(3)) :: Nil)
    //force shard redistribution
    store.taskShardLogForce()

    assert(store.mainLog.files.size == 3)
    assert(store.mainLog.lastVersion == 3)
    assert(store.lastShardedLogVersion == 3)

    val lastFiles = store.mainLog.files.firstEntry().getValue

    store.rollback(2L)

    assert(store.mainLog.lastVersion == 2)
    assert(store.shards.firstKey() <= 2)

    assert(store.mainLog.files.firstKey() == 2L)

    assert(!lastFiles.keyFile.exists())
    assert(!lastFiles.valueFile.exists())

    assert(store.mainLog.files.size == 2)
    assert(store.get(key) == fromLong(2))
  }

  def allShardFiles(store: LSMStore): Set[File] =
    store.getShards.asScala.values
      .flatMap(_.files.values().asScala)
      .flatMap(f => List(f.keyFile, f.valueFile))
      .toSet

  @Test def rollback_shard_split(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8,
      minMergeCount = 1,
      shardEveryVersions = 1,
      splitSize = 1024)
    val key = fromLong(100)

    store.update(1L, Nil, (key, fromLong(1)) :: Nil)
    store.update(2L, Nil, (key, fromLong(2)) :: Nil)

    //all files associated with shards at given version
    val shardFiles2 = allShardFiles(store)

    //add enough items to trigger shard split
    store.update(3L, Nil,
      (1000 to 9000).map(i => (fromLong(i), fromLong(i)))
    )

    //move data from main log to shards
    store.taskShardLogForce()
    //split large shard into smaller shards
    store.taskShardMerge()

    val shardFiles3 = allShardFiles(store)

    assert(store.mainLog.files.size == 3)
    assert(store.mainLog.lastVersion == 3)
    assert(store.lastShardedLogVersion == 3)
    assert(store.shards.size == 2)
    assert(store.shards.lastKey() == 3)
    assert(store.shards.lastEntry().getValue.size() > 1)

    val lastFiles = store.mainLog.files.firstEntry().getValue

    store.rollback(2L)

    assert(store.mainLog.lastVersion == 2)
    assert(store.shards.lastKey() <= 2)

    assert(store.mainLog.files.firstKey() == 2L)

    assert(!lastFiles.keyFile.exists())
    assert(!lastFiles.valueFile.exists())

    assert(store.mainLog.files.size == 2)
    assert(store.get(key) == fromLong(2))

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
  }

}
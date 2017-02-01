package io.iohk.iodb

import java.io.{File, FileOutputStream, RandomAccessFile}

import io.iohk.iodb.Store._
import io.iohk.iodb.TestUtils._
import org.junit.Test

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

class LSMStoreTest extends TestWithTempDir {

  val v1 = TestUtils.fromLong(1L)
  val v2 = TestUtils.fromLong(2L)
  val v3 = TestUtils.fromLong(3L)

  @Test def testShard(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8, splitSize = 1024, executor = null)
    val toUpdate = (1L to 10000L).map(fromLong).map(k => (k, k))

    for (i <- 0 until 5) {
      store.update(versionID = fromLong(i), toRemove = Nil, toUpdate = toUpdate)
      store.taskSharding()
    }

    //journal should only single file, once sharding is completed
    assert(store.fileHandles.keys.filter(_ < 0).size == 1)
    assert(store.fileOuts.keys.filter(_ < 0).size == 1)
    assert(store.journalCache.isEmpty)

    //check shard was created
    store.taskShardMerge(store.shards.firstKey())
    assert(store.shards.size() > 1)

    store.close()
  }

  @Test def keyValues(): Unit = {
    //fill update file with values, check merged content
    val store = new LSMStore(dir = dir, keySize = 8, splitSize = 1024, executor = null)

    val fileNum = store.createEmptyShard()
    var log: List[LogFileUpdate] = Nil
    val ref = new mutable.HashMap[K, V]()
    val r = new Random()

    val limit = 10 + 100 * TestUtils.longTest()
    for (i <- 0L until limit) {
      //keys to delete
      val toRemove = ref.keySet.take(10)
      toRemove.foreach(ref.remove(_))

      //keys to insert
      val keyvals = (0 until 1000)
        .map(a => (fromLong(r.nextLong()), fromLong(r.nextLong())))
      keyvals.foreach(a => ref.put(a._1, a._2))

      //write data
      val update = store.updateAppend(fileNum = fileNum,
        toRemove = toRemove, toUpdate = keyvals,
        versionID = tombstone, prevVersionID = tombstone,
        merged = i == 0,
        shardStartKey = null, shardEndKey = null)

      log = update :: log

      //calculate merged content, check everything is there
      val merged: Seq[(K, V)] = store.keyValues(log).toBuffer

      assert(merged.size == ref.size)
      merged.foreach { p =>
        assert(p._2 == ref(p._1))
      }
    }
  }

  @Test def shard_merge_to_next_file(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8, splitSize = 1024 * 1024 * 1024, executor = null)

    def check(): Unit = {
      //read file, check the start/end keys are preserved
      val num = store.shards.lastEntry().getValue.head.fileNum
      var updates = store.loadUpdates(store.numToFile(num), num)
      assert(updates.head.shardStartKey == fromLong(0))
      assert(updates.head.shardEndKey == null)
    }

    for (i <- 0L until 100) {
      val keyvals = (0L until 1000)
        .map(a => (fromLong(a), fromLong(a)))
      store.update(
        versionID = fromLong(i),
        toRemove = Nil,
        toUpdate = keyvals)
      store.taskSharding()
      check()
    }

    assert(store.shards.size() >= 1)
    store.taskShardMerge(store.shards.firstKey())
    assert(store.shards.lastEntry().getValue.head.fileNum >= 2)

    check()
  }

  @Test def split_shard_correct_start_keys(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8, splitSize = 100, executor = null)

    for (i <- 0 until 10) {
      store.update(
        versionID = fromLong(i),
        toRemove = Nil,
        toUpdate = (1 + i until 10000 - i).map(fromLong(_)).map(a => (a, a))
      )
      store.taskSharding()
      store.verify()
    }
    store.taskShardMerge(shardKey = fromLong(0))
    assert(store.shards.size > 1)

    store.verify()
  }


  @Test def reopen(): Unit = {
    //fill update file with values, check merged content
    val store = new LSMStore(dir = dir, keySize = 8, splitSize = 10240, executor = null)

    val fileNum = store.createEmptyShard()
    var log: List[LogFileUpdate] = Nil
    val ref = new mutable.HashMap[K, V]()
    val r = new Random()

    val limit = 10 + 100 * TestUtils.longTest()
    for (i <- 0L until limit) {
      //keys to delete
      val toRemove = ref.keySet.take(10)
      toRemove.foreach(ref.remove(_))

      //keys to insert
      val keyvals = (0 until 1000)
        .map(a => (fromLong(r.nextLong()), fromLong(r.nextLong())))
      keyvals.foreach(a => ref.put(a._1, a._2))

      store.update(versionID = fromLong(i), toRemove = toRemove, toUpdate = keyvals)
    }
    store.taskSharding()
    store.shards.keySet().asScala.foreach { key => store.taskShardMerge(key) }

    //open secondary store, compare its content
    val store2 = new LSMStore(dir = dir, keySize = 8, splitSize = 10240, executor = null)

    storeEquals(store, store2)
  }

  def reopen_branched(keepVersions: Int): Unit = {
    def openStore = new LSMStore(dir = dir, keySize = 8, splitSize = 1024, executor = null, keepVersions = keepVersions)

    val store = openStore
    val r = new Random()
    val limit = 200 + TestUtils.longTest() * 10000
    for (i <- 0L until limit) {
      val update = (0 until 100).map(a => (fromLong(r.nextLong()), fromLong(r.nextLong())))
      store.update(versionID = fromLong(i), toUpdate = update, toRemove = Nil)
      store.verify()
      storeEquals(store, openStore)
      store.taskSharding()
      store.verify()
      storeEquals(store, openStore)
    }
    for (shardKey <- store.shards.keySet().asScala.toSeq) {
      store.taskShardMerge(shardKey)
      store.verify()
      storeEquals(store, openStore)
    }
    store.close()
  }

  @Test def reopen_branched_shards(): Unit = {
    reopen_branched(0)
  }

  @Test def reopen_branched_shards2(): Unit = {
    reopen_branched(10)
  }

  private def storeEquals(store: LSMStore, store2: LSMStore) = {
    assert(store.journalDirty == store2.journalDirty)
    assert(store.journalRollback == store2.journalRollback)
    assert(store.journalLastVersionID == store2.journalLastVersionID)
    assert(store.journalCache == store2.journalCache)
    assert(store.fileHandles == store2.fileHandles)
    assert(store.fileOuts.keySet == store2.fileOuts.keySet)
    assert(store.shards == store2.shards)
    store2.close()
  }

  @Test def get_sorted_journal(): Unit = {
    val store = new LSMStore(dir = dir)
    //create all files
    Random.shuffle((-1000 until -1).toList).foreach { n =>
      new RandomAccessFile(store.numToFile(n), "rw")
    }

    def numbers = store.journalListSortedFiles().map(store.journalFileToNum(_))

    assert(numbers == (-1000L until -1))

    //delete all journal files but last one
    store.journalDeleteFilesButNewest()
    assert(numbers == List(-1000L))
  }

  @Test def find_shard_heads(): Unit = {
    val s = new LSMStore(dir = dir, keySize = 8)

    def u(l1: Long, l2: Long, from: K = fromLong(0), to: K = null) = new LogFileUpdate(
      versionID = fromLong(l1), prevVersionID = fromLong(l2),
      merged = false, fileNum = 0, keyCount = 0,
      offset = 0,
      shardStartKey = from, shardEndKey = to
    )

    //no links, should return entire list
    var l = List(u(1, 2), u(3, 4), u(5, 6))
    assert(l == s.shardFindHeads(l))

    //cyclic reference
    assert(Nil == s.shardFindHeads(List(u(1, 2), u(2, 1))))

    //simple list
    assert(List(u(1, 2)) == s.shardFindHeads(List(u(1, 2), u(2, 3))))

    assert(List(u(3, 2), u(4, 2)) == s.shardFindHeads(List(u(2, 1), u(3, 2), u(4, 2))))

    //test expand tails
    //no links, should return entire list of list
    assert(l.map(List(_)) == s.shardExpandHeads(l))

    //simple list
    l = List(u(1, 2), u(2, 3))
    assert(List(l) == s.shardExpandHeads(l))


    assert(List(List(u(3, 2), u(2, 1)), List(u(4, 2), u(2, 1)))
      == s.shardExpandHeads(List(u(2, 1), u(3, 2), u(4, 2))))

  }

  @Test def open_shard_File(): Unit = {
    val s = new LSMStore(dir = dir, keySize = 8)

    val start = fromLong(100)
    val end = fromLong(200)

    val f = s.numToFile(1)
    val out = new FileOutputStream(f)
    //first record in shard is not linked, it contains shardStartKey and shardEndKey
    out.write(
      s.createUpdateData(
        versionID = tombstone, prevVersionID = tombstone,
        toRemove = List(start, end),
        toUpdate = Nil, isMerged = false)
    )
    val offset1 = out.getChannel.position()
    out.write(
      s.createUpdateData(
        versionID = fromLong(100), prevVersionID = tombstone,
        toRemove = Nil,
        toUpdate = List((fromLong(150), fromLong(150))),
        isMerged = true)
    )
    val offset2 = out.getChannel.position()
    out.write(
      s.createUpdateData(
        versionID = fromLong(101), prevVersionID = fromLong(100),
        toRemove = Nil,
        toUpdate = List((fromLong(151), fromLong(151))),
        isMerged = false)
    )
    out.flush()
    out.close()

    //load shard from created file
    val updates = s.loadUpdates(f, fileNum = 1)

    assert(updates ==
      List(
        new LogFileUpdate(
          versionID = fromLong(100), prevVersionID = tombstone,
          shardStartKey = start, shardEndKey = end,
          merged = true,
          offset = offset1.toInt,
          fileNum = 1L,
          keyCount = 1
        ),

        new LogFileUpdate(
          versionID = fromLong(101), prevVersionID = fromLong(100),
          shardStartKey = start, shardEndKey = end,
          merged = false,
          offset = offset2.toInt,
          fileNum = 1L,
          keyCount = 1
        )
      )
    )
  }

  @Test def keep_journal_for_rollback(): Unit = {
    val s = new LSMStore(dir = dir, keySize = 8, keepVersions = 10, executor = null)

    for (i <- 0 until 100) {
      s.update(versionID = fromLong(i), toRemove = Nil, toUpdate = List((fromLong(i), fromLong(i))))
      if (i % 31 == 0) {
        s.taskSharding()
      }
      assert(i < 10 || s.journalRollback.size >= 10)
      if (i > 80)
        assert(s.journalRollback.size < 70)
    }

    s.close()
  }


  @Test def rollback(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8, keepVersions = 10)
    val key = fromLong(100)

    store.update(v1, Nil, (key, fromLong(1)) :: Nil)
    store.update(v2, Nil, (key, fromLong(2)) :: Nil)
    store.update(v3, Nil, (key, fromLong(3)) :: Nil)
    assert(store.fileHandles.size == 1)
    assert(store.journalDirty.size == 3)

    store.rollback(v2)
    assert(store.journalDirty.head.versionID == v2)
    assert(store.get(key) == Some(fromLong(2)))

    store.close()
  }

  @Test def rollback_shard_merge(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8, splitSize = 1, keepVersions = 100)
    val key = fromLong(100)

    store.update(v1, Nil, (key, fromLong(1)) :: Nil)
    store.update(v2, Nil, (key, fromLong(2)) :: Nil)
    store.update(v3, Nil, (key, fromLong(3)) :: Nil)
    //force shard redistribution
    store.taskSharding()
    store.shards.keySet().asScala.foreach(store.taskShardMerge(_))

    assert(store.lastVersionID.get == v3)
    //assert(store.lastShardedLogVersion == 3)

    val shardFiles = store.fileHandles.keySet.filter(_ >= 0).toBuffer

    store.rollback(v2)

    assert(store.lastVersionID.get == v2)
    assert(store.shards.isEmpty)

    assert(shardFiles.forall(fileNum => store.numToFile(fileNum).exists()))

    assert(store.fileHandles.keySet.filter(_ >= 0) == Set(1L))
    assert(store.fileOuts.keySet.filter(_ >= 0) == Set(1L))

    assert(store.journalDirty.size == 2)
    assert(store(key) == fromLong(2))

    store.close()
  }

  def allShardFiles(store: LSMStore): Iterable[File] =
    store.fileHandles.keys.filter(_ >= 0).map(store.numToFile(_))


  @Test def getVersionIDEmpty(): Unit = {
    val store = new LSMStore(dir = dir)
    assert(None == store.lastVersionID)
  }


  @Test def max_file_size(): Unit = {
    val keySize = 1000
    val maxFileSize = 1024 * 1024
    val s = new LSMStore(dir = dir, maxFileSize = maxFileSize, keySize = 1000, executor = null)

    for (i <- 0 until 100000) {
      s.update(
        versionID = fromLong(i),
        toUpdate = List(Pair(randomA(keySize), randomA(keySize * 4))),
        toRemove = Nil)

      dir.listFiles().foreach { f =>
        assert(f.length() <= maxFileSize * 2, f.getName)
      }
    }
    s.close()
  }


}
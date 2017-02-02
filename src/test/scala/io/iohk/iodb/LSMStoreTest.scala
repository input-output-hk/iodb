package io.iohk.iodb

import java.io._

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
    store.taskCleanup()
    //journal should only be single file, once sharding is completed
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
        merged = i == 0)

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
    //    assert(store.fileHandles.keySet == store2.fileHandles.keySet)
    //    assert(store.fileOuts.keySet == store2.fileOuts.keySet)
    //    assert(store.shards == store2.shards)
    assert(store.shardRollback == store2.shardRollback)
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


  @Test def open_shard_File(): Unit = {
    val s = new LSMStore(dir = dir, keySize = 8)

    val start = fromLong(100)
    val end = fromLong(200)

    val f = s.numToFile(1)
    val out = new FileOutputStream(f)
    out.write(
      s.serializeUpdate(
        versionID = fromLong(100), prevVersionID = tombstone,
        toRemove = Nil,
        toUpdate = List((fromLong(150), fromLong(150))),
        isMerged = true)
    )
    val offset2 = out.getChannel.position()
    out.write(
      s.serializeUpdate(
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
          merged = true,
          offset = 0,
          fileNum = 1L,
          keyCount = 1
        ),

        new LogFileUpdate(
          versionID = fromLong(101), prevVersionID = fromLong(100),
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
        s.taskCleanup()
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

    for (i <- 0 until 10000) {
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


  @Test def file_cut1(): Unit = {
    //open with small file size, that puts each update into separate file
    def open() = new LSMStore(dir = dir, maxFileSize = 1, keySize = 8, executor = null,
      keepVersions = 10)

    val s = open()

    for (i <- 1 until 100) {
      val k = fromLong(i)
      s.update(k, toRemove = Nil, toUpdate = List((k, k)))
      s.taskSharding()
      s.verify()
      storeEquals(s, open())
      s.taskCleanup()
      s.verify()
      storeEquals(s, open())

      assert(s.fileHandles.keySet.filter(_ < 0).size < 14)
    }
  }

  @Test def ser_shard_spec: Unit = {
    val spec0 = new ShardSpecEntry(fileNum = 1L, startKey = fromLong(0), endKey = fromLong(100), versionID = fromLong(111))
    val spec1 = new ShardSpecEntry(fileNum = 2L, startKey = fromLong(100), endKey = fromLong(200), versionID = fromLong(222))
    val spec2 = new ShardSpecEntry(fileNum = 3L, startKey = fromLong(200), endKey = null, versionID = fromLong(333))

    val s = List(spec0, spec1, spec2)
    val e = new LogFileUpdate(offset = 0, keyCount = 0, merged = false, fileNum = 1L,
      versionID = tombstone, prevVersionID = tombstone)

    val t = List((spec0.startKey, spec0.fileNum, spec0.versionID), (spec1.startKey, spec1.fileNum, spec1.versionID), (spec2.startKey, spec2.fileNum, spec2.versionID))

    val store = new LSMStore(dir = dir, keySize = 8)
    val b = store.serializeShardSpec(versionID = fromLong(111), shards = t)

    val in = new DataInputStream(new ByteArrayInputStream(b))
    val s2 = store.deserializeShardSpec(in)
    assert(in.read() == -1)
    assert(s2 == new ShardSpec(versionID = fromLong(111L), s))
  }

  @Test def shard_spec(): Unit = {
    val store = new LSMStore(dir = dir, keySize = 8, splitSize = 1, keepVersions = 100)
    store.update(versionID = fromLong(1L), toRemove = Nil, toUpdate = List((fromLong(1), fromLong(1))))
    store.taskSharding()
    assert(store.shards.size == 1)
    val spec = store.deserializeShardSpec(
      new DataInputStream(new FileInputStream(new File(store.dir, LSMStore.shardLayoutLog))))
    val e = new ShardSpecEntry(startKey = fromLong(0L), endKey = null, fileNum = 1L, versionID = fromLong(1L))
    assert(spec == ShardSpec(versionID = fromLong(1L), List(e)))

    for (i <- 2 until 100) {
      store.update(versionID = fromLong(i), toRemove = Nil, toUpdate = List((fromLong(i), fromLong(1))))
      store.taskSharding()
      store.taskShardMerge(shardKey = fromLong(0))

    }
    assert(store.shards.size > 1)

    val fileNum = store.shards.lastEntry().getValue.head.fileNum
    val in = new DataInputStream(new FileInputStream(new File(store.dir, LSMStore.shardLayoutLog)));
    var spec2: ShardSpec = null
    try {
      //get last spec in stream, it will fail with an exception
      while (true)
        spec2 = store.deserializeShardSpec(in)
    } catch {
      case _ =>
    }
    assert(spec2.shards.size == store.shardRollback.last._2.size)
  }

}
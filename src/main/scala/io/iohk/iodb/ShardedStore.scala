package io.iohk.iodb

import java.io.File

import io.iohk.iodb.Store.{K, V, VersionID}

class ShardedStore(val dir: File, val keySize: Int, val shardCount: Int = 1)
  extends Store {

  val journal = new LogStore(keySize = keySize, dir = dir, filePrefix = "journal")

  val shard1 = new LogStore(keySize = keySize, dir = dir, filePrefix = "shard1")

  val shards = new java.util.TreeMap[K, LogStore]()
  shards.put(new K(new Array[Byte](keySize)), shard1)


  override def get(key: K): Option[V] = {
    val v = journal.getDistribute(key)
    if (v._1.isDefined)
      return v._1 //value was found in journal
    if (v._2.isEmpty)
      return None //map of shards was not found

    val shardsFromDist = v._2.get
    //FIXME assumes single shard
    val shardPos = shardsFromDist.values.last
    val shard = shards.firstEntry().getValue

    //FIXME filePos not used
    return shard.get(key)
  }

  override def getAll(consumer: (K, V) => Unit): Unit = {
    journal.getAll(consumer)
  }

  override def clean(count: Int): Unit = {
    //TODO
  }

  override def lastVersionID: Option[VersionID] = {
    journal.lastVersionID
  }

  override def update(versionID: VersionID, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    journal.update(versionID = versionID, toRemove = toRemove, toUpdate = toUpdate)
  }

  override def rollback(versionID: VersionID): Unit = {
    journal.rollback(versionID)
  }

  override def close(): Unit = {
    journal.close()
  }

  override def rollbackVersions(): Iterable[VersionID] = {
    journal.rollbackVersions()
  }

  override def verify(): Unit = {
    journal.verify()
  }

  def distribute(): Unit = {
    val offsets = journal.loadUpdateOffsets(stopAtMerge = true)
    if (offsets.isEmpty)
      return
    //lock all files for reading
    val files = offsets.map(o => (o.fileNum, journal.fileLock(o.fileNum))).toMap
    try {

      val dataset = journal.loadKeyValues(offsets, files, dropTombstones = false).toIterable
      val versionID = journal.loadVersionID(offsets.last)

      val shard1Offset = shard1.updateDistribute(versionID = versionID, data = dataset, triggerCompaction = false)

      val journalOffset = offsets.head
      // FIXME single shard
      // insert distribute entry into journal
      journal.appendDistributeEntry(journalPos = journalOffset, shards = Map(shards.firstKey() -> shard1Offset))
    } finally {
      for ((fileNum, fileHandle) <- files) {
        if (fileHandle != null)
          journal.fileUnlock(fileNum)
      }
    }
  }
}

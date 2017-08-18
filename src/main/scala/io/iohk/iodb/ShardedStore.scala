package io.iohk.iodb

import java.io.{File, PrintStream, RandomAccessFile}

import com.google.common.base.Strings
import com.google.common.io.Closeables
import io.iohk.iodb.Store.{K, V, VersionID}

class ShardedStore(
                    val dir: File,
                    val keySize: Int = 32,
                    val shardCount: Int = 1)
  extends Store {

  val journal = new LogStore(keySize = keySize, dir = dir, filePrefix = "journal")

  val shard1 = new LogStore(keySize = keySize, dir = dir, filePrefix = "shard1")

  val shards = new java.util.TreeMap[K, LogStore]()
  shards.put(new K(new Array[Byte](keySize)), shard1)


  override def get(key: K): Option[V] = {
    assert(key.size > 7)
    val v = journal.getDistribute(key)
    if (v._1.isDefined)
      return v._1 //value was found in journal
    if (v._2.isEmpty)
      return None //map of shards was not found

    val shardsFromDist = v._2.get
    //FIXME assumes single shard
    val shardPos = shardsFromDist.values.last
    val shard = shards.firstEntry().getValue

    return shard.get(key = key, pos = shardPos)
  }

  override def getAll(consumer: (K, V) => Unit): Unit = {
    journal.getAll(consumer)
  }

  override def clean(count: Int): Unit = {
    //TODO cleanup
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
    val pos: FilePos = journal.appendDistrubutePlaceholder()

    val offsets = journal.loadUpdateOffsets(stopAtMerge = true, stopAtDistribute = true, startPos = pos)
    if (offsets.isEmpty)
      return
    //lock all files for reading
    val files = offsets.map(o => (o.pos.fileNum, journal.fileLock(o.pos.fileNum))).toMap
    try {

      val dataset = journal.loadKeyValues(offsets.filter(_.entryType == LogStore.headUpdate), files, dropTombstones = false).toIterable
      if (dataset.isEmpty)
        return

      //        val versionID = journal.loadVersionID(offsets.last.pos)

      val shard1Offset = shard1.updateDistribute(versionID = new K(0), data = dataset, triggerCompaction = false)

      val journalOffset = offsets.head.pos
      // FIXME single shard
      // insert distribute entry into journal
      val pos2: FilePos = journal.appendDistributeEntry(journalPos = journalOffset, shards = Map(shards.firstKey() -> shard1Offset))
      // insert alias, so , so it points to prev
      journal.appendFileAlias(pos.fileNum, pos.offset, pos2.fileNum, pos2.offset, updateEOF = true)
    } finally {
      for ((fileNum, fileHandle) <- files) {
        if (fileHandle != null)
          journal.fileUnlock(fileNum)
      }
    }
  }


  def printDirContent(dir: File = dir, out: PrintStream = System.out): Unit = {
    if (!dir.exists() || !dir.isDirectory) {
      out.println("Not a directory: " + dir)
      return
    }


    def printE(name: String, e: Any): Unit = {
      out.println(Strings.padStart(name, 15, ' ') + " = " + e)
    }

    // loop over files in dir
    for (f <- dir.listFiles(); if (f.isFile)) {
      try {
        out.println("")
        out.println("=== " + f.getName + " ===")
        val fileLength = f.length()
        printE("file length", fileLength)

        // loop over log entries in file
        val r = new RandomAccessFile(f, "r")
        try {

          while (r.getFilePointer < fileLength) {
            out.println("----------------")

            printE("Entry offset", r.getFilePointer)
            val entrySize = r.readInt()
            val entryEndOffset = r.getFilePointer - 4 + entrySize
            printE("size", entrySize)
            val head = r.readByte()


            def printLink(name: String): Unit = {
              printE(name, r.readLong() + ":" + r.readLong())
            }

            def printPrevLink(): Unit = {
              printLink("prev")
            }

            head match {
              case LogStore.headUpdate => {
                printE("head", head + " - Update")
                printPrevLink()
                val keyCount = r.readInt()
                val keySize = r.readInt()
                printE("key count", keyCount)
                printE("key size", keySize)
              }

              case LogStore.headMerge => {
                printE("head", head + " - Merge")
                printPrevLink()

                val keyCount = r.readInt()
                val keySize = r.readInt()
                printE("key count", keyCount)
                printE("key size", keySize)
              }

              case LogStore.headDistributePlaceholder => {
                printE("head", head + " - Distribute Placeholder")
                printPrevLink()
              }

              case LogStore.headDistributeFinished => {
                printE("head", head + " - Distribute Finished")
                printPrevLink()
                printLink("journal")
                val shardCount = r.readInt()

                for (i <- 0 until shardCount) {
                  out.println("")
                  printE("shard num", i)
                  printLink("shard")
                  val key = new K(keySize)
                  r.readFully(key.data)
                  printE("shard key", key)
                }
              }

              case LogStore.headAlias => {
                printE("head", head + " - Alias")
                printPrevLink()
                printLink("old")
                printLink("new")
              }

              case _ => {
                out.println(" !!! UNKNOWN HEADER !!!")
              }

            }

            // seek to end of entry
            r.seek(entryEndOffset)

          }
        } finally {
          Closeables.close(r, true)
        }


      } catch {
        case e: Exception => e.printStackTrace(out)
      }
    }


  }

}

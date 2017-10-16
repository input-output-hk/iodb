package io.iohk.iodb

import java.io.{File, PrintStream, RandomAccessFile}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executor, Executors}

import com.google.common.base.Strings
import com.google.common.io.Closeables
import io.iohk.iodb.Store.{K, V, VersionID}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class ShardedStore(
                    val dir: File,
                    val keySize: Int = 32,
                    val shardCount: Int = 1,
                    val executor:Executor = Executors.newCachedThreadPool()
                  )extends Store {

  val journal = new LogStore(keySize = keySize, dir = dir, filePrefix = "journal", executor=executor)


  val shards = new java.util.TreeMap[K, LogStore]()

  //initialize shards
  assert(shardCount > 0)
  for (i <- 0 until shardCount) {
    val key = ByteArrayWrapper(Utils.shardPrefix(shardCount, i, keySize))
    val shard = new LogStore(keySize = keySize, dir = dir, filePrefix = "shard_" + i, executor=executor, compactEnabled = true)
    shards.put(key, shard)
  }

  val updateCounter = new AtomicLong(0)

  override def get(key: K): Option[V] = {
    assert(key.size > 7)
    val v = journal.getDistribute(key)
    if (v._1.isDefined)
      return v._1 //value was found in journal
    if (v._2.isEmpty)
      return None //map of shards was not found

    val shardsFromDist = v._2.get

    val shardPos = shardsFromDist.floorEntry(key).getValue
    val shard = shards.floorEntry(key).getValue

    return shard.get(key = key, pos = shardPos)
  }

  override def getAll(consumer: (K, V) => Unit): Unit = {
    //FIXME content is loaded to heap
    val j = new java.util.TreeMap[K, V]
    journal.getAll({ (k, v) =>
      j.put(k, v)
    }, dropTombstone = false)

    for (shard <- shards.values().asScala) {
      shard.getAll { (k, v) =>
        j.putIfAbsent(k, v)
      }
    }
    j.asScala.foreach { p =>
      if (!(p._2 eq Store.tombstone))
        consumer(p._1, p._2)
    }
  }

  override def clean(count: Int): Unit = {
    //TODO cleanup
  }

  override def lastVersionID: Option[VersionID] = {
    journal.lastVersionID
  }

  override def update(versionID: VersionID, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = {
    journal.update(versionID = versionID, toRemove = toRemove, toUpdate = toUpdate)

    if(updateCounter.incrementAndGet()>10){
      updateCounter.set(0)
      executor.execute(runnable {
        taskDistribute()
      })
    }
  }

  override def rollback(versionID: VersionID): Unit = {
    journal.rollback(versionID)
//    shards.values().asScala.foreach(_.rollback(versionID))
  }

  override def close(): Unit = {
    journal.close()
    shards.values().asScala.foreach(_.close())
  }

  override def rollbackVersions(): Iterable[VersionID] = {
    journal.rollbackVersions()
  }

  override def verify(): Unit = {
    journal.verify()
    shards.values().asScala.foreach(_.verify())
  }

  def taskDistribute(): Unit = {
    val (prev,pos) = journal.appendDistributePlaceholder()

    val offsets = journal.loadUpdateOffsets(stopAtMerge = true, stopAtDistribute = true, startPos = pos)
    if (offsets.isEmpty)
      return
    //lock all files for reading
    val files = offsets.map(o => (o.pos.fileNum, journal.fileLock(o.pos.fileNum))).toMap
    try {

      val dataset = journal.loadKeyValues(offsets.filter(_.entryType == LogStore.headUpdate), files, dropTombstones = false)
      if (dataset.isEmpty)
        return

      val data = new ArrayBuffer[(K, V)]()

      val shardIter = shards.asScala.iterator
      var curr: (K, LogStore) = shardIter.next()

      val distributeEntryContent = new ArrayBuffer[(K, FilePos)]


      def flushShard(nextShardKey: K): Unit = {
        var next: (K, V) = null
        //collect keys, until next shard is reached
        do {
          next = if (dataset.hasNext) dataset.next() else null
          if (next != null && (nextShardKey == null || next._1 < nextShardKey)) {
            data += next
          }
        } while (next != null && (nextShardKey == null || next._1 < nextShardKey))

        //reached end of data, or next shard, flush current shard
        //TODO versionID
        val shardOffset = curr._2.updateDistribute(versionID = new K(0), data = data, triggerCompaction = false)

        distributeEntryContent += ((curr._1, shardOffset))

        data.clear()
        //next belongs to next shard
        if (next != null) {
          data += next
        }
      }

      //iterate over shards
      while (shardIter.hasNext) {
        val next = shardIter.next()
        flushShard(next._1)
        curr = next
      }
      //update last shard
      flushShard(null)

      //append distribute entry into journal

      val journalOffset = offsets.head.pos
      // insert distribute entry into journal
      val pos2: FilePos = journal.appendDistributeEntry(journalPos = journalOffset, prevPos=prev, shards = distributeEntryContent)
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

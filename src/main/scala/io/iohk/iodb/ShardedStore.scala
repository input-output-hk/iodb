package io.iohk.iodb

import java.io.{File, PrintStream, RandomAccessFile}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent._

import com.google.common.base.Strings
import com.google.common.io.Closeables
import io.iohk.iodb.Store.{K, V, VersionID}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class ShardedStore(
                    val dir: File,
                    val keySize: Int = 32,
                    val shardCount: Int = 20,
                    //unlimited thread executor, but the threads will exit faster to prevent memory leak
                    val executor:Executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 1, TimeUnit.SECONDS, new SynchronousQueue[Runnable]())
                  )extends Store {

  val journal = new LogStore(keySize = keySize, dir = dir, filePrefix = "journal", executor=executor)


  val shards = new java.util.TreeMap[K, LogStore]()

  /** ensures that only single distribute task runs at a time */
  private val distributeLock = new ReentrantLock()

  @volatile private var isClosed = false

  //initialize shards
  assert(shardCount > 0)
  for (i <- 0 until shardCount) {
    val key = ByteArrayWrapper(Utils.shardPrefix(shardCount, i, keySize))
    val shard = new LogStore(keySize = keySize, dir = dir, filePrefix = "shard_" + i + "_", executor=executor, compactEnabled = true)
    shards.put(key, shard)
  }

  if(executor!=null){
    def waitForStart(): Unit ={
      //wait initial time until store is initialized
      var count = 60
      while(count>0 && !isClosed){
        count-=1
        Thread.sleep(1000)
      }
    }

    //start background compaction task
    executor.execute(runnable{
      waitForStart()
      //start loop
      while(!isClosed) {
        try {
          //find the most fragmented shard
          val shard:LogStore = shards.values().asScala.toBuffer.sortBy{s:LogStore=>s.unmergedUpdates.get}.reverse.head
          if(shard.unmergedUpdates.get>4) {
            shard.taskCompact()
          }else{
            Thread.sleep(100)
          }
        } catch {
          case e: Throwable => new ExecutionException("Background shard compaction task failed", e).printStackTrace()
        }
      }
    })

    executor.execute(runnable{
      waitForStart()
      while(!isClosed) {
        try {
          if(updateCounter.incrementAndGet()>10) {
            updateCounter.set(0)
            taskDistribute()
          }else{
            Thread.sleep(100)
          }
        } catch {
          case e: Throwable => new ExecutionException("Background distribution task failed", e).printStackTrace()
        }
      }
    })
  }


  val updateCounter = new AtomicLong(0)

  override def get(key: K): Option[V] = {
    assert(key.size > 7)
    val v = journal.getDistribute(key)
    if (v._1.isDefined)
      return v._1 //value was found in journal
    if (v._2.isEmpty)
      return None //map of shards was not found

    val shardEntry = v._2.get

    val shardPos = shardEntry.shards.floorEntry(key).getValue
    val shard = shards.floorEntry(key).getValue

    return shard.get(key = key, pos = shardPos)
  }

  override def getAll(consumer: (K, V) => Unit): Unit = {
    //FIXME content is loaded to heap
    val (data, shardEntry) = journal.getAllDistribute()

    if(shardEntry!=null) {
      shardEntry.shards.asScala.foreach { a =>
        val shardKey = a._1
        val shardPos = a._2
        val shard = shards.get(shardKey)

        shard.getAll({ (k, v) =>
          data.putIfAbsent(k, v)
        }, dropTombstone = true, startPos = shardPos)
      }
    }

    data.asScala.foreach { p =>
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
  }

  override def rollback(versionID: VersionID): Unit = {
    journal.appendLock.lock()
    try {
      journal.rollback(versionID)
      //find last distribute entry
      val offsets = journal.loadUpdateOffsets(stopAtMerge = false, stopAtDistribute = false)
      val distOffsets = offsets.filter(_.entryType==LogStore.headDistributeFinished)
     // println(distOffsets.toBuffer)
      //if there is some, load its version ID and rollback all shards
      if(!distOffsets.isEmpty) {
        val de = journal.loadDistributeEntry(distOffsets.head.pos)
        shards.asScala.foreach { a=>
          val shardKey = a._1
          val shard = a._2
          val shardOffset = de.shards.get(shardKey)
          shard.rollbackToOffset(shardOffset)
        }
      }else{
        //there is no distr entry, so remove all content from shards
        shards.values().asScala.foreach { s =>
          s.rollbackToZero()
          assert(s.getAll().isEmpty)
          assert(s.lastVersionID==None)
        }

      }
    }finally{
      journal.appendLock.unlock()
    }
  }

  override def close(): Unit = {
    distributeLock.lock()
    try {
      isClosed = true
      journal.close()
      shards.values().asScala.foreach(_.close())
    }finally{
      distributeLock.unlock()
    }
  }

  override def rollbackVersions(): Iterable[VersionID] = {
    journal.rollbackVersions()
  }

  override def verify(): Unit = {
    journal.verify()
    shards.values().asScala.foreach(_.verify())
  }


  def taskDistribute(): Unit = {
    distributeLock.lock()
    try {
      if(isClosed)
        return

      val (prev, pos) = journal.appendDistributePlaceholder()

      val offsets = journal.loadUpdateOffsets(stopAtMerge = true, stopAtDistribute = true, startPos = pos)
      if (offsets.isEmpty)
        return
      //lock all files for reading
      val files = offsets.map(o => (o.pos.fileNum, journal.fileLock(o.pos.fileNum))).toMap
      try {

        val offsets2 = offsets.filter(t => t.entryType == LogStore.headUpdate || t.entryType == LogStore.headMerge)
        if (offsets2.isEmpty)
          return
        val versionID = journal.loadVersionID(offsets2.head.pos)

        val dataset = journal
          .loadKeyValues(offsets2, files, dropTombstones = false)
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
        val pos2: FilePos = journal.appendDistributeEntry(journalPos = pos,
          prevPos = prev, versionID = versionID, shards = distributeEntryContent)
        // insert alias, so , so it points to prev
        journal.appendFileAlias(pos.fileNum, pos.offset, pos2.fileNum, pos2.offset, updateEOF = true)

        journal.unmergedUpdates.set(0)
      } finally {
        for ((fileNum, fileHandle) <- files) {
          if (fileHandle != null)
            journal.fileUnlock(fileNum)
        }
      }
    }finally{
      distributeLock.unlock()
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

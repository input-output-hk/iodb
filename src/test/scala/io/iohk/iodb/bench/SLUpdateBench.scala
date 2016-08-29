package io.iohk.iodb.bench

import java.io.File

import io.iohk.iodb.TestUtils
import io.iohk.iodb.skiplist.AuthSkipList
import org.mapdb.DBMaker

/**
  * Created by jan on 8/29/16.
  */
object SLUpdateBench {

  val defaultLimit = 1e8.toLong

  def main(args: Array[String]): Unit = {
    val limit = if(args.isEmpty) defaultLimit else args(0).toInt
    val file = File.createTempFile("iodb","mapdb")
    println("")
    file.delete()
    val store = DBMaker.fileDB(file).fileMmapEnable().make().getStore
    file.deleteOnExit()
    val start = System.currentTimeMillis();
    var tick = start;
    def printProgress(i:Long): Unit = {
      val items = limit-i
      val p = 100L * items / limit
      val s = (System.currentTimeMillis()-tick)/1000
      val size = file.length()/1024
      print("\r"+p+"% - " + f" $items%,d items - $s%,d seconds - $size%,d KB")
    }
    val source = for(
      i <- (0L to limit).reverse;
      key = TestUtils.fromLong(i);
      xx = {//print progress
        if(tick+5000<System.currentTimeMillis()){
          printProgress(i)
          tick = System.currentTimeMillis()
        }
      }
      ) yield (key, key)

    val sl = AuthSkipList.createFrom(source=source, store=store, keySize = 8)
    println()
    println("===============Finished===============")
    println()
    printProgress(0)
    store.close()
  }
}

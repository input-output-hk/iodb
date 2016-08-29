package io.iohk.iodb.bench

import java.io.File

import io.iohk.iodb.TestUtils
import io.iohk.iodb.skiplist.AuthSkipList
import org.mapdb.DBMaker

object SLUpdateBench {

  val defaultLimit = 1e7.toLong

  def main(args: Array[String]): Unit = {
    val limit = if (args.isEmpty) defaultLimit else args(0).toInt
    val file = File.createTempFile("iodb", "mapdb")
    println("")
    file.delete()
    val store = DBMaker.fileDB(file).fileMmapEnable().make().getStore
    file.deleteOnExit()
    val start = System.currentTimeMillis()
    val source = (limit to 0 by -1).map(a => (TestUtils.fromLong(a), TestUtils.fromLong(a)))
    val sl = AuthSkipList.createFrom(source = source, store = store, keySize = 8)
    println()
    println("===============Finished===============")
    val s = (System.currentTimeMillis() - start) / 1000
    val size = file.length() / 1024
    print(f" $limit%,d items - $s%,d seconds - $size%,d KB")
    store.close()
  }
}

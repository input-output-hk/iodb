package io.iohk.iodb.skiplist

import io.iohk.iodb.ByteArrayWrapper
import org.junit.{Ignore, Test}
import org.mapdb.DBMaker
import org.scalatest.Assertions
import io.iohk.iodb.TestUtils._

class HashTest extends Assertions {

  val store = DBMaker.memoryDB().make().getStore

  @Test def emptyHash(): Unit ={
    val list = AuthSkipList.createEmpty(store, keySize=8)
    assert(list.loadHead().hashes == List(new ByteArrayWrapper(list.hasher.DigestSize)))
  }


  @Test @Ignore
  def oneEntry(): Unit ={
    val key = fromLong(1L)
    val list = AuthSkipList.createFrom(source=List((key, key)), store=store, keySize=8)
    val keyHash = list.hashKey(key)
    assert(list.loadHead().hashes == List(keyHash))
  }
}

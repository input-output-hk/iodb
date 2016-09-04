package io.iohk.iodb.skiplist

import io.iohk.iodb.TestUtils._
import io.iohk.iodb.skiplist.AuthSkipList._
import org.junit.Test
import org.mapdb.DBMaker
import org.scalatest.Assertions

class HashTest extends Assertions {

  val store = DBMaker.memoryDB().make().getStore

  implicit val hasher = defaultHasher

  @Test def emptyHash(): Unit = {
    val list = AuthSkipList.createEmpty(store, keySize = 8)
    val expected = hashNode(hashEntry(negativeInfinity, new V(0)), hashEntry(positiveInfinity, new V(0)))
    assert(list.loadHead().hashes == List(expected))
  }

  @Test def emptyHash2(): Unit = {
    val list = AuthSkipList.createFrom(store = store, source = List.empty, keySize = 8)
    val expected = hashNode(hashEntry(negativeInfinity, new V(0)), hashEntry(positiveInfinity, new V(0)))
    assert(list.loadHead().hashes == List(expected))
  }


  @Test
  def oneEntry(): Unit = {
    val key = fromLong(2L)
    assert(0 == levelFromKey(key)) // key which only creates base level

    val list = AuthSkipList.createFrom(source = List((key, key)), store = store, keySize = 8)
    assert(2 == list.loadHead().right.size)
    val expected =
      hashNode(
        hashEntry(negativeInfinity, new V(0)),
        hashNode(
          hashEntry(key, key),
          hashEntry(positiveInfinity, new V(0))))
    assert(list.loadHead().hashes == List(expected, expected))
  }


  @Test
  def twoEntries(): Unit = {
    val key = fromLong(2L)
    val key2 = fromLong(3L)
    assert(0 == levelFromKey(key)) // we need key which only creates base level
    assert(0 == levelFromKey(key2))

    val list = AuthSkipList.createFrom(source = List((key2, key2), (key, key)), store = store, keySize = 8)
    assert(2 == list.loadHead().right.size)
    val expected =
      hashNode(
        hashEntry(negativeInfinity, new V(0)),
        hashNode(
          hashEntry(key, key),
          hashNode(
            hashEntry(key2, key2),
            hashEntry(positiveInfinity, new V(0)))))
    assert(list.loadHead().hashes == List(expected, expected))
  }


  @Test
  def oneEntryHigher(): Unit = {
    val key = fromLong(1L)
    assert(1 == levelFromKey(key)) // need two levels

    val list = AuthSkipList.createFrom(source = List((key, key)), store = store, keySize = 8)
    assert(3 == list.loadHead().right.size)
    val headBottom = hashNode(
      hashEntry(negativeInfinity, new V(0)),
      hashEntry(key, key)
    )

    val expected =
      hashNode(
        headBottom,
        hashNode(
          hashEntry(key, key),
          hashEntry(positiveInfinity, new V(0))))
    assert(list.loadHead().hashes == List(headBottom, expected, expected))
  }

}

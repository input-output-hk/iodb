package io.iohk.iodb

import io.iohk.iodb.ShardedIterator._
import io.iohk.iodb.Store._
import io.iohk.iodb.TestUtils._
import org.junit.Test
import org.scalatest.Matchers._

class ShardedIteratorTest {

  val k1 = fromLong(1)
  val k2 = fromLong(2)
  val k3 = fromLong(3)
  val k4 = fromLong(4)

  var iter: Iterator[(K, V)] = null

  def assertEmpty(): Unit = {
    iter.hasNext shouldBe false
    assertThrows[NoSuchElementException] {
      iter.next()
    }
  }

  @Test def empty(): Unit = {
    iter = distIter(journalIter = None.iterator, shardIters = List())
    assertEmpty()
  }


  @Test def empty_journal_tombstone(): Unit = {
    iter = distIter(
      journalIter = List((k1, tombstone)).iterator,
      shardIters = List())
    assertEmpty()
  }


  @Test def one(): Unit = {
    val list = List((k1, k1))
    iter = distIter(
      journalIter = list.iterator,
      shardIters = List())
    iter.toList shouldBe list
    assertEmpty()
  }


  @Test def two(): Unit = {
    val list = List((k1, k1), (k2, k2))
    iter = distIter(
      journalIter = list.iterator,
      shardIters = List())
    iter.toList shouldBe list
    assertEmpty()
  }


  @Test def oneShard(): Unit = {
    val list = List((k1, k1))
    iter = distIter(
      journalIter = None.iterator,
      shardIters = List(list.iterator))
    iter.toList shouldBe list
    assertEmpty()
  }


  @Test def twoShard(): Unit = {
    val list = List((k1, k1), (k2, k2))
    iter = distIter(
      journalIter = None.iterator,
      shardIters = List(list.iterator))
    iter.toList shouldBe list
    assertEmpty()
  }


  @Test def twoShards(): Unit = {
    val list1 = List((k1, k1), (k2, k2))
    val list2 = List((k3, k3), (k4, k4))
    iter = distIter(
      journalIter = None.iterator,
      shardIters = List(list1.iterator, list2.iterator))
    iter.toList shouldBe list1 ::: list2
    assertEmpty()
  }


  @Test def twoShardsPlus(): Unit = {
    val list1 = List((k1, k1), (k2, k2))
    val list2 = List((k3, k3), (k4, k4))
    iter = distIter(
      journalIter = List((k3, tombstone)).iterator,
      shardIters = List(list1.iterator, list2.iterator))
    iter.toList shouldBe List((k1, k1), (k2, k2), (k4, k4))
    assertEmpty()
  }
}

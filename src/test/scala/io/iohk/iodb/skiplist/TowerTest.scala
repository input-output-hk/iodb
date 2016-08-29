package io.iohk.iodb.skiplist

import org.junit.Test
import org.mapdb.DBMaker
import org.scalatest.Assertions
import io.iohk.iodb.TestUtils._

class TowerTest extends Assertions {

  @Test def serialize(): Unit = {
    val store = DBMaker.memoryDB().make().getStore
    val tower = Tower(
      key = fromLong(1112),
      value = fromLong(12309),
      right = List(1L, 2L),
      hashes = List(randomA(11), randomA(11))
    )
    val ser = new TowerSerializer(keySize = 8, hashSize = 11)
    val recid = store.put(tower, ser)
    val tower2 = store.get(recid, ser)

    assert(tower == tower2)
  }
}
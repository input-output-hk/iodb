package io.iohk.iodb.skiplist

import io.iohk.iodb.TestUtils
import org.junit.Test
import org.mapdb._
import org.scalatest.Assertions


class AuthSkipListTest extends Assertions{

  @Test def putGet(): Unit ={
    //produce randomly ordered set
    val set = (0 until 10000).map(i=>TestUtils.randomA(32)).toSet

    val store = DBMaker.memoryDB().make().getStore
    val list = AuthSkipList.empty(store,32)
    set.foreach(key=> list.put(key, key))

    //check existing
    set.foreach { key =>
      assert(key === list.get(key))
    }
    //check non existing
    for(i<-0 until 1000){
      val key = TestUtils.randomA(32)
      if(!set.contains(key))
        assert(null == list.get(key))
    }

  }
}
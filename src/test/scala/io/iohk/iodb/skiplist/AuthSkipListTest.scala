package io.iohk.iodb.skiplist

import java.util

import io.iohk.iodb.skiplist.AuthSkipList._
import io.iohk.iodb.{ByteArrayWrapper, TestUtils}
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

  @Test def rootHash(): Unit ={
    //insert stuff into list
    val data = (0L until 10).map(TestUtils.fromLong)
    val store = DBMaker.memoryDB().make().getStore
    val list = AuthSkipList.empty(store,8)
    data.foreach { key =>
      list.put(key, key)
    }

    //calculate expected hash
    val hash = data.foldRight(nullArray){(b:K, rightHash:Hash)=>
      list.nodeHash(key=b, value=b, rightHash=rightHash, bottomHash = null)
    }
    val root = list.loadRoot()
    val node = list.loadNode(root(0))
    assert(util.Arrays.equals(hash,node.hash))
  }

  @Test def some_keys_are_towers(): Unit ={
    val store = DBMaker.memoryDB().make().getStore
    val list = AuthSkipList.empty(store,8)

    val count = 10000L;
    val levelSum = (0L until count)
      .map(TestUtils.fromLong(_))
      .map(list.levelFromKey(_))
      .sum

    //base level is 0, multiply by 0.2 to make sure there are some multi level keys (towers)
    assert(levelSum > count*0.2)
  }
}
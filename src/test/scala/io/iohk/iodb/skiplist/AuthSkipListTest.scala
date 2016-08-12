package io.iohk.iodb.skiplist

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
    val data = (0L until 10).map(TestUtils.fromLong(_))
    val store = DBMaker.memoryDB().make().getStore
    val list = AuthSkipList.empty(store,8)
    data.foreach { key =>
      list.put(key, key)
    }

    list.printStructure()

    //calculate expected hash
    val hash = data.foldRight(0){(b:K, rightHash:Hash)=>
      println(rightHash)
      nodeHash(key=b, value=b, rightHash=rightHash, bottomHash = 0)
    }
    assert(hash==list.loadHead().hash)
  }

}
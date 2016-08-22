package io.iohk.iodb.skiplist

import java.util

import io.iohk.iodb.skiplist.AuthSkipList._
import io.iohk.iodb.TestUtils._
import org.junit.{Ignore, Test}
import org.mapdb._
import org.scalatest.Assertions

import scala.util.Random


class AuthSkipListTest extends Assertions{

  @Test @Ignore
  def putGet(): Unit ={
    //produce randomly ordered set
    val set = (0 until 10000).map(i=>randomA(32)).toSet

    val store = DBMaker.memoryDB().make().getStore
    val list = AuthSkipList.createEmpty(store,32)
    set.foreach(key=> list.put(key, key))

    assert(0.2<list.calculateAverageLevel())
    //check existing
    set.foreach { key =>
      assert(key === list.get(key))
    }
    //check non existing
    for(i<-0 until 1000){
      val key = randomA(32)
      if(!set.contains(key))
        assert(null == list.get(key))
    }
  }

  @Test def create_from_iterator(): Unit ={
    val size = 1000
    val store = DBMaker.memoryDB().make().getStore
    val source = (1L to size).map(fromLong).map(k=>(k,k)).reverse
    val list = AuthSkipList.createFrom(source=source, store=store, keySize = 8)

    (1L to size).map(fromLong).foreach{key=>
      assert(key==list.get(key))
    }
  }

  @Test def findPath(): Unit ={
    //construct list and check path for all keys is found
    val size = 1000
    val store = DBMaker.memoryDB().make().getStore
    val source = (1L to size).map(fromLong).map(k=>(k,k)).reverse
    val list = AuthSkipList.createFrom(source=source, store=store, keySize = 8)

    (1L to size).map(fromLong).foreach{key=>
      val path = list.findPath(key)
      assert(path.level == 0)
      assert(key==path.tower.key)
    }
  }


  @Test @Ignore def remove(): Unit ={
    //construct list and check path for all keys is found
    val size = 10
    val store = DBMaker.memoryDB().make().getStore
    val source = (1L to size).map(fromLong).map(k=>(k,k)).reverse
    val list = AuthSkipList.createFrom(source=source, store=store, keySize = 8)

    val r = Random.shuffle(source.toBuffer)
    while(!r.isEmpty){
      val (key,value) =  r.remove(0)
      assert(list.remove(key)==value)
      for((key2,value2)<-r){
        assert(value2==list.get(key2))
      }
    }

  }
//
//  @Test def rootHash(): Unit ={
//    //insert stuff into list
//    val data = (0L until 10).map(fromLong)
//    val store = DBMaker.memoryDB().make().getStore
//    val list = AuthSkipList.createEmpty(store,8)
//    data.foreach { key =>
//      list.put(key, key)
//    }
//
//    //calculate expected hash
//    val hash = data.foldRight(nullArray){(b:K, rightHash:Hash)=>
//      list.nodeHash(key=b, value=b, rightHash=rightHash, bottomHash = null)
//    }
//    val root = list.loadHead()
//    val node = list.loadNode(root(0))
//    assert(util.Arrays.equals(hash,node.hash))
//  }
//
//  @Test def some_keys_are_towers(): Unit ={
//    val store = DBMaker.memoryDB().make().getStore
//    val list = AuthSkipList.createEmpty(store,8)
//
//    val count = 10000L;
//    val levelSum = (0L until count)
//      .map(fromLong)
//      .map(list.levelFromKey(_))
//      .sum
//
//    //base level is 0, multiply by 0.2 to make sure there are some multi level keys (towers)
//    assert(levelSum > count*0.2)
//  }

//  //shortcut method to create node, does not handle hash
//  def  node(k:Long=0L, v:Long=null, r:Long=0L, b:Long=0L) = new Node(
//      key=fromLong(k),
//      value = if(v==null) null else fromLong(v),
//      rightLink = r,
//      bottomLink = b,
//      hash = null
//    )
//
//  @Test def root_path_down(): Unit ={
//    val store = DBMaker.memoryDB().make().getStore
//    val list = AuthSkipList.empty(store,8)
//
//    //create tower next to root
//    val r0 = store.put(node(k=2), list.nodeSerializer)
//    val r1 = store.put(node(k=2, b=r0), list.nodeSerializer)
//    val r2 = store.put(node(k=2, b=r2), list.nodeSerializer)
//    val root = Array(r0,r1,r2)
//    store.update(list.headRecid, root, Serializer.LONG_ARRAY)
//
//    //now get path, it should dive all the way into bottom, path should have three empty nodes
//    val path = list.findPath(fromLong(1))
//    assert(path.size==3)
//  }

}
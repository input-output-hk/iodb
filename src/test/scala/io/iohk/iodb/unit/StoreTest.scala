package io.iohk.iodb.unit

import io.iohk.iodb.{QuickStore, Store, TestWithTempDir}

import io.iohk.iodb.TestUtils._

abstract class StoreTest extends TestWithTempDir{

  def open():Store


  def testReopen(): Unit = {

    var store = open()
    store.update(fromLong(1L), toUpdate = (1L to 100L).map(i=>(fromLong(i), fromLong(i))), toRemove=Nil)
    store.update(fromLong(2L), toUpdate = Nil, toRemove = (90L to 100L).map(fromLong))

    def check(): Unit = {
      (1L to 89L).foreach(i=> assert(Some(fromLong(i))== store.get(fromLong(i))))
    }

    check()
    store.close()
    store = open()

    check()

  }
}


class QuickStoreRefTest extends StoreTest {
  override def open(): Store = new QuickStore(dir)
}

class LSMStoreRefTest extends StoreTest {
  override def open(): Store = new QuickStore(dir)
}

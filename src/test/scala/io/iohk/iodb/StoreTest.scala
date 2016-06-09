package io.iohk.iodb

import org.scalatest.Assertions
import org.junit.Test
import java.io.File
import java.util
import java.util.Random

class StoreTest extends Assertions {

  // random Array[Byte]
  val a = (0 until 4).map{i=>
    val b = Array[Byte](32)
    new Random().nextBytes(b)
    b
  }


  var dir:File = null

  def makeStore():Store = {
    dir = new File(System.getProperty("java.io.tmpdir")+"/iodb"+Math.random())
    return makeStore(dir)
  }

  def makeStore(dir:File):Store = {
    dir.mkdirs()
    return new TrivialStore(dir)
  }


  @Test def put_get_delete_rollback() {
    val store = makeStore()

    store.update(1, List.empty, List((a(0), a(1))))

    assert(dir.listFiles().size === 1)
    assert(new File(dir,"1").exists())
    assert(store.lastVersion === 1)
    assert(util.Arrays.equals(a(1), store.get(a(0))))
    assert(store.get(a(1)) === null)

    store.update(2, List(a(0)), List.empty)

    assert(dir.listFiles().size === 2)
    assert(new File(dir,"2").exists())
    assert(store.lastVersion === 2)

    assert(store.get(a(0)) === null)

    store.rollback(1)

    assert(dir.listFiles().size === 1)
    assert(new File(dir,"1").exists())
    assert(store.lastVersion === 1)
    assert(util.Arrays.equals(a(1), store.get(a(0))))
    assert(store.get(a(1)) === null)
  }

  @Test def reopen(){
    var store = makeStore()
    store.update(3, List.empty, List((a(0), a(1))))
    store.close()

    store = makeStore(dir)
    assert(3===store.lastVersion)
    assert(util.Arrays.equals(a(1),store.get(a(0))))
  }



//    intercept[NoSuchElementException] {
//      emptyStack.pop()
//    }


}

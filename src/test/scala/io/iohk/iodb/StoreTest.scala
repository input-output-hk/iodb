package io.iohk.iodb

import org.scalatest.Assertions
import org.junit.Test
import org.junit.After
import java.io.File
import java.util.Random

class StoreTest extends Assertions {


  // random Array[Byte]
  val a = (0 until 4).map { i =>
    TestUtils.randomA()
  }

  var dir: File = null

  def makeStore(): Store = {
    dir = new File(System.getProperty("java.io.tmpdir") + "/iodb" + Math.random())
    return makeStore(dir)
  }

  def makeStore(dir: File): Store = {
    dir.mkdirs()
    return new TrivialStore(dir)
  }

  @After def deleteFiles(): Unit = {
    if (dir == null) return;
    dir.listFiles().foreach(_.delete())
    dir.delete()
  }

  @Test def put_get_delete_rollback() {
    val store = makeStore()

    store.update(1, List.empty, List((a(0), a(1))))

    assert(dir.listFiles().size === 1)
    assert(new File(dir, "1").exists())
    assert(store.lastVersion === 1)
    assert(a(1) === store.get(a(0)))
    assert(store.get(a(1)) === null)

    store.update(2, List(a(0)), List.empty)

    assert(dir.listFiles().size === 2)
    assert(new File(dir, "2").exists())
    assert(store.lastVersion === 2)

    assert(store.get(a(0)) === null)

    store.rollback(1)

    assert(dir.listFiles().size === 1)
    assert(new File(dir, "1").exists())
    assert(store.lastVersion === 1)
    assert(a(1) === store.get(a(0)))
    assert(store.get(a(1)) === null)
  }

  @Test def reopen() {
    var store = makeStore()
    store.update(3, List.empty, List((a(0), a(1))))
    store.close()

    store = makeStore(dir)
    assert(3 === store.lastVersion)
    assert(a(1) === store.get(a(0)))
  }


  //    intercept[NoSuchElementException] {
  //      emptyStack.pop()
  //    }

  @Test def null_update() {
    var store = makeStore()
    store.update(1, List.empty, List((a(0), a(1))))

    assert(1 === store.lastVersion)
    intercept[NullPointerException] {
      store.update(2, List(a(0)), List((null, a(1))))
    }
    assert(1 === store.lastVersion)
    assert(a(1) === store.get(a(0)))

    intercept[NullPointerException] {
      store.update(2, List(null), List((a(0), a(2))))
    }
    assert(1 === store.lastVersion)
    assert(a(1) === store.get(a(0)))
  }

  @Test def wrong_key_size() {
    var store = makeStore()
    store.update(1, List.empty, List((a(0), a(1))))

    assert(1 === store.lastVersion)
    val wrongKey = TestUtils.randomA(size=1)
    intercept[IllegalArgumentException] {
      store.update(2, List(a(0)), List((wrongKey, a(1))))
    }
    assert(1 === store.lastVersion)
    assert(a(1) === store.get(a(0)))
  }
}
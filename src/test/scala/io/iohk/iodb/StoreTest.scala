package io.iohk.iodb

import java.io.File

import org.junit.Test

abstract class StoreTest extends TestWithTempDir {

  def numberOfFilesPerUpdate:Int
  def makeStore(dir: File): Store

  def filePrefix:String
  def fileSuffix:String

  def file(version:Int) = new File(dir, filePrefix + version + fileSuffix);

  // random Array[Byte]
  val a = (0 until 4).map { i =>
    TestUtils.randomA()
  }

  @Test def put_get_delete_rollback() {
    val store = makeStore(dir)

    store.update(1, List.empty, List((a(0), a(1))))

    assert(dir.listFiles().size === 1 * numberOfFilesPerUpdate)
    assert(file(1).exists())
    assert(store.lastVersion === 1)
    assert(a(1) === store.get(a(0)))
    assert(store.get(a(1)) === null)

    store.update(2, List(a(0)), List.empty)

    assert(dir.listFiles().size === 2 * numberOfFilesPerUpdate)
    assert(file(2).exists())
    assert(store.lastVersion === 2)

    assert(store.get(a(0)) === null)

    store.rollback(1)

    assert(dir.listFiles().size === 1 * numberOfFilesPerUpdate)
    assert(file(1).exists())
    assert(store.lastVersion === 1)
    assert(a(1) === store.get(a(0)))
    assert(store.get(a(1)) === null)
  }

  @Test def reopen() {
    var store = makeStore(dir)
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
    var store = makeStore(dir)
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
    var store = makeStore(dir)
    store.update(1, List.empty, List((a(0), a(1))))

    assert(1 === store.lastVersion)
    val wrongKey = TestUtils.randomA(size = 1)
    intercept[IllegalArgumentException] {
      store.update(2, List(a(0)), List((wrongKey, a(1))))
    }
    assert(1 === store.lastVersion)
    assert(a(1) === store.get(a(0)))
  }
}

class StoreTrivialTest extends StoreTest{

  override def numberOfFilesPerUpdate: Int = 1

  override def makeStore(dir: File): Store = new TrivialStore(dir)

  override def filePrefix: String = "storeTrivial"
  override def fileSuffix: String = ""

}


class StoreLogTest extends StoreTest{

  override def numberOfFilesPerUpdate: Int = 2

  override def makeStore(dir: File): Store = new LogStore(dir, filePrefix)

  override def filePrefix: String = "store"
  override def fileSuffix: String = ".keys"
}
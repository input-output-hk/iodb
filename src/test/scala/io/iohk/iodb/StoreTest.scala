package io.iohk.iodb

import java.io.File

import org.junit.Test

abstract class StoreTest extends TestWithTempDir {


  val v1 = TestUtils.fromLong(1L)
  val v2 = TestUtils.fromLong(2L)
  val v3 = TestUtils.fromLong(3L)

  def numberOfFilesPerUpdate:Int
  def makeStore(dir: File): Store

  def filePrefix:String
  def fileSuffix:String

  def file(version:Int) = new File(dir, filePrefix + version + fileSuffix)

  // random Array[Byte]
  val a = (0 until 4).map { i =>
    TestUtils.randomA()
  }

  def countFiles() = dir.listFiles().filter(!_.getName.endsWith(Utils.shardInfoFileExt)).length

  @Test def put_get_delete_rollback() {
    val store = makeStore(dir)

    store.update(v1, List.empty, List((a(0), a(1))))

    assert(countFiles() === 1 * numberOfFilesPerUpdate)
    assert(file(1).exists())
    assert(store.lastVersionID.get === v1)
    assert(Some(a(1)) === store.get(a(0)))
    assert(store.get(a(1)) === None)

    store.update(v2, List(a(0)), List.empty)

    assert(countFiles() === 2 * numberOfFilesPerUpdate)
    assert(file(2).exists())
    assert(store.lastVersionID.get === v2)

    assert(store.get(a(0)) === None)

    store.rollback(v1)

    assert(countFiles() === 1 * numberOfFilesPerUpdate)
    assert(file(1).exists())
    assert(store.lastVersionID.get === v1)
    assert(a(1) === store(a(0)))
    assert(store.get(a(1)) === None)
    store.close()
  }

  @Test def reopen() {
    var store = makeStore(dir)
    store.update(v3, List.empty, List((a(0), a(1))))
    store.close()

    store = makeStore(dir)
    assert(v3 === store.lastVersionID.get)
    assert(Some(a(1)) === store.get(a(0)))
    store.close()
  }

  @Test def null_update() {
    var store = makeStore(dir)
    store.update(v1, List.empty, List((a(0), a(1))))

    assert(v1 === store.lastVersionID.get)
    intercept[NullPointerException] {
      store.update(v2, List(a(0)), List((null, a(1))))
    }
    assert(v1 === store.lastVersionID.get)
    assert(Some(a(1)) === store.get(a(0)))

    intercept[NullPointerException] {
      store.update(v3, List(null), List((a(0), a(2))))
    }
    assert(v1 === store.lastVersionID.get)
    assert(a(1) === store(a(0)))
    store.close()
  }

  @Test def wrong_key_size() {
    var store = makeStore(dir)
    store.update(v1, List.empty, List((a(0), a(1))))

    assert(v1 === store.lastVersionID.get)
    val wrongKey = TestUtils.randomA(size = 1)
    intercept[IllegalArgumentException] {
      store.update(v2, List(a(0)), List((wrongKey, a(1))))
    }
    assert(v1 === store.lastVersionID.get)
    assert(Some(a(1)) === store.get(a(0)))
    store.close()
  }
}


class LSMLogTest extends StoreTest {

  override def numberOfFilesPerUpdate: Int = 1

  override def makeStore(dir: File): Store = new LSMStore(dir)

  override def filePrefix: String = "log-"

  override def fileSuffix: String = ".log"
}

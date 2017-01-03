package io.iohk.iodb

import io.iohk.iodb.TestUtils._
import org.junit.Test

abstract class FileAccessTest extends TestWithTempDir {

  implicit val access: FileAccess

  @Test
  def getAll(): Unit = {
    val store = new LogStore(dir = dir, filePrefix = "aa", keySize = 8)

    val keyVals = (1 to 10000).map(i => (fromLong(i), fromLong(i * 2))).toBuffer
    store.update(fromLong(1), 1L, toRemove = Nil, toUpdate = keyVals)

    assert(keyVals == store.versionIterator(1L).toBuffer)

    val handle = store.files.lastEntry().getValue.fileHandle
    val keyOffset = store.files.lastEntry().getValue.baseKeyOffset
    assert(keyVals == access.readKeyValues(fileHandle = handle, baseKeyOffset = keyOffset, keySize = 8).toBuffer)

  }

  @Test
  def binarySearch(): Unit = {
    val store = new LogStore(dir = dir, filePrefix = "aa", keySize = 8)

    val keyVals = (1 to 10000).map(i => (fromLong(i), fromLong(i * 2))).toBuffer
    store.update(fromLong(1), 1L, toRemove = Nil, toUpdate = keyVals)

    assert(keyVals == store.versionIterator(1L).toBuffer)

    val handle = store.files.lastEntry().getValue.fileHandle
    val keyOffset = store.files.lastEntry().getValue.baseKeyOffset

    for ((key, value) <- keyVals) {
      val value2 = access.getValue(fileHandle = handle, key = key, baseKeyOffset = keyOffset, keySize = 8)
      assert(value == value2)
    }
  }
}

class FileAccessTest_MMAP extends FileAccessTest {
  override val access = FileAccess.MMAP
}


class FileAccessTest_UNSAFE extends FileAccessTest {
  override val access = FileAccess.UNSAFE
}


class FileAccessTest_FILE_CHANNEL extends FileAccessTest {
  override val access = FileAccess.FILE_CHANNEL
}


class FileAccessTest_SAFE extends FileAccessTest {
  override val access = FileAccess.SAFE
}
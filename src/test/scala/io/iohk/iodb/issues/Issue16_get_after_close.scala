package io.iohk.iodb.issues

import io.iohk.iodb._
import org.junit.Test

class Issue16_get_after_close extends TestWithTempDir {

  @Test def fchannel = test(FileAccess.FILE_CHANNEL)

  @Test def mmap = test(FileAccess.MMAP)

  @Test def safe = test(FileAccess.SAFE)

  @Test def unsafe = test(FileAccess.UNSAFE)


  def test(fileAccess: FileAccess) {
    val TestKeySize = 32
    val testByteArray = (0 until TestKeySize).map(_.toByte).toArray

    val lSMStore: LSMStore = new LSMStore(dir = dir, keySize = TestKeySize, fileAccess = fileAccess)
    lSMStore.update(ByteArrayWrapper(testByteArray), Seq(), Seq())

    lSMStore.close()
    intercept[StoreAlreadyClosed] {
      lSMStore.get(ByteArrayWrapper(testByteArray))
    }
  }

}

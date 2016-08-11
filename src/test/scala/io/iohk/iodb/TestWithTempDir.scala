package io.iohk.iodb

import java.io.File

import org.junit.{After, Before}
import org.scalatest.Assertions

trait TestWithTempDir extends Assertions {

  var dir: File = _

  def storeSize: Long = TestUtils.dirSize(dir)

  @Before def init() {
    dir = TestUtils.tempDir()
  }

  @After def deleteFiles(): Unit = {
    if (dir == null) return
    TestUtils.deleteRecur(dir)
  }
}

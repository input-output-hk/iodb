package io.iohk.iodb

import java.io.File

import org.junit.{After, Before}
import org.scalatest.Assertions

/**
  * Created by jan on 6/20/16.
  */
trait TestWithTempDir extends Assertions {

  var dir: File = null

  def storeSize: Long = dir.listFiles().map(_.length()).sum

  @Before def init() {
    dir = TestUtils.tempDir()
  }

  @After def deleteFiles(): Unit = {
    if (dir == null) return;
    TestUtils.deleteRecur(dir)
  }


}

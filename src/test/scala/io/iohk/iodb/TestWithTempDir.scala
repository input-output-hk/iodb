package io.iohk.iodb

import java.io.File

import org.junit.{After, Before}
import org.scalatest.Assertions

/**
  * Created by jan on 6/20/16.
  */
class TestWithTempDir extends Assertions{

  var dir: File = null
  @Before def init() {
    dir = new File(System.getProperty("java.io.tmpdir") + "/iodb" + Math.random())
    dir.mkdirs()
  }

  @After def deleteFiles(): Unit = {
    if (dir == null) return;
    dir.listFiles().foreach(_.delete())
    dir.delete()
  }


}

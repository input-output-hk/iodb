package io.iohk.iodb

import java.io.File
import java.nio.ByteBuffer

import scala.util.Random

/**
  * test utilities
  */
object TestUtils {

  def dirSize(dir: File): Long = dir.listFiles().map(_.length()).sum

  def tempDir(): File = {
    val dir = new File(System.getProperty("java.io.tmpdir") + "/iodb" + Math.random())
    dir.mkdirs()
    dir
  }

  def deleteRecur(dir: File): Unit = {
    if (dir == null) return
    val files = dir.listFiles()
    if (files != null)
      files.foreach(deleteRecur)
    dir.delete()
  }

  /** generates random byte[] of given size */
  def randomA(size: Int = 32): ByteArrayWrapper = {
    val b = new Array[Byte](size)
    Random.nextBytes(b)
    ByteArrayWrapper(b)
  }

  /** return value of  `-DlongTest=1` property, 0 if value is not defined, or 1 if its defined but not a number */
  def longTest(): Long = {
    var ret = 0L
    try {
      val prop = System.getProperty("longTest")
      if (prop != null) {
        ret = 1
        if (prop.matches("[0-9]+")) {
          ret = prop.toLong
        }
      }
    } catch {
      case _: Exception =>
    }

    ret
  }

  /** Duration of long running tests.
    * Its value is controlled by `-DlongTest=1` property, where the value is test runtime in minutes.
    * By default tests run 5 seconds, but it can be increased by setting this property.
    */
  def endTimestamp(): Long =
    5 * 1000 + longTest() * 60 * 1000 + System.currentTimeMillis()

  /** measures time it takes to execute function */
  def runningTimeUnit(computation: => Unit): Long = {
    val s = System.currentTimeMillis()
    computation
    System.currentTimeMillis() - s
  }

  /** measures time it takes to execute function */
  def runningTime[A](computation: => A): (Long, A) = {
    val s = System.currentTimeMillis()
    val res = computation
    (System.currentTimeMillis() - s, res)
  }

  def fromLong(id: Long): ByteArrayWrapper = {
    val b = ByteBuffer.allocate(8)
    b.putLong(0, id)
    ByteArrayWrapper(b.array())
  }
}

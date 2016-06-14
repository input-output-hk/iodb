package io.iohk.iodb

import java.util.Random

/**
  * test utilities
  */
object TestUtils {

  private val random =  new Random();

  def randomA(size:Int=32): ByteArrayWrapper ={
    val b = new Array[Byte](size)
    random.nextBytes(b)
    new ByteArrayWrapper(b)
  }
}

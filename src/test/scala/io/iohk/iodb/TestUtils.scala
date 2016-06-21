package io.iohk.iodb

import java.util.Random

/**
  * test utilities
  */
object TestUtils {

  private val random =  new Random();

  /** generates random byte[] of given size */
  def randomA(size:Int=32): ByteArrayWrapper ={
    val b = new Array[Byte](size)
    random.nextBytes(b)
    new ByteArrayWrapper(b)
  }

  /** Duration of long running tests.
    * Its value is controlled by `-DlongTest=1` property, where the value is test runtime in minutes.
    * By default tests run 5 seconds, but it can be increased by setting this property.
    */
  def endTimestamp():Long={
    var end:Long = 5*1000;
    try{
      val prop = System.getProperty("longTest")
      if(prop!=null)
        end = 60*1000
      if(prop.matches("[0-9]+")){
        end = 60*1000*prop.toLong
      }
    }catch{
      case _:Exception =>
    }

    return end + System.currentTimeMillis()
  }
}

package io.iohk.iodb

import java.io.Serializable
import java.util.Arrays

/**
  * Wraps byte array and provides hashCode, equals and compare methods.
  */
case class ByteArrayWrapper(val data: Array[Byte])
  extends Serializable
    with Comparable[ByteArrayWrapper] {

  //TODO wrapped data immutable?

  override def equals(o: Any): Boolean = {
    return o.isInstanceOf[ByteArrayWrapper] &&
      Arrays.equals(data, o.asInstanceOf[ByteArrayWrapper].data)
  }

  override def hashCode: Int = {
    return Utils.byteArrayHashCode(data)
  }

  def compareTo(o: ByteArrayWrapper): Int = {
    return Utils.BYTE_ARRAY_COMPARATOR.compare(this.data, o.data)
  }

}
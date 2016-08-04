package io.iohk.iodb

import java.io.Serializable
import java.util

/**
  * Wraps byte array and provides hashCode, equals and compare methods.
  */
case class ByteArrayWrapper(data: Array[Byte]) extends Serializable with Comparable[ByteArrayWrapper] {

  require(data != null)

  //TODO wrapped data immutable?

  override def equals(o: Any): Boolean =
    o.isInstanceOf[ByteArrayWrapper] &&
      util.Arrays.equals(data, o.asInstanceOf[ByteArrayWrapper].data)

  override def hashCode: Int = Utils.byteArrayHashCode(data)

  def compareTo(o: ByteArrayWrapper): Int = Utils.BYTE_ARRAY_COMPARATOR.compare(this.data, o.data)

  override def toString: String = getClass.getSimpleName+util.Arrays.toString(data)
}
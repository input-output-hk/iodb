package io.iohk.iodb

import java.io.Serializable
import java.util

import scorex.crypto.encode.Base58

/**
  * Wraps byte array and provides hashCode, equals and compare methods.
  */
case class ByteArrayWrapper(data: Array[Byte])
  extends Serializable
    with Comparable[ByteArrayWrapper]
    with Ordered[ByteArrayWrapper] {

  /** alternative constructor which takes array size and creates new empty array */
  def this(size:Int) = this(new Array[Byte](size))

  def size = data.length

  require(data != null)

  //TODO wrapped data immutable?

  override def equals(o: Any): Boolean =
    o.isInstanceOf[ByteArrayWrapper] &&
      util.Arrays.equals(data, o.asInstanceOf[ByteArrayWrapper].data)

  override def hashCode: Int = Utils.byteArrayHashCode(data)

  override def compareTo(o: ByteArrayWrapper): Int = Utils.BYTE_ARRAY_COMPARATOR.compare(this.data, o.data)

  override def compare(that: ByteArrayWrapper): Int = compareTo(that)

  override def toString: String = getClass.getSimpleName+"["+Base58.encode(data)+"]"

}
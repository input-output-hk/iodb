package io.iohk.iodb.skiplist

import io.iohk.iodb.ByteArrayWrapper
import org.mapdb.{DataInput2, DataOutput2, Serializer}

/**
  * Tower (vertical column of nodes) in auth skip list. This SL implementation stores nodes together in towers to simplify storage.
  */
case class Tower(
                  key: K,
                  value: V,
                  right: List[Recid],
                  hashes: List[Hash]) {

  assert(key != null || key == value) //both key and value are null on head
  assert(value != null || key == value)
  assert(right.nonEmpty)
  assert(right.size == hashes.size)
}


/** converts Tower from/into binary form */
class TowerSerializer(val keySize: Int, val hashSize: Int) extends Serializer[Tower] {

  override def serialize(out: DataOutput2, value: Tower): Unit = {
    assert(value.key == null || keySize == value.key.data.length)
    assert(value.hashes.forall(_.size == hashSize))

    if (value.key != null) {
      out.packInt(value.value.data.length)
      out.write(value.key.data)
      out.write(value.value.data)
    } else {
      out.packInt(Integer.MIN_VALUE) // head has null key and value
    }

    out.packInt(value.right.length)
    value.right.foreach(out.packLong)
    value.hashes.foreach(b => out.write(b.data))
  }

  override def deserialize(input: DataInput2, available: Int): Tower = {
    val valueSize = input.unpackInt()
    var key: K = null
    var value: V = null

    if (valueSize >= 0) {
      key = new ByteArrayWrapper(keySize)
      input.readFully(key.data)
      value = new ByteArrayWrapper(valueSize)
      input.readFully(value.data)
    }
    val rightSize = input.unpackInt()
    val right = (0 until rightSize).map(a => input.unpackLong()).toList
    val hashes = (0 until rightSize).map { a =>
      val hash = new ByteArrayWrapper(hashSize)
      input.readFully(hash.data)
      hash
    }.toList
    Tower(key = key, value = value, right = right, hashes = hashes)
  }
}
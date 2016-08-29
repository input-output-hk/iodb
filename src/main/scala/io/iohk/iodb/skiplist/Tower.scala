package io.iohk.iodb.skiplist

import AuthSkipList._
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

  assert(key != null)
  assert(value != null)
  assert(right.nonEmpty)
  assert(right.size == hashes.size)
}


/** converts Tower from/into binary form */
class TowerSerializer(val keySize: Int, val hashSize: Int) extends Serializer[Tower] {

  override def serialize(out: DataOutput2, value: Tower): Unit = {
    assert(keySize == value.key.data.length)
    assert(value.hashes.forall(_.size == hashSize))

    out.write(value.key.data)
    out.packInt(value.value.data.length)
    out.write(value.value.data)

    out.packInt(value.right.length)
    value.right.foreach(out.packLong)
    value.hashes.foreach(b => out.write(b.data))
  }

  override def deserialize(input: DataInput2, available: Int): Tower = {
    val key = new ByteArrayWrapper(keySize)
    input.readFully(key.data)
    val value = new ByteArrayWrapper(input.unpackInt())
    input.readFully(value.data)

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
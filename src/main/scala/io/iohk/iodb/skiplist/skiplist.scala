package io.iohk.iodb

import com.google.common.primitives.{Bytes, Ints}
import scorex.crypto.hash.{Blake2b256, CryptographicHash}

package object skiplist {

  type K = ByteArrayWrapper
  type V = ByteArrayWrapper
  type Recid = Long
  type Hash = ByteArrayWrapper

  def defaultHasher = Blake2b256

  val MaxKeySize = 512

  /** represents positive infinity for calculating chained hash */
  protected[skiplist] def positiveInfinity: (K, V) = (new K(Array.fill(MaxKeySize)(-1: Byte)), new V(Array(127: Byte)))

  /** represents negative infity for calculating negative hash */
  protected[skiplist] def negativeInfinity: (K, V) = (new K(Array.fill(1)(0: Byte)), new V(Array(-128: Byte)))

  protected[skiplist] def hashEntry(key: K, value: V)(implicit hasher: CryptographicHash): Hash = {
    ByteArrayWrapper(hasher.hash(Ints.toByteArray(key.data.length) ++ Ints.toByteArray(value.data.length) ++ key.data ++ value.data))
  }


  protected[skiplist] def hashNode(hash1: Hash, hash2: Hash)(implicit hasher: CryptographicHash): Hash = {
    assert(hash1.size == hasher.DigestSize)
    assert(hash2.size == hasher.DigestSize)
    val joined = Bytes.concat(hash1.data, hash2.data)
    ByteArrayWrapper(hasher.hash(joined))
  }


  /** Level for each key is not determined by probability, but from key hash to make Skip List structure deterministic.
    * Probability is simulated by checking if hash is dividable by a number without remainder (N % probability == 0).
    * At each level divisor increases exponentially.
    */
  protected[skiplist] def levelFromKey(key: K): Int = {
    var propability = 3
    val maxLevel = 10
    val hash = key.hashCode
    for (level <- 0 to maxLevel) {
      if (hash % propability != 0)
        return level
      propability = propability * propability
    }
    maxLevel
  }

}

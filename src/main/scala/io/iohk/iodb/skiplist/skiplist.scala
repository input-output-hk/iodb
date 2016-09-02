package io.iohk.iodb

import scorex.crypto.hash.Blake2b256

package object skiplist {

  type K = ByteArrayWrapper
  type V = ByteArrayWrapper
  type Recid = Long
  type Hash = ByteArrayWrapper

  def defaultHasher = Blake2b256

}

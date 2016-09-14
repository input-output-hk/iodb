package io.iohk.iodb.skiplist

import scorex.crypto.hash.{CommutativeHash, CryptographicHash}


case class SkipListPathEntry(sideHash: Hash, isRightHash: Boolean)

/**
  * Shows proof of existence
  */
case class SkipListPath(key: K, value: V, hashPath: List[SkipListPathEntry], hasher: CryptographicHash) {

  private implicit val hasher2 = new CommutativeHash(hasher)


  def rootHash(): Hash = {
    val startHash = hashEntry(key, value)
    hashPath.foldLeft(startHash) { (prevHash: Hash, entry: SkipListPathEntry) =>
      if (entry.isRightHash) {
        //hash code from entry is on right, prevHash is on bottom
        if (entry.sideHash == null) {
          //reuse bottom hash, if there is no right link
          prevHash
        } else {
          hashNode(prevHash, entry.sideHash)
        }
      } else {
        //hash code from entry is bottom, prevHash is on right
        hashNode(entry.sideHash, prevHash)
      }
    }
  }

}

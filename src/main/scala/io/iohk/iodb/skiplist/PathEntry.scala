package io.iohk.iodb.skiplist

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class PathEntry(
                      prev: PathEntry,
                      recid: Recid,
                      comeFromLeft: Boolean,
                      level: Int,
                      rightRecid: Recid,
                      leftRecid: Recid) {

  @tailrec final def findRight(): Recid = {
    if (rightRecid != 0L) {
      return rightRecid
    }

    var r = this
    var v = prev
    while (v != null && r.comeFromLeft) {
      r = v
      v = v.prev
    }
    if (v == null) 0L
    else v.findRight()
  }

  def verticalRecids: List[Recid] = {
    val ret = ArrayBuffer[Recid]()
    var entry = this
    var fromLeft = false
    while (entry != null) {
      if (!fromLeft)
        ret += entry.recid

      fromLeft = entry.comeFromLeft
      entry = entry.prev
    }

    ret.toList
  }

  def verticalRightTowers: mutable.Buffer[Recid] = {
    val ret = ArrayBuffer[Recid]()
    var entry = this
    var fromLeft = false
    while (entry != null) {
      if (!fromLeft)
        ret += entry.findRight()

      fromLeft = entry.comeFromLeft
      entry = entry.prev
    }

    ret
  }
}
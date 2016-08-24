package io.iohk.iodb.skiplist

import scala.annotation.tailrec


case class PathEntry(
                      prev:PathEntry,
                      recid:Long,
                      tower:Tower,
                      comeFromLeft:Boolean,
                      level:Int,
                      rightTower:Tower,
                      leftRecid:Long,
                      leftTower:Tower) {
  @tailrec final def findRight(): (Long, Tower) = {
    if(rightTower!=null){
      return (tower.right(level), rightTower)
    }

    var r = this
    var v = prev
    while(v!=null && r.comeFromLeft){
      r = v
      v = v.prev
    }
    if(v==null)
      return null
    return v.findRight()
  }
}
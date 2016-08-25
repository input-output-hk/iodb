package io.iohk.iodb.skiplist

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import AuthSkipList._

import scala.collection.mutable

case class PathEntry(
                      prev:PathEntry,
                      recid:Recid,
                      tower:Tower,
                      comeFromLeft:Boolean,
                      level:Int,
                      rightTower:Tower,
                      leftRecid:Recid,
                      leftTower:Tower) {

  @tailrec final def findRight(): (Recid, Tower) = {
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
      return (0L, null)
    return v.findRight()
  }

  def verticalRecids: List[Recid] ={
    val ret = ArrayBuffer[Recid]()
    var entry = this
    var fromLeft = false;
    while(entry!=null){
      if(!fromLeft)
        ret+=entry.recid

      fromLeft = entry.comeFromLeft
      entry = entry.prev
    }

    ret.toList
  }

  def verticalRightTowers: mutable.Buffer[(Recid, Tower)] ={
    val ret = ArrayBuffer[(Recid, Tower)]()
    var entry = this
    var fromLeft = false;
    while(entry!=null){
      if(!fromLeft)
        ret+=entry.findRight()

      fromLeft = entry.comeFromLeft
      entry = entry.prev
    }

    ret
  }

}
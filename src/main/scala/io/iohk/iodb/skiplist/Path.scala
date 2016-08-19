package io.iohk.iodb.skiplist

/**
  * Search Path used to find an node.
  * Used for proof of existence, and for updating commulative hash after update
  */
case class Path() {

}


case class PathEntry(
                      tower:Tower,
                      comeFromLeft:Boolean,
                      level:Int,
                      rightTower:Tower,
                      leftTower:Tower)
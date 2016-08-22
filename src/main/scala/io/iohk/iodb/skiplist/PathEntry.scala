package io.iohk.iodb.skiplist


case class PathEntry(
                      prev:PathEntry,
                      recid:Long,
                      tower:Tower,
                      comeFromLeft:Boolean,
                      level:Int,
                      rightTower:Tower,
                      leftRecid:Long,
                      leftTower:Tower)
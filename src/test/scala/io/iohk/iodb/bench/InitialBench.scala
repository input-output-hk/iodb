package io.iohk.iodb.bench

import java.io.File

import io.iohk.iodb.Store


object InitialBench {
  val updates = 1000000 //1M blocks

  val inputs = 1900  //average number of inputs per block
  val outputs = 2000 //average number of outputs per block

  def bench(store: Store, dir: File): Unit = {
    
  }

  def main(args: Array[String]): Unit = {

  }
}

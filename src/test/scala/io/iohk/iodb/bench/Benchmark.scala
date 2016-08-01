package io.iohk.iodb.bench


trait Benchmark {
  type Key = Array[Byte]

  val keySize = 32
  val valueSize = 256
}

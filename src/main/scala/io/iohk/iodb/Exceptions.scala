package io.iohk.iodb

/**
  * Exception if data in files are corrupted
  */
class DataCorruptionException(msg: String)
  extends RuntimeException(msg) {

}

class StoreAlreadyClosed extends RuntimeException("Store was already closed")
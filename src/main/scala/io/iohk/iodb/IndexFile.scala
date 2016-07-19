package io.iohk.iodb

import java.io.File

/**
  * Sorted table of keys. It is used to lookup keys.
  *
  * from is non-inclusive
  */
class IndexFile(
    override protected val dir:File,
    override protected val filePrefix:String, shardNum:Long,
    from:ByteArrayWrapper, to:ByteArrayWrapper) extends WithLogFile{



}

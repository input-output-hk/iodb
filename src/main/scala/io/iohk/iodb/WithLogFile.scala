package io.iohk.iodb

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption

/**
  * Contains series of log files.
  */
protected[iodb] trait WithLogFile{

  protected val dir:File;

  protected val filePrefix:String;

  protected val fileKeyExt:String = ".keys"
  protected val fileValueExt:String = ".values";
  protected val mergedExt:String = ".merged";


  case class LogFile(val version:Long, val isMerged:Boolean){
    def keyFile = WithLogFile.this.keyFile(version, isMerged)
    def valueFile = WithLogFile.this.valueFile(version, isMerged)
    val keyBuf = mmap(keyFile)
    val valueBuf = mmap(valueFile)
  }


  protected[iohk] def keyFile(version:Long, isMerged:Boolean=false) =
    new File(dir, filePrefix+version+fileKeyExt +
      (if(isMerged) mergedExt else "" ))

  protected[iohk] def valueFile(version:Long, isMerged:Boolean=false) =
    new File(dir, filePrefix+version+fileValueExt +
      (if(isMerged) mergedExt else "" ))

  protected def mmap(file: File): ByteBuffer = {
    val c = FileChannel.open(file.toPath, StandardOpenOption.READ)
    val ret = c.map(MapMode.READ_ONLY, 0, file.length())
    c.close()
    return ret
  }

}

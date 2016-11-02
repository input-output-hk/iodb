package io.iohk.iodb

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption

/**
  * Series of log files.
  * This trait contains variables and methods to manipulate series of log files.
  */
protected[iodb] trait WithLogFile {

  /** directory in which log files are stored */
  protected val dir: File

  /** log file prefix */
  protected val filePrefix: String

  /** file extension for keys */
  protected val fileKeyExt: String = ".keys"
  /** file extensoon for values */
  protected val fileValueExt: String = ".values"
  /** file extension for merge files */
  protected val mergedExt: String = ".merged"


  /**
    * Represents single log file.
    * In practice thats are two files (keys, values).
    * Both files are memory mapped on start.
    *
    * @param version  versionID for which LogFile is created
    * @param isMerged true if this file is merged (it contains all values from older versions).
    */
  case class LogFile(version: Long, isMerged: Boolean) {
    def keyFile = WithLogFile.this.keyFile(version, isMerged)

    def valueFile = WithLogFile.this.valueFile(version, isMerged)

    val keyBuf = mmap(keyFile)
    val valueBuf = mmap(valueFile)

    var unmapped = false;

    def deleteFiles(): Unit = {
      unmapped = true
      Utils.unmap(keyBuf)
      Utils.unmap(valueBuf)
      fileDelete(keyFile)
      fileDelete(valueFile)
    }


    protected def fileDelete(f: File): Unit = {
      assert(f.exists())
      val deleted = f.delete()
      assert(deleted)
    }

    def close(): Unit = {
      Utils.unmap(keyBuf)
      Utils.unmap(valueBuf)
    }

  }

  /**
    * Construct log file
    *
    * @param version  versionID for which LogFile is created
    * @param isMerged true if this file is merged (it contains all values from older versions).
    * @return mapped buffer
    */
  protected[iohk] def keyFile(version: Long, isMerged: Boolean = false) =
    new File(dir, filePrefix + version + fileKeyExt +
      (if (isMerged) mergedExt else ""))

  /**
    * Construct value file
    *
    * @param version  versionID for which LogFile is created
    * @param isMerged true if this file is merged (it contains all values from older versions).
    * @return mapped buffer
    */
  protected[iohk] def valueFile(version: Long, isMerged: Boolean = false) =
    new File(dir, filePrefix + version + fileValueExt +
      (if (isMerged) mergedExt else ""))

  /**
    * Memory maps file into read-only ByteBuffer. File must be smaller than 2GB due to addressing limit.
    *
    * @param file to be mapped
    * @return ByteByffer of memory mapped file
    */
  protected def mmap(file: File): ByteBuffer = {
    val c = FileChannel.open(file.toPath, StandardOpenOption.READ)
    val ret = c.map(MapMode.READ_ONLY, 0, file.length())
    c.close()
    ret
  }

}

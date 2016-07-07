package io.iohk.iodb

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.nio.file.StandardOpenOption
import java.util.Comparator
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.google.common.collect.Iterators

import scala.collection.JavaConverters._


/**
  * Store which uses Log-Structured-Merge tree
  */
class LSMStore(dir: File, keySize: Int = 32, keepLastN: Int = 10) extends Store {

  override def get(key: K): V = ???

  override def lastVersion: Long = ???

  override def update(versionID: Long, toRemove: Iterable[K], toUpdate: Iterable[(K, V)]): Unit = ???

  override def rollback(versionID: Long): Unit = ???

  override def clean(): Unit = ???

  override def close(): Unit = ???

  override def cleanStop(): Unit = ???
}

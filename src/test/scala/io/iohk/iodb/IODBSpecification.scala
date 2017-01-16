package io.iohk.iodb

import java.security.MessageDigest

import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{Matchers, PropSpec}

class IODBSpecification extends PropSpec
  with PropertyChecks
  with GeneratorDrivenPropertyChecks
  with Matchers {


  property("writeKey test") {
    val iFile = TestUtils.tempDir()
    iFile.mkdirs()
    val blocksStorage = new LSMStore(iFile)
    var ids: Seq[ByteArrayWrapper] = Seq()
    var i = 0

    forAll { (key: String, value: Array[Byte]) =>
      val id: ByteArrayWrapper = hash(i + key)
      val fValue: ByteArrayWrapper = ByteArrayWrapper(value)
      ids = id +: ids
      i = i + 1

      blocksStorage.update(
        id,
        Seq(),
        Seq(id -> fValue))
    }
    //old keys are defined
    ids.foreach { id =>
      blocksStorage.get(id) match {
        case None => throw new Error(s"Id $id} not found")
        case Some(v) =>
      }
    }
    //removed keys are defined not defined
    val toRemove = ids.take(5)
    blocksStorage.update(hash("removing"), toRemove, Seq())
    toRemove.foreach { id =>
      blocksStorage.get(id) match {
        case None =>
        case Some(v) => throw new Error(s"Id $id} is defined after delete")
      }
    }

    TestUtils.deleteRecur(iFile)
  }


  def hash(s: String) = ByteArrayWrapper(MessageDigest.getInstance("SHA-256").digest(s.getBytes))
}

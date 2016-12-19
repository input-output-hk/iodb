package io.iohk.iodb

import java.io.File
import java.security.MessageDigest

import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{Matchers, PropSpec}

import scala.util.Random

class IODBSpecification extends PropSpec
  with PropertyChecks
  with GeneratorDrivenPropertyChecks
  with Matchers {


  property("writeKey test") {
    val iFile = new File(s"/tmp/scorex/scorextest-${Random.nextInt(10000000)}")
    iFile.mkdirs()
    val blocksStorage = new LSMStore(iFile)
    var ids: Seq[ByteArrayWrapper] = Seq()
    var i = 0

    forAll { (key: String, value: Array[Byte]) =>
      val id: ByteArrayWrapper = ByteArrayWrapper(MessageDigest.getInstance("SHA-256").digest((i + key).getBytes))
      val fValue: ByteArrayWrapper = ByteArrayWrapper(value)
      ids = id +: ids
      i = i + 1

      blocksStorage.update(
        id,
        Seq(),
        Seq(id -> fValue))
    }
    ids.foreach { id =>
      blocksStorage.get(id) match {
        case None => throw new Error(s"Id $id} not found")
        case Some(v) =>
      }
    }
  }

}

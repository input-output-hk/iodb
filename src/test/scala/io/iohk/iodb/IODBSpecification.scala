package io.iohk.iodb

import java.security.MessageDigest

import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{BeforeAndAfterAll, Matchers, PropSpec}

import scala.util.Random

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class IODBSpecification extends PropSpec
  with PropertyChecks
  with GeneratorDrivenPropertyChecks
  with Matchers
  with BeforeAndAfterAll {

  val iFile = TestUtils.tempDir()
  iFile.mkdirs()


  property("writeKey test") {
    val blocksStorage = new LSMStore(iFile)
    var ids: Seq[ByteArrayWrapper] = Seq()
    var removed: Seq[ByteArrayWrapper] = Seq()
    var i = 0

    forAll { (key: String, value: Array[Byte], removing: Boolean) =>
      val toRemove = if (removing && ids.nonEmpty) Seq(ids(Random.nextInt(ids.length))) else Seq()
      toRemove.foreach { tr =>
        ids = ids.filter(i => i != tr)
        removed = tr +: removed
      }

      val id: ByteArrayWrapper = hash(i + key)
      val fValue: ByteArrayWrapper = ByteArrayWrapper(value)
      ids = id +: ids
      i = i + 1

      //      println(s"remove $toRemove, add ${Seq(id -> fValue)}")
      blocksStorage.update(
        id,
        toRemove,
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
    removed.foreach { id =>
      blocksStorage.get(id) match {
        case None =>
        case Some(v) => throw new Error(s"Id $id} is defined after delete")
      }
    }

  }


  override protected def afterAll(): Unit = TestUtils.deleteRecur(iFile)


  def hash(s: String) = ByteArrayWrapper(MessageDigest.getInstance("SHA-256").digest(s.getBytes))
}

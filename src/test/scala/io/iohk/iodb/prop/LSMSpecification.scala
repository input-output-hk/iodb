package io.iohk.iodb.prop

import io.iohk.iodb.{ByteArrayWrapper, LSMStore, TestUtils}
import org.scalacheck.{Gen, Prop}
import org.scalacheck.commands.Commands

import scala.util.{Success, Try}
import org.scalacheck.Arbitrary.arbitrary


object LSMSpecification extends Commands {

  type Version = Int

  type Appended = IndexedSeq[(ByteArrayWrapper, ByteArrayWrapper)]
  type Removed = IndexedSeq[ByteArrayWrapper]

  type AppendsIndex = Map[Version, Int]
  type RemovalsIndex = Map[Version, Int]

  type State = (Version, AppendsIndex, RemovalsIndex, Appended, Removed)

  type Sut = LSMStore

  val initialState: State = (0, Map(0 -> 1), Map(), IndexedSeq(ByteArrayWrapper(Array.fill(32)(0:Byte)) -> ByteArrayWrapper.fromLong(5)), IndexedSeq())

  override def canCreateNewSut(newState: (Version, AppendsIndex, RemovalsIndex, Appended, Removed),
                               initSuts: Traversable[(Version, AppendsIndex, RemovalsIndex, Appended, Removed)],
                               runningSuts: Traversable[LSMStore]): Boolean = true

  override def newSut(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): LSMStore = {
    val dir = TestUtils.tempDir()
    val s = new LSMStore(dir, maxJournalEntryCount = 1000, keepVersions = 1000)
    s.update(state._1, state._5, state._4)
    s
  }

  override def initialPreCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): Boolean = true


  override def genCommand(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): Gen[Command] = {

    def genKey: Gen[ByteArrayWrapper] = Gen.listOfN(32, arbitrary[Byte]).map(_.toArray).map(ByteArrayWrapper.apply)

    def genKV: Gen[(ByteArrayWrapper, ByteArrayWrapper)] = for{
      k <- genKey
      v <- arbitrary[Array[Byte]].map(ByteArrayWrapper.apply)
    } yield (k, v)

    val genFwd = Gen.nonEmptyListOf(genKV).map(kv => AppendForward(state._1 + 1, kv, Seq()))

    genFwd
  }


  override def destroySut(sut: LSMStore): Unit = {
    sut.close()
    TestUtils.deleteRecur(sut.dir)
  }

  override def genInitialState: Gen[(Version, AppendsIndex, RemovalsIndex, Appended, Removed)] = Gen.const(initialState)

  case class AppendForward(version: Version, toAppend: Seq[(ByteArrayWrapper, ByteArrayWrapper)], toRemove: Seq[ByteArrayWrapper]) extends UnitCommand {
    override def postCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed), success: Boolean): Prop = success

    override def run(sut: LSMStore): Unit =
      sut.update(ByteArrayWrapper.fromLong(version), toRemove, toAppend)

    override def nextState(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)):
      (Version, AppendsIndex, RemovalsIndex, Appended, Removed) = {
      (version,
        state._2 ++ Seq((version, state._4.size + toAppend.size)),
        state._3 ++ Seq((version, state._5.size + toRemove.size)),
        state._4 ++ toAppend,
        state._5 ++ toRemove
        )
    }

    override def preCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): Boolean = version == state._1 + 1
  }

  class Rollback(version: Version) extends UnitCommand {
    override def postCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed), success: Boolean): Prop = success && (state._1 == version)

    override def run(sut: LSMStore): Unit = sut.rollback(ByteArrayWrapper.fromLong(version))

    override def nextState(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): (Version, AppendsIndex, RemovalsIndex, Appended, Removed) = {
      val ap = state._2(version)
      val rp = state._3(version)
      (version, state._2.filterKeys(_ > version), state._3.filterKeys(_ > version), state._4.take(ap), state._5.take(rp))
    }

    override def preCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): Boolean = state._1 > version
  }

  class GetExisting(key: ByteArrayWrapper) extends Command{
    override type Result = ByteArrayWrapper

    override def run(sut: LSMStore): ByteArrayWrapper = sut.get(key).get

    override def nextState(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): (Version, AppendsIndex, RemovalsIndex, Appended, Removed) = state

    override def preCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): Boolean = true

    override def postCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed), result: Try[ByteArrayWrapper]): Prop = {
      val v = state._4.find{case (k, _) => key == k}.get._2
      result == Success(v)
    }
  }

  //todo: implement
  object GetRemoved

  object CleanUp extends UnitCommand {
    override def postCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed), success: Boolean): Prop = success

    override def run(sut: LSMStore): Unit = sut.taskCleanup()

    override def nextState(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): (Version, AppendsIndex, RemovalsIndex, Appended, Removed) = state

    override def preCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): Boolean = true
  }
}


object CTL extends App {
  LSMSpecification.property().check
}
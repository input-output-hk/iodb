package io.iohk.iodb.prop

import io.iohk.iodb.{ByteArrayWrapper, LSMStore, TestUtils}
import org.scalacheck.{Gen, Prop}
import org.scalacheck.commands.Commands

import scala.util.{Failure, Random, Success, Try}
import org.scalacheck.Test._


object LSMSpecification extends Commands {

  type Version = Int

  type Appended = IndexedSeq[(ByteArrayWrapper, ByteArrayWrapper)]
  type Removed = IndexedSeq[ByteArrayWrapper]

  type AppendsIndex = Map[Version, Int]
  type RemovalsIndex = Map[Version, Int]

  type State = (Version, AppendsIndex, RemovalsIndex, Appended, Removed)

  type Sut = LSMStore

  val initialState: State = (0, Map(0 -> 1), Map(), IndexedSeq(ByteArrayWrapper(Array.fill(32)(0: Byte)) -> ByteArrayWrapper.fromLong(5)), IndexedSeq())

  override def canCreateNewSut(newState: (Version, AppendsIndex, RemovalsIndex, Appended, Removed),
                               initSuts: Traversable[(Version, AppendsIndex, RemovalsIndex, Appended, Removed)],
                               runningSuts: Traversable[LSMStore]): Boolean = true

  override def newSut(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): LSMStore = {
    val dir = TestUtils.tempDir()
    val s = new LSMStore(dir, maxJournalEntryCount = 1000, keepVersions = 100)
    s.update(state._1, state._5, state._4)
    s
  }

  override def initialPreCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): Boolean = true

  override def genCommand(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): Gen[Command] = {

    lazy val appendsCount = Random.nextInt(500) + 100

    lazy val toAppend = (0 until appendsCount).map { _ =>
      val k = Array.fill(32)(Random.nextInt(Byte.MaxValue).toByte)
      val v = Array.fill(Random.nextInt(100) + 5)(Random.nextInt(Byte.MaxValue).toByte)
      ByteArrayWrapper(k) -> ByteArrayWrapper(v)
    }

    lazy val remCount = Math.min(Random.nextInt(100), state._4.size)

    lazy val toRemove = (0 until remCount).map { _ =>
      val ap = Random.nextInt(state._4.size)
      state._4(ap)._1
    }.filter(k => !state._5.contains(k)).toSet.toSeq

    lazy val genFwd = {
      Gen.const(AppendForward(state._1 + 1, toAppend, toRemove))
    }

    lazy val genGetExisting = {
      var existingIdOpt: Option[ByteArrayWrapper] = None

      do {
        val ap = Random.nextInt(state._4.size)
        val eId = state._4(ap)._1
        if (!state._5.contains(eId)) existingIdOpt = Some(eId)
      } while (existingIdOpt.isEmpty)

      val existingId = existingIdOpt.get

      Gen.const(new GetExisting(existingId))
    }

    lazy val genGetRemoved = {
      val rp = Random.nextInt(state._5.size)
      val removedId = state._5(rp)

      Gen.const(new GetRemoved(removedId))
    }

    lazy val genCleanup = Gen.const(CleanUp)

    lazy val genRollback = Gen.choose(2, 10).map(d => new Rollback(state._1 - d))

    val gf = Seq(500 -> genFwd, 50 -> genGetExisting, 1 -> genCleanup)
    val gfr = if (state._5.isEmpty) gf else gf ++ Seq(30 -> genGetRemoved)
    val gfrr = if (state._1 > 20) gfr ++ Seq(2 -> genRollback) else gfr
    Gen.frequency(gfrr: _*)
  }

  override def destroySut(sut: LSMStore): Unit = {
    sut.close()
    TestUtils.deleteRecur(sut.dir)
  }

  override def genInitialState: Gen[(Version, AppendsIndex, RemovalsIndex, Appended, Removed)] = Gen.const(initialState)

  case class AppendForward(version: Version, toAppend: Seq[(ByteArrayWrapper, ByteArrayWrapper)], toRemove: Seq[ByteArrayWrapper]) extends Command {
    type Result = Try[Unit]

    override def run(sut: LSMStore): Try[Unit] = {
    //  println("appending: " + toAppend.size + " removing: " + toRemove.size)
      Try(sut.update(ByteArrayWrapper.fromLong(version), toRemove, toAppend))
    }

    override def nextState(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)):
    (Version, AppendsIndex, RemovalsIndex, Appended, Removed) = {
      (version,
        state._2 ++ Seq((version, state._4.size + toAppend.size)),
        state._3 ++ Seq((version, state._5.size + toRemove.size)),
        state._4 ++ toAppend,
        state._5 ++ toRemove
        )
    }

    override def preCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): Boolean = {
      val keys = toAppend.map(_._1) ++ toRemove
      (version == state._1 + 1) && (keys.toSet.size == keys.size)
    }

    override def postCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed), result: Try[Try[Unit]]): Prop = {
      result.flatten match {
        case Success(_) => true
        case Failure(e) => println(e.getMessage); false
      }
    }
  }

  //todo: check that created after a rollback version element not exist anymore and that created before the rollback and deleted after element exists
  class Rollback(version: Version) extends Command {
    type Result = Try[Unit]

    override def run(sut: LSMStore): Try[Unit] = {
      println("last store version: " + sut.lastVersionID)
      Try(sut.rollback(ByteArrayWrapper.fromLong(version)))
    }

    override def nextState(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): (Version, AppendsIndex, RemovalsIndex, Appended, Removed) = {
      println(s"rolling back from ${state._1} to $version")
      val ap = state._2(version)
      val rp = state._3(version)
      (version, state._2.filterKeys(_ > version), state._3.filterKeys(_ > version), state._4.take(ap), state._5.take(rp))
    }

    override def preCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): Boolean = state._1 > version

    override def postCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed), result: Try[Try[Unit]]): Prop = {
      val res = result.flatten.isSuccess
      if (!res) println("rollback failed: " + result.flatten)
      res
    }
  }

  class GetExisting(key: ByteArrayWrapper) extends Command {
    override type Result = Option[ByteArrayWrapper]

    override def run(sut: LSMStore): Option[ByteArrayWrapper] = sut.get(key)

    override def nextState(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): (Version, AppendsIndex, RemovalsIndex, Appended, Removed) = state

    override def preCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): Boolean = true

    override def postCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed), result: Try[Option[ByteArrayWrapper]]): Prop = {
      val v = state._4.find { case (k, _) => key == k }.get._2
      val res = result.toOption.flatten.contains(v)
      if (!res) println(s"key not found: $key")
      res
    }
  }

  class GetRemoved(key: ByteArrayWrapper) extends Command {
    override type Result = Option[ByteArrayWrapper]

    override def run(sut: LSMStore): Option[ByteArrayWrapper] = sut.get(key)

    override def nextState(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): (Version, AppendsIndex, RemovalsIndex, Appended, Removed) = state

    override def preCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): Boolean = true

    override def postCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed), result: Try[Option[ByteArrayWrapper]]): Prop = {
      state._5.contains(key) && result.map(_.isEmpty).getOrElse(false)
    }
  }

  object CleanUp extends UnitCommand {
    override def postCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed), success: Boolean): Prop = success

    override def run(sut: LSMStore): Unit = {
      println("performing cleanup")
      sut.taskCleanup()
      sut.verify()
    }

    override def nextState(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): (Version, AppendsIndex, RemovalsIndex, Appended, Removed) = state

    override def preCondition(state: (Version, AppendsIndex, RemovalsIndex, Appended, Removed)): Boolean = true
  }

}


object CTL extends App {

  val params = Parameters.default
    .withMinSize(1024)
    .withMaxSize(2048)
    .withMinSuccessfulTests(2)
    .withWorkers(2)

  LSMSpecification.property().check(params)
}
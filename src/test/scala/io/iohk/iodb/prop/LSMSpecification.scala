package io.iohk.iodb.prop

import io.iohk.iodb.{ByteArrayWrapper, LSMStore, TestUtils}
import org.junit.Test
import org.scalacheck.Test._
import org.scalacheck.commands.Commands
import org.scalacheck.{Gen, Prop}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.junit.JUnitSuite
import org.scalatest.prop.Checkers

import scala.util.{Failure, Random, Success, Try}


class LSMSpecification extends JUnitSuite with Checkers with BeforeAndAfterAll {

  val params = Parameters.default
    .withMinSize(1024)
    .withMaxSize(2048)
    .withMinSuccessfulTests(2)
    .withWorkers(2)

  //todo: pass initial set size? for now the set is only about 1 element

  @Test
  def testLsm(): Unit = {
    check(new LSMCommands(maxJournalEntryCount = 1000, keepVersion = 1500).property(), params)
    check(new LSMCommands(maxJournalEntryCount = 10, keepVersion = 1500).property(), params)
  }

}

//todo: comments
class LSMCommands(val maxJournalEntryCount: Int, val keepVersion: Int) extends Commands {

  type Version = Int

  type Appended = IndexedSeq[(ByteArrayWrapper, ByteArrayWrapper)]
  type Removed = IndexedSeq[ByteArrayWrapper]

  type AppendsIndex = Map[Version, Int]
  type RemovalsIndex = Map[Version, Int]

  // type State = (Version, AppendsIndex, RemovalsIndex, Appended, Removed)
  case class State(version: Version, appendsIndex: AppendsIndex, removalsIndex: RemovalsIndex, appended: Appended, removed: Removed)


  type Sut = LSMStore

  val initialState: State = State(0, Map(0 -> 1), Map(), IndexedSeq(ByteArrayWrapper(Array.fill(32)(0: Byte)) -> ByteArrayWrapper.fromLong(5)), IndexedSeq())

  override def canCreateNewSut(newState: State,
                               initSuts: Traversable[State],
                               runningSuts: Traversable[LSMStore]): Boolean = true

  override def newSut(state: State): LSMStore = {
    val folder = TestUtils.tempDir()
    val s = new LSMStore(folder, maxJournalEntryCount = maxJournalEntryCount, keepVersions = keepVersion /*, executor = null*/)
    s.update(state.version, state.removed, state.appended)
    s
  }

  override def initialPreCondition(state: State): Boolean = true

  override def genCommand(state: State): Gen[Command] = {

    lazy val appendsCount = Random.nextInt(500) + 100

    lazy val toAppend = (0 until appendsCount).map { _ =>
      val k = Array.fill(32)(Random.nextInt(Byte.MaxValue).toByte)
      val v = Array.fill(Random.nextInt(100) + 5)(Random.nextInt(Byte.MaxValue).toByte)
      ByteArrayWrapper(k) -> ByteArrayWrapper(v)
    }

    lazy val remCount = Math.min(Random.nextInt(100), state.appended.size)

    lazy val toRemove = (0 until remCount).map { _ =>
      val ap = Random.nextInt(state.appended.size)
      state.appended(ap)._1
    }.filter(k => !state.removed.contains(k)).toSet.toSeq

    lazy val genFwd = {
      Gen.const(AppendForward(state.version + 1, toAppend, toRemove))
    }

    lazy val genGetExisting = {
      var existingIdOpt: Option[ByteArrayWrapper] = None

      do {
        val ap = Random.nextInt(state.appended.size)
        val eId = state.appended(ap)._1
        if (!state.removed.contains(eId)) existingIdOpt = Some(eId)
      } while (existingIdOpt.isEmpty)

      val existingId = existingIdOpt.get

      Gen.const(new GetExisting(existingId))
    }

    lazy val genGetRemoved = {
      val rp = Random.nextInt(state.removed.size)
      val removedId = state.removed(rp)

      Gen.const(new GetRemoved(removedId))
    }

    lazy val genCleanup = Gen.const(CleanUp)

    lazy val genRollback = Gen.choose(2, 10).map(d => new Rollback(state.version - d))

    val gf = Seq(500 -> genFwd, 50 -> genGetExisting, 1 -> genCleanup)
    val gfr = if (state.removed.isEmpty) gf else gf ++ Seq(30 -> genGetRemoved)
    val gfrr = if (state.version > 20) gfr ++ Seq(2 -> genRollback) else gfr
    Gen.frequency(gfrr: _*)
  }

  override def destroySut(sut: LSMStore): Unit = {
    sut.close()
    TestUtils.deleteRecur(sut.dir)
  }

  override def genInitialState: Gen[State] = Gen.const(initialState)

  case class AppendForward(version: Version, toAppend: Seq[(ByteArrayWrapper, ByteArrayWrapper)], toRemove: Seq[ByteArrayWrapper]) extends Command {
    type Result = Try[Unit]

    override def run(sut: LSMStore): Try[Unit] = {
      //  println("appending: " + toAppend.size + " removing: " + toRemove.size)
      Try(sut.update(ByteArrayWrapper.fromLong(version), toRemove, toAppend))
    }

    override def nextState(state: State): State = {
      assert(state.appendsIndex.get(version) == None)
      assert(state.removalsIndex.get(version) == None)
      State(version,
        state.appendsIndex ++ Seq((version, state.appended.size + toAppend.size)),
        state.removalsIndex ++ Seq((version, state.removed.size + toRemove.size)),
        state.appended ++ toAppend,
        state.removed ++ toRemove
        )
    }

    override def preCondition(state: State): Boolean = {
      val keys = toAppend.map(_._1) ++ toRemove
      (version == state.version + 1) && (keys.toSet.size == keys.size)
    }

    override def postCondition(state: State, result: Try[Try[Unit]]): Prop = {
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

    override def nextState(state: State): State = {
      println(s"rolling back from ${state.version} to $version")
      val ap = state.appendsIndex(version)
      val rp = state.removalsIndex(version)
      State(version, state.appendsIndex.filterKeys(_ <= version), state.removalsIndex.filterKeys(_ <= version), state.appended.take(ap), state.removed.take(rp))
    }

    override def preCondition(state: State): Boolean = state.version > version

    override def postCondition(state: State, result: Try[Try[Unit]]): Prop = {
      val res = result.flatten.isSuccess
      if (!res) println("rollback failed: " + result.flatten)
      res
    }
  }

  class GetExisting(key: ByteArrayWrapper) extends Command {
    override type Result = Option[ByteArrayWrapper]

    override def run(sut: LSMStore): Option[ByteArrayWrapper] = sut.get(key)

    override def nextState(state: State): State = state

    override def preCondition(state: State): Boolean = true

    override def postCondition(state: State, result: Try[Option[ByteArrayWrapper]]): Prop = {
      val v = state.appended.find { case (k, _) => key == k }.get._2
      val res = result.toOption.flatten.contains(v)
      if (!res) println(s"key not found: $key")
      res
    }
  }

  class GetRemoved(key: ByteArrayWrapper) extends Command {
    override type Result = Option[ByteArrayWrapper]

    override def run(sut: LSMStore): Option[ByteArrayWrapper] = sut.get(key)

    override def nextState(state: State): State = state

    override def preCondition(state: State): Boolean = true

    override def postCondition(state: State, result: Try[Option[ByteArrayWrapper]]): Prop = {
      state.removed.contains(key) && result.map(_.isEmpty).getOrElse(false)
    }
  }

  object CleanUp extends UnitCommand {
    override def postCondition(state: State, success: Boolean): Prop = success

    override def run(sut: LSMStore): Unit = {
      println("performing cleanup")
      sut.taskCleanup()
      sut.verify()
    }

    override def nextState(state: State): State = state

    override def preCondition(state: State): Boolean = true
  }

}